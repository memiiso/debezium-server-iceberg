/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(IcebergChangeConsumerTestProfile.class)
public class IcebergChangeConsumerTest extends BaseSparkTest {
  
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumerTest.class);
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;
  
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.iceberg.warehouse")
  String warehouseLocation;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  
  protected org.apache.iceberg.Table getTable(String table) {
    HadoopCatalog catalog = getIcebergCatalog();
    return catalog.loadTable(TableIdentifier.of(Namespace.of(namespace), tablePrefix + table.replace(".", "_")));
  }
  
  @Test
  public void testConsumingVariousDataTypes() throws Exception {
    assertEquals(sinkType, "iceberg");
    String sql = "\n" +
        "        DROP TABLE IF EXISTS inventory.data_types;\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.data_types (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar VARCHAR,\n" +
        "            c_int INTEGER,\n" +
        "            c_date DATE,\n" +
        "            c_timestamp TIMESTAMP,\n" +
        "            c_timestamptz TIMESTAMPTZ,\n" +
        "            c_float FLOAT,\n" +
        "            c_decimal DECIMAL(18,4),\n" +
        "            c_numeric NUMERIC(18,4),\n" +
        "            c_interval INTERVAL,\n" +
        "            c_boolean BOOLEAN,\n" +
        "            c_uuid UUID,\n" +
        "            c_bytea BYTEA,\n" +
        "            c_json JSON,\n" +
        "            c_jsonb JSONB\n" +
        "          );";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.data_types (" +
        "c_id, " +
        "c_text, c_varchar, c_int, c_date, c_timestamp, c_timestamptz, " +
        "c_float, c_decimal,c_numeric,c_interval,c_boolean,c_uuid,c_bytea,  " +
        "c_json, c_jsonb) " +
        "VALUES (1, null, null, null,null,null,null," +
        "null,null,null,null,null,null,null," +
        "null,null)," +
        "(2, 'val_text', 'A', 123, current_date , current_timestamp, current_timestamp," +
        "'1.23'::float,'1234566.34456'::decimal,'345672123.452'::numeric, interval '1 day',false," +
        "'3f207ac6-5dba-11eb-ae93-0242ac130002'::UUID, 'aBC'::bytea," +
        "'{\"reading\": 1123}'::json, '{\"reading\": 1123}'::jsonb" +
        ")";
    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.data_types");
        df.show(true);
        return df.where("c_text is null AND c_varchar is null AND c_int is null " +
            "AND c_date is null AND c_timestamp is null AND c_timestamptz is null " +
            "AND c_float is null AND c_decimal is null AND c_numeric is null AND c_interval is null " +
            "AND c_boolean is null AND c_uuid is null AND c_bytea is null").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testSchemaChanges() throws Exception {
    // TEST add new columns, drop not null constraint
    SourcePostgresqlDB.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ADD test_varchar_column varchar(255);");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ADD test_boolean_column boolean;");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ADD test_date_column date;");

    SourcePostgresqlDB.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1'  WHERE id = 1002 ;");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ALTER COLUMN email DROP NOT NULL;");
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'SallyUSer2','Thomas',null,'value1',false, '2020-01-01');");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ALTER COLUMN last_name DROP NOT NULL;");
    SourcePostgresqlDB.runSQL("UPDATE inventory.customers SET last_name = NULL  WHERE id = 1002 ;");
    SourcePostgresqlDB.runSQL("DELETE FROM inventory.customers WHERE id = 1004 ;");

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        //ds.show();
        return
            ds.where("__op == 'r'").count() == 4 // snapshot rows. initial table data
            && ds.where("__op == 'u'").count() == 3 // 3 update
            && ds.where("__op == 'c'").count() == 1 // 1 insert
            && ds.where("__op == 'd'").count() == 1 // 1 insert
            && ds.where("first_name == 'George__UPDATE1'").count() == 3
            && ds.where("first_name == 'SallyUSer2'").count() == 1
            && ds.where("last_name is null").count() == 1
            && ds.where("id == '1004'").where("__op == 'd'").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });
    
    // added columns are not recognized by iceberg
    getTableData("testc.inventory.customers").show();
    
    // TEST add new columns to iceberg table then check if its data populated!
    Table table = getTable("testc.inventory.customers");
    // NOTE column list below are in reverse order!! testing the behaviour purpose!
    table.updateSchema()
        // NOTE test_date_column is Long type because debezium serializes date type as number
        .addColumn("test_date_column", Types.LongType.get())
        .addColumn("test_boolean_column", Types.BooleanType.get())
        .addColumn("test_varchar_column", Types.StringType.get())
        .commit();
    // insert row after defining new column in target iceberg table
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'After-Defining-Iceberg-fields','Thomas',null,'value1',false, '2020-01-01');");
    
    // remove column from source
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers DROP COLUMN email;");
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'User3','lastname_value3','after-dropping-email-column-from-source',true, '2020-01-01'::DATE);");

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show();
        return ds.where("first_name == 'User3'").count() == 1
            && ds.where("first_name == 'After-Defining-Iceberg-fields'").count() == 1
            && ds.where("test_varchar_column == 'after-dropping-email-column-from-source' AND email is null").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });
    getTableData("testc.inventory.customers").show();

    // CASE 1:(Adding new column to source) (A column missing in iceberg table)
    // data of the new column is ignored till same column defined in iceberg table
    // for example: if a column not found in iceberg table its data is dropped ignored and not copied to target!
    // once iceberg table adds same column then data for this column recognized and populated

    // CASE 2:(Removing column from source) (An extra column in iceberg table)
    // these columns are populated with null value
    
    // CASE 3:(Renaming column from source) 
    // this is CASE 2 + CASE 1 : old column will be populated with null values and new column will not be recognized 
    // and populated till it's added to iceberg table
    
    S3Minio.listFiles();

  }
  
//  @Test
//  @Disabled
//  public void testDataTypeChanges() throws Exception {
//  @TODO change boolean to string. string to bolean
//  @TODO change int to string, string to int
//  }

  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show();
        return ds.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });
    
    // test nested data(struct) consumed
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.geom");
        ds.show(false);
        return ds.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });
  }
}
