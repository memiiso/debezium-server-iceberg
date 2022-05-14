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
import javax.inject.Inject;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Disabled;
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

  @Inject
  IcebergChangeConsumer icebergConsumer;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;

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
  public void testConsumingArrayDataType() throws Exception {
    String sql = "    DROP TABLE IF EXISTS inventory.array_data;\n" +
                 "    CREATE TABLE IF NOT EXISTS inventory.array_data (\n" +
                 "      name text,\n" +
                 "      pay_by_quarter integer[],\n" +
                 "      schedule text[][]\n" +
                 "    );\n" +
                 "  INSERT INTO inventory.array_data\n" +
                 "  VALUES " +
                 "('Carol2',\n" +
                 "      ARRAY[20000, 25000, 25000, 25000],\n" +
                 "      ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']]),\n" +
                 "('Bill',\n" +
                 "      '{10000, 10000, 10000, 10000}',\n" +
                 "      '{{\"meeting\", \"lunch\"}, {\"training\", \"presentation\"}}'),\n" +
                 "  ('Carol1',\n" +
                 "          '{20000, 25000, 25000, 25000}',\n" +
                 "          '{{\"breakfast\", \"consulting\"}, {\"meeting\", \"lunch\"}}')" +
                 ";";
    SourcePostgresqlDB.runSQL(sql);

    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.array_data");
        df.show(false);
        return df.count() >= 3;
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

    S3Minio.listFiles();
  }

  @Test
  @Disabled
  public void testDataTypeChanges() throws Exception {
    String sql = "\n" +
                 "        DROP TABLE IF EXISTS inventory.data_type_changes;\n" +
                 "        CREATE TABLE IF NOT EXISTS inventory.data_type_changes (\n" +
                 "            c_id INTEGER ,\n" +
                 "            c_varchar VARCHAR,\n" +
                 "            c_int2string INTEGER,\n" +
                 "            c_date2string DATE,\n" +
                 "            c_timestamp2string TIMESTAMP,\n" +
                 "            string2int VARCHAR,\n" +
                 "            string2timestamp VARCHAR,\n" +
                 "            string2boolean VARCHAR\n" +
                 "          );";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.data_type_changes " +
          " (c_id, c_varchar, c_int2string, c_date2string, c_timestamp2string, string2int, string2timestamp, string2boolean) " +
          " VALUES (1, 'STRING-DATA-1', 123, current_date , current_timestamp, 111, current_timestamp, false)";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.data_type_changes " +
          " (c_id, c_varchar, c_int2string, c_date2string, c_timestamp2string, string2int, string2timestamp, string2boolean) " +
          " VALUES (2, 'STRING-DATA-2', 222, current_date , current_timestamp, 222, current_timestamp, true)";
    SourcePostgresqlDB.runSQL(sql);

    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.data_type_changes " +
                              "ALTER COLUMN c_int2string TYPE VARCHAR(555), " +
                              "ALTER COLUMN c_date2string TYPE VARCHAR(555), " +
                              "ALTER COLUMN c_timestamp2string TYPE VARCHAR(555), " +
                              "ALTER COLUMN string2int TYPE INTEGER USING string2int::integer, " +
                              "ALTER COLUMN string2timestamp TYPE TIMESTAMP USING string2timestamp::TIMESTAMP, " +
                              "ALTER COLUMN string2boolean TYPE boolean USING string2boolean::boolean"
    );
    sql = "INSERT INTO inventory.data_type_changes " +
          " (c_id, c_varchar, c_int2string, c_date2string, c_timestamp2string, string2int, string2timestamp, string2boolean) " +
          " VALUES (3, 'STRING-DATA-3', '333', 'current_date-3' , 'current_timestamp-3', 333, current_timestamp, false)";
    SourcePostgresqlDB.runSQL(sql);

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.data_type_changes");
        ds.printSchema();
        ds.show();
        return ds.where("__op == 'r'").count() == 19;
      } catch (Exception e) {
        return false;
      }
    });
  }

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


  @Test
  public void testPartitionedTable() {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show(false);
        return ds.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });
    S3Minio.listFiles();
  }

  @Test
  public void testMapDestination() {
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "table"), icebergConsumer.mapDestination("table1"));
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "table"), icebergConsumer.mapDestination("table2"));
  }

}
