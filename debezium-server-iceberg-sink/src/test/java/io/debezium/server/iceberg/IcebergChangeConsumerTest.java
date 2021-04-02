/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.DebeziumServer;
import io.debezium.server.testresource.BaseSparkTest;
import io.debezium.server.testresource.S3Minio;
import io.debezium.server.testresource.SourcePostgresqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
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

  @Inject
  DebeziumServer server;
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.iceberg.warehouse")
  String warehouseLocation;

  {
    // Testing.Debug.enable();
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  private HadoopCatalog getIcebergCatalog() {
    // loop and set hadoopConf
    Configuration hadoopConf = new Configuration();
    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith("debezium.sink.iceberg.")) {
        hadoopConf.set(name.substring("debezium.sink.iceberg.".length()),
            ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    HadoopCatalog icebergCatalog = new HadoopCatalog("iceberg", hadoopConf, warehouseLocation);
    return icebergCatalog;
  }

  private org.apache.iceberg.Table getTable(String table) {
    HadoopCatalog catalog = getIcebergCatalog();
    return catalog.loadTable(TableIdentifier.of(table.replace(".", "-")));
  }

  @Test
  public void testDatatypes() throws Exception {
    assertEquals(sinkType, "iceberg");
    String sql = "\n" +
        "        DROP TABLE IF EXISTS inventory.table_datatypes;\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.table_datatypes (\n" +
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
        "            c_boolean BOOLean,\n" +
        "            c_uuid UUID,\n" +
        "            c_bytea BYTEA\n" +
        "          );";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.table_datatypes (" +
        "c_id, " +
        "c_text, c_varchar, c_int, c_date, c_timestamp, c_timestamptz, " +
        "c_float, c_decimal,c_numeric,c_interval,c_boolean,c_uuid,c_bytea  " +
        ") " +
        "VALUES (1, null, null, null,null,null,null," +
        "null,null,null,null,null,null,null" +
        ")," +
        "(2, 'val_text', 'A', 123, current_date , current_timestamp, current_timestamp," +
        "'1.23'::float,'1234566.34456'::decimal,'345672123.452'::numeric, interval '1 day',false," +
        "'3f207ac6-5dba-11eb-ae93-0242ac130002'::UUID, 'aBC'::bytea" +
        ")";
    SourcePostgresqlDB.runSQL(sql);

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.table_datatypes");
        df.show();
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
  public void testIcebergConsumer() throws Exception {
    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        return ds.count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });

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

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        return ds.where("first_name == 'George__UPDATE1'").count() == 3
            && ds.where("first_name == 'SallyUSer2'").count() == 1
            && ds.where("last_name is null").count() == 1
            && ds.where("id == '1004'").where("__op == 'd'").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });

    getTableData("testc.inventory.customers").show();
    // add new columns to iceberg table!  and check if new column values are populated!
    Table table = getTable("testc.inventory.customers");

    // !!!!! IMPORTANT !!! column list here is in reverse order!! for testing purpose!
    table.updateSchema()
        // test_date_column is Long type because debezium serializes date type as number
        .addColumn("test_date_column", Types.LongType.get())
        .addColumn("test_boolean_column", Types.BooleanType.get())
        .addColumn("test_varchar_column", Types.StringType.get())
        .commit();

    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers DROP COLUMN email;");
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'User3','lastname_value3','test_varchar_value3',true, '2020-01-01'::DATE);");

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        return ds.where("first_name == 'User3'").count() == 1
            && ds.where("test_varchar_column == 'test_varchar_value3'").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });

  }

  @Test
  public void testSimpleUpload() throws Exception {
    // test that max batch size is respected! `debezium.source.max.batch.size`
    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show();
        return ds.count() > 4;
      } catch (Exception e) {
        return false;
      }
    });
  }
}
