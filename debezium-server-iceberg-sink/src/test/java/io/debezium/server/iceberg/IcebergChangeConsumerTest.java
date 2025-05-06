/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerTest.TestProfile.class)
public class IcebergChangeConsumerTest extends BaseSparkTest {

  @Test
  public void testConsumingVariousDataTypes() throws Exception {
    assertEquals(sinkType, "iceberg");
    String sql = """
        DROP TABLE IF EXISTS inventory.data_types;
        CREATE TABLE IF NOT EXISTS inventory.data_types (
            c_id INTEGER ,
            c_text TEXT,
            c_varchar VARCHAR,
            c_int INTEGER,
            --- time fields
            c_date DATE,
            c_timestamp TIMESTAMP,
            c_timestamp0 TIMESTAMP(0),
            c_timestamp1 TIMESTAMP(1),
            c_timestamp2 TIMESTAMP(2),
            c_timestamp3 TIMESTAMP(3),
            c_timestamp4 TIMESTAMP(4),
            c_timestamp5 TIMESTAMP(5),
            c_timestamp6 TIMESTAMP(6),
            c_timestamptz TIMESTAMPTZ,
            c_timestamptz0 TIMESTAMPTZ(0),
            c_timestamptz1 TIMESTAMPTZ(1),
            c_timestamptz2 TIMESTAMPTZ(2),
            c_timestamptz3 TIMESTAMPTZ(3),
            c_timestamptz4 TIMESTAMPTZ(4),
            c_timestamptz5 TIMESTAMPTZ(5),
            c_timestamptz6 TIMESTAMPTZ(6),
            c_time_tz TIME WITH TIME ZONE,
            c_time_without_tz TIME WITHOUT TIME ZONE,
            ---
            c_float FLOAT,
            c_decimal DECIMAL(18,6),
            c_numeric NUMERIC(18,6),
            c_interval INTERVAL,
            c_boolean BOOLEAN,
            c_uuid UUID,
            c_bytea BYTEA,
            c_json JSON,
            c_jsonb JSONB,
            c_hstore_keyval hstore,
            c_last_field VARCHAR
          );
        """;
    SourcePostgresqlDB.runSQL(sql);
    SourcePostgresqlDB.runSQL("""
        INSERT INTO inventory.data_types (c_id)
        VALUES (1)
        """);

    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.data_types").filter("c_id = 1");
        Assertions.assertEquals(1, df.count());
        df.show(false);
        return df.where("c_text is null AND c_varchar is null AND c_int is null " +
            "AND c_date is null AND c_timestamp is null AND c_timestamp2 is null AND c_timestamptz is null " +
            "AND c_float is null AND c_decimal is null AND c_numeric is null AND c_interval is null " +
            "AND c_boolean is null AND c_uuid is null AND c_bytea is null").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });

    sql = """
        INSERT INTO inventory.data_types (
        c_id,\s
        c_text, c_varchar, c_int,
        -- >>>> time fields <<<<
        c_date,
        c_timestamp, c_timestamp0, c_timestamp1, c_timestamp2, c_timestamp3, c_timestamp4, c_timestamp5, c_timestamp6,
        c_timestamptz, c_timestamptz0, c_timestamptz1, c_timestamptz2, c_timestamptz3, c_timestamptz4, c_timestamptz5, c_timestamptz6,
        c_time_tz, c_time_without_tz,
        -- >>>> end time fields <<<<
        c_float, c_decimal,c_numeric,c_interval,c_boolean,c_uuid,c_bytea, \s
        c_json, c_jsonb, c_hstore_keyval, c_last_field)\s
        VALUES (2, 'val_text', 'A', 123,
        -- >>>> time field values <<<<
        '2024-05-05'::DATE ,
        '2019-07-09 02:28:57.123456+01' , '2019-07-09 02:28:57.123456+01' , '2019-07-09 02:28:57.123456+01' , '2019-07-09 02:28:57.123456+01' , '2019-07-09 02:28:57.123456+01' , '2019-07-09 02:28:57.123456+01','2019-07-09 02:28:57.123456+01','2019-07-09 02:28:57.123456+01',
        '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01', '2019-07-09 02:28:10.123456+01',
        '04:05:11 PST', '04:05:11.789',
        -- >>>> end time field values <<<<
        '1.23'::float,'1234566.34456'::decimal,'345672123.452'::numeric, interval '1 day',false,
        '3f207ac6-5dba-11eb-ae93-0242ac130002'::UUID, 'aBC'::bytea,
        '{"reading": 1123}'::json, '{"reading": 1123}'::jsonb,\s
        'mapkey1=>1, mapkey2=>2'::hstore,\s
        'stringvalue'\s
        )
        """
    ;
    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.data_types").filter("c_id = 2");
        df.show(false);

        Assertions.assertEquals(1, df.count());
        // date
        Assertions.assertEquals(DataTypes.DateType, getSchemaField(df, "c_date").dataType());
        Assertions.assertEquals(1, df.filter("c_date = to_date('2024-05-05', 'yyyy-MM-dd')").count());
        // number fields
        Assertions.assertEquals(consumer.config.debezium().decimalHandlingMode(), RelationalDatabaseConnectorConfig.DecimalHandlingMode.DOUBLE);
        Assertions.assertEquals(1, df.filter("c_float = CAST('1.23' AS DOUBLE)").count(), "c_float not matching");
        Assertions.assertEquals(1, df.filter("c_decimal = CAST('1234566.34456' AS DOUBLE)").count(), "c_decimal not matching");
        Assertions.assertEquals(1, df.filter("c_numeric = CAST('345672123.452' AS DOUBLE)").count(), "c_numeric not matching");
        // temporal fields
        Assertions.assertEquals(DataTypes.TimestampNTZType, getSchemaField(df, "c_timestamp").dataType());
        Assertions.assertEquals(DataTypes.TimestampNTZType, getSchemaField(df, "c_timestamp0").dataType());
        Assertions.assertEquals(DataTypes.TimestampNTZType, getSchemaField(df, "c_timestamp6").dataType());
        Assertions.assertEquals(1, df.filter("CAST(c_timestamp AS STRING) = '2019-07-09 02:28:57.123456'").count(), "c_timestamp not matching");
        Assertions.assertEquals(1, df.filter("CAST(c_timestamp0 AS STRING) = '2019-07-09 02:28:57'").count(), "c_timestamp2 not matching");
        Assertions.assertEquals(1, df.filter("CAST(c_timestamp2 AS STRING) = '2019-07-09 02:28:57.12'").count(), "c_timestamp2 not matching");
        Assertions.assertEquals(1, df.filter("CAST(c_timestamp6 AS STRING) = '2019-07-09 02:28:57.123456'").count(), "c_timestamp2 not matching");
        Assertions.assertEquals(DataTypes.TimestampType, getSchemaField(df, "c_timestamptz").dataType());
        Assertions.assertEquals(DataTypes.TimestampType, getSchemaField(df, "c_timestamptz0").dataType());
        Assertions.assertEquals(DataTypes.TimestampType, getSchemaField(df, "c_timestamptz6").dataType());
        Assertions.assertEquals(1, df.filter("CAST(c_timestamptz AS STRING) = '2019-07-09 03:28:10.123456'").count(), "c_timestamptz not matching");
        Assertions.assertEquals(1, df.filter("CAST(c_timestamptz0 AS STRING) = '2019-07-09 03:28:10'").count(), "c_timestamptz not matching");
        Assertions.assertEquals(1, df.filter("CAST(c_timestamptz2 AS STRING) = '2019-07-09 03:28:10.12'").count(), "c_timestamptz not matching");
        Assertions.assertEquals(1, df.filter("CAST(c_timestamptz6 AS STRING) = '2019-07-09 03:28:10.123456'").count(), "c_timestamptz not matching");
        return true;
      } catch (Exception e) {
        e.printStackTrace();
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
        "      c_array_of_map hstore[],\n" +
        "      schedule text[][]\n" +
        "    );\n" +
        "  INSERT INTO inventory.array_data\n" +
        "  VALUES " +
        "('Carol2',\n" +
        "      ARRAY[20000, 25000, 25000, 25000],\n" +
        "      ARRAY['mapkey1=>1, mapkey2=>2'::hstore],\n" +
        "      ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']]),\n" +
        "('Bill',\n" +
        "      '{10000, 10000, 10000, 10000}',\n" +
        "      ARRAY['mapkey1=>1, mapkey2=>2'::hstore],\n" +
        "      '{{\"meeting\", \"lunch\"}, {\"training\", \"presentation\"}}'),\n" +
        "  ('Carol1',\n" +
        "      '{20000, 25000, 25000, 25000}',\n" +
        "      ARRAY['mapkey1=>1, mapkey2=>2'::hstore],\n" +
        "      '{{\"breakfast\", \"consulting\"}, {\"meeting\", \"lunch\"}}')" +
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
        ds.show();
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
  public void testFieldAddition() throws Exception {
    String sql = """
        DROP TABLE IF EXISTS inventory.data_type_changes;
        CREATE TABLE IF NOT EXISTS inventory.data_type_changes (
            c_id INTEGER ,
            c_varchar VARCHAR
          );
        INSERT INTO inventory.data_type_changes
         (c_id, c_varchar)
         VALUES (1, 'STRING-DATA-1')
        """;
    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.data_type_changes");
        ds.printSchema();
        ds.show();
        return ds.count() >= 1 &&
            Objects.equals(dataTypeString(ds, "c_varchar"), "string");
      } catch (Exception e) {
        return false;
      }
    });
    sql = "ALTER TABLE inventory.data_type_changes ADD COLUMN c_integer INTEGER;" +
        " INSERT INTO inventory.data_type_changes " +
        " (c_id, c_varchar, c_integer) " +
        " VALUES (2, 'STRING-DATA-2', 222)";
    SourcePostgresqlDB.runSQL(sql);

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.data_type_changes");
        ds.printSchema();
        ds.show();
        System.out.println(dataTypeString(ds, "c_integer"));
        return ds.count() >= 2 &&
            Objects.equals(dataTypeString(ds, "c_integer"), "integer");
      } catch (Exception e) {
        return false;
      }
    });
    sql = "ALTER TABLE inventory.data_type_changes ADD COLUMN c_timestamptz TIMESTAMPTZ;" +
        " INSERT INTO inventory.data_type_changes " +
        " (c_id, c_varchar, c_integer, c_timestamptz) " +
        " VALUES (3, 'STRING-DATA-3', 333, current_timestamp)";
    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.data_type_changes");
        ds.printSchema();
        ds.show();
        System.out.println(dataTypeString(ds, "c_timestamptz"));
        return ds.count() >= 3 &&
            Objects.equals(dataTypeString(ds, "c_timestamptz"), "timestamp");
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testDebeziumConfig() {
    debeziumConfig.transformsConfigs().forEach( (k,v) -> {
      LOGGER.error("{} ==> {}", k, v);
    } );
    LOGGER.error("transforms ==> {}", config.debezium().transforms());
    assertTrue(config.debezium().transformsConfigs().containsKey("unwrap.type"));
    assertEquals(debeziumConfig.transforms(), "unwrap");
    assertEquals(config.debezium().isEventFlatteningEnabled(), true);

  }

  @Test
  public void testSimpleUpload() {

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show(false);
        return ds.count() >= 3;
      } catch (Exception e) {
        e.printStackTrace();
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

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> d = getTableDataV2(TableIdentifier.of(ICEBERG_CATALOG_TABLE_NAMESPACE, "debezium_offset_storage_table"));
        System.out.println(Lists.newArrayList(d));
        return Lists.newArrayList(d).size() == 1;
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
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "debeziumcdc_table"), consumer.mapDestination("table1"));
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "debeziumcdc_table"), consumer.mapDestination("table2"));
    // test
    when(consumer.config.iceberg()).thenReturn(icebergConfig);
    when(icebergConfig.destinationUppercaseTableNames()).thenReturn(true);
    when(icebergConfig.destinationLowercaseTableNames()).thenReturn(false);
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "DEBEZIUMCDC_TABLE_NAME"), consumer.mapDestination("table_name"));
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "DEBEZIUMCDC_TABLE_NAME"), consumer.mapDestination("Table_Name"));
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "DEBEZIUMCDC_TABLE_NAME"), consumer.mapDestination("TABLE_NAME"));
    when(consumer.config.iceberg()).thenReturn(icebergConfig);
    when(icebergConfig.destinationUppercaseTableNames()).thenReturn(false);
    when(icebergConfig.destinationLowercaseTableNames()).thenReturn(true);
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "debeziumcdc_table_name"), consumer.mapDestination("Table_Name"));
    assertEquals(TableIdentifier.of(Namespace.of(namespace), "debeziumcdc_table_name"), consumer.mapDestination("TABLE_NAME"));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.write.format.default", "orc");
      config.put("debezium.sink.iceberg.destination-regexp", "\\d");
      //config.put("debezium.sink.iceberg.destination-regexp-replace", "");
      config.put("debezium.source.hstore.handling.mode", "map");
      return config;
    }
  }

}
