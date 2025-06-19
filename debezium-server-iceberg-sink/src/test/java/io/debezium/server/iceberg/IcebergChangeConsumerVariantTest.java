/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.try_variant_get;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerVariantTest.TestProfile.class)
public class IcebergChangeConsumerVariantTest extends BaseSparkTest {

  @Test
  public void testSimpleUpload() {

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.customers");
        df.show(false);
        df.withColumn("after_first_name", try_variant_get(col("after"), "$.first_name", "string"))
            .show(false);
        Assertions.assertEquals(DataTypes.VariantType, getSchemaField(df, "after").dataType());
        return df.count() >= 3;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    });

  }

  @Test
  public void testConsumingVariousDataTypes() throws Exception {
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
        getTableData("testc.inventory.data_types").show(false);
        Dataset<Row> df = getTableData("testc.inventory.data_types");
        df.show(false);
        Assertions.assertEquals(DataTypes.VariantType, getSchemaField(df, "after").dataType());
        Assertions.assertEquals(DataTypes.VariantType, getSchemaField(df, "before").dataType());
        df.withColumn("after_c_text", try_variant_get(col("after"), "$.c_text", "string"))
            .show(false);
        return true;
      } catch (Exception | AssertionError e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });
  }


  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.nested-as-variant", "true");
      config.put("debezium.transforms", ",");
      return config;
    }
  }

}
