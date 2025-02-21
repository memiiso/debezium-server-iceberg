/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerTemporalIsoStringTest.TestProfile.class)
public class IcebergChangeConsumerTemporalIsoStringTest extends BaseSparkTest {

  @Test
  public void testConsumingVariousDataTypes() throws Exception {
    String sql =
        "DROP TABLE IF EXISTS inventory.data_types;\n" +
            "CREATE TABLE IF NOT EXISTS inventory.data_types (\n" +
            "    c_id INTEGER ,\n" +
            "    c_date DATE,\n" +
            "    c_time TIME,\n" +
            "    c_timestamp TIMESTAMP,\n" +
            "    c_timestamptz TIMESTAMPTZ\n" +
            ");";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.data_types \n" +
        "(c_id, c_date, c_time, c_timestamp, c_timestamptz) \n" +
        "VALUES \n" +
        "(1, null, null, null, null) \n" +
        ",(2, CURRENT_DATE , CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP ) \n" +
        ",(3, '2024-01-02'::DATE , CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP ) ";

    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.data_types");
        df.show(false);
        df.schema().printTreeString();

        Assertions.assertEquals(3, df.count(), "Incorrect row count");
        // Validate date field and values
        Assertions.assertEquals(DataTypes.DateType, getSchemaField(df, "c_date").dataType());
        Assertions.assertEquals(1, df.filter("c_id = 2 AND c_date = CURRENT_DATE()").count());
        Assertions.assertEquals(1, df.filter("c_id = 3 AND c_date = to_date('2024-01-02', 'yyyy-MM-dd')").count());
        return true;
      } catch (Exception e) {
//        e.printStackTrace();
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.source.hstore.handling.mode", "map");
//      config.put("debezium.source.table.whitelist", "inventory.data_types");
      config.put("debezium.source.time.precision.mode", "isostring");
      return config;
    }
  }
}
