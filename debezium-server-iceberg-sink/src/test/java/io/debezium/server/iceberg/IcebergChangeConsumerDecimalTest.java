/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerDecimalTest.TestProfile.class)
public class IcebergChangeConsumerDecimalTest extends BaseSparkTest {

  @Test
  public void testConsumingNumerics() throws Exception {
    assertEquals(sinkType, "iceberg");
    String sql = "\n" +
        "        DROP TABLE IF EXISTS inventory.data_types;\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.data_types (\n" +
        "            c_id INTEGER ,\n" +
        "            c_decimal DECIMAL(18,6)\n" +
        "          );";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.data_types (c_id, c_decimal) " +
        "VALUES (1, '1234566.34456'::decimal)";
    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.data_types");
        df.show(false);

        Assertions.assertEquals(1, df.count());
        Assertions.assertEquals(1, df.filter("c_id = 1 AND c_decimal = CAST('1234566.344560' AS DECIMAL(18,6))").count(), "c_decimal not matching");
        return true;
      } catch (Exception | AssertionError e) {
        e.printStackTrace();
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.destination-regexp", "\\d");
      config.put("debezium.source.decimal.handling.mode", "precise");
      return config;
    }
  }

}
