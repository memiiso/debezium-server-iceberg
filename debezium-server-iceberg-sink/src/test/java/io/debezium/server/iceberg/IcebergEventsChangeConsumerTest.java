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
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@WithTestResource(value = S3Minio.class)
@WithTestResource(value = SourceMysqlDB.class)
@TestProfile(IcebergEventsChangeConsumerTest.TestProfile.class)
public class IcebergEventsChangeConsumerTest extends BaseSparkTest {
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

  @Test
  public void testSimpleUpload() {
    Assertions.assertEquals(sinkType, "icebergevents");
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = spark.newSession().sql("SELECT * FROM debeziumevents.debezium_events");
        ds.show(false);
        return ds.count() >= 10
               && ds.select("event_destination").distinct().count() >= 2;
      } catch (Exception e) {
        return false;
      }
    });

    S3Minio.listFiles();
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "icebergevents");
      config.put("quarkus.profile", "mysql");
      config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
      config.put("%mysql.debezium.source.table.whitelist", "inventory.customers,inventory.test_delete_table");
      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }

}
