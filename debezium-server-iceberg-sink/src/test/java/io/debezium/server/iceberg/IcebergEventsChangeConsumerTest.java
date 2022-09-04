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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergEventsChangeConsumerTest.IcebergEventsChangeConsumerTestProfile.class)
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

  public static class IcebergEventsChangeConsumerTestProfile implements QuarkusTestProfile {

    //This method allows us to override configuration properties.
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();

      config.put("debezium.sink.type", "icebergevents");

      return config;
    }
  }

}
