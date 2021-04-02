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
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import javax.inject.Inject;

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
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(IcebergEventsChangeConsumerTestProfile.class)
public class IcebergEventsChangeConsumerTest extends BaseSparkTest {
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;
  @Inject
  DebeziumServer server;

  @Test
  public void testIcebergEvents() throws Exception {
    Assertions.assertEquals(sinkType, "icebergevents");
    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = spark.sql("SELECT * FROM default.debezium_events");
        ds.show();
        return ds.count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });

    // @TODO test distinct destination number(number of replicated tables)
    // @TODO destination(replicated table)
  }
}
