/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.testresource.BaseSparkTest;
import io.debezium.server.testresource.S3Minio;
import io.debezium.server.testresource.SourceMysqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourceMysqlDB.class)
@TestProfile(BatchSparkChangeConsumerMysqlTestProfile.class)
public class BatchSparkChangeConsumerMysqlTest extends BaseSparkTest {


  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;

  @Test
  public void testSimpleUpload() {
    Testing.Print.enable();

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.customers");
        df.show(false);
        return df.filter("id is not null").count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
  }

}
