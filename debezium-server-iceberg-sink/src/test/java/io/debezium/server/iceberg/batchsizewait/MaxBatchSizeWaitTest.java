/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

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
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxBatchSizeWaitTest.MaxBatchSizeWaitTestProfile.class)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
class MaxBatchSizeWaitTest extends BaseSparkTest {
  @Inject
  MaxBatchSizeWait waitBatchSize;
  @ConfigProperty(name = "debezium.source.poll.interval.ms", defaultValue = "1000")
  Integer pollIntervalMs;
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;


  @Test
  public void testBatchsizeWait() throws Exception {
    int iteration = 100;
    PGCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      PGLoadTestDataTable(maxBatchSize / 10, true);
    }
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_data");
        df.createOrReplaceGlobalTempView("test_data_batch_size");
        df = spark
            .sql("SELECT substring(input_file,94,60) as input_file, " +
                 "count(*) as batch_size FROM global_temp.test_data_batch_size group " +
                 "by 1");
        //df.show(false);
        return df.filter("batch_size = " + maxBatchSize).count() >= 5;
      } catch (Exception e) {
        return false;
      }
    });
  }

  public static class MaxBatchSizeWaitTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      // wait
      config.put("debezium.sink.batch.batch-size-wait", "MaxBatchSizeWait");
      config.put("debezium.sink.batch.metrics.snapshot-mbean", "debezium.postgres:type=connector-metrics,context=snapshot,server=testc");
      config.put("debezium.sink.batch.metrics.streaming-mbean", "debezium.postgres:type=connector-metrics,context=streaming,server=testc");
      config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
      config.put("debezium.source.max.batch.size", "5000");
      config.put("debezium.source.max.queue.size", "70000");
      //config.put("debezium.source.poll.interval.ms", "1000");
      config.put("debezium.sink.batch.batch-size-wait.max-wait-ms", "5000");
      config.put("debezium.sink.batch.batch-size-wait.wait-interval-ms", "1000");
      config.put("quarkus.log.category.\"io.debezium.server.iceberg.batchsizewait\".level", "DEBUG");
      return config;
    }
  }
}