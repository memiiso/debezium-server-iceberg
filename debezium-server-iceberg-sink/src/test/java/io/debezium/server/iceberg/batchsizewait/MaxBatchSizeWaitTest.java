/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import io.debezium.server.iceberg.BaseSparkTest;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@QuarkusTest
@TestProfile(MaxBatchSizeWaitTest.TestProfile.class)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class MaxBatchSizeWaitTest extends BaseSparkTest {

  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;


  @Test
  public void testBatchsizeWait() throws Exception {
    int iteration = 100;
    PGCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      this.PGLoadTestDataTable(maxBatchSize / 2, true);
    }
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = spark.sql("SELECT substring(input_file_name(),0,260) as input_file, " +
                 "count(*) as batch_size FROM debeziumevents.debeziumcdc_testc_inventory_test_data group " +
                 "by 1");
        df.show(false);
        // commited batch size should be equal to maxBatchSize
        return df.filter("batch_size = " + maxBatchSize).count() >= 3;
      } catch (Exception e) {
        //e.printStackTrace();
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      // wait
      config.put("debezium.sink.batch.batch-size-wait", "MaxBatchSizeWait");
      config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
      config.put("debezium.source.max.batch.size", "2000");
      config.put("debezium.source.max.queue.size", "70000");
      //config.put("debezium.source.poll.interval.ms", "1000");
      config.put("debezium.sink.batch.batch-size-wait.max-wait-ms", "5000");
      config.put("debezium.sink.batch.batch-size-wait.wait-interval-ms", "1000");
      config.put("quarkus.log.category.\"io.debezium.server.iceberg.batchsizewait\".level", "DEBUG");
      return config;
    }
  }
}