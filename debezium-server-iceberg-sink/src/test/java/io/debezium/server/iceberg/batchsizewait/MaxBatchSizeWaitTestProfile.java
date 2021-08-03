/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class MaxBatchSizeWaitTestProfile implements QuarkusTestProfile {

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