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

public class DynamicBatchSizeWaitTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.arc.selected-alternatives", "DynamicBatchSizeWait");
    config.put("debezium.source.max.batch.size", "100");
    config.put("debezium.sink.batch.batch-size-wait.max-wait-ms", "5000");
    config.put("debezium.source.poll.interval.ms", "5000");
    return config;
  }
}
