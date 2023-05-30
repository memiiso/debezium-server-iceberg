/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DynamicBatchSizeWaitTest.TestProfile.class)
class DynamicBatchSizeWaitTest {

  @Inject
  DynamicBatchSizeWait waitBatchSize;

  @ConfigProperty(name = "debezium.source.poll.interval.ms", defaultValue = "1000")
  Integer pollIntervalMs;

  @Test
  void shouldIncreaseSleepMs() {
    DynamicBatchSizeWait dynamicSleep = waitBatchSize;
    // if its consuming small batch sizes, the sleep delay should increase to adjust batch size
    // sleep size should increase and stay at max (pollIntervalMs)
    int sleep = 0;
    sleep = dynamicSleep.getWaitMs(3);
    Assertions.assertTrue(sleep < pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(2);
    Assertions.assertTrue(sleep <= pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(1);
    Assertions.assertEquals((Integer) sleep, pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(1);
    Assertions.assertEquals((Integer) sleep, pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(1);
    Assertions.assertEquals((Integer) sleep, pollIntervalMs);
  }

  @Test
  void shouldDecreaseSleepMs() {
    DynamicBatchSizeWait dynamicSleep = waitBatchSize;
    // if its consuming large batch sizes, the sleep delay should decrease
    dynamicSleep.getWaitMs(3);
    dynamicSleep.getWaitMs(2);
    dynamicSleep.getWaitMs(1);
    // start test
    // max batch size = debezium.source.max.batch.size = 100
    int sleep1 = dynamicSleep.getWaitMs(120);
    int sleep2 = dynamicSleep.getWaitMs(120);
    Assertions.assertTrue(sleep2 <= sleep1);
    int sleep3 = dynamicSleep.getWaitMs(120);
    Assertions.assertTrue(sleep3 <= sleep2);
    int sleep4 = dynamicSleep.getWaitMs(120);
    Assertions.assertTrue(sleep4 <= sleep3);
    dynamicSleep.getWaitMs(120);
    dynamicSleep.getWaitMs(120);
    dynamicSleep.getWaitMs(120);
    dynamicSleep.getWaitMs(120);
    Assertions.assertTrue(dynamicSleep.getWaitMs(120) <= 100);
  }

  public static class TestProfile implements QuarkusTestProfile {

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

}