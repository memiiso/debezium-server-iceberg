/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.debezium.server.DebeziumMetrics;
import io.debezium.server.iceberg.BatchConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MaxBatchSizeWaitUnitTest {

  private BatchConfig config;
  private DebeziumMetrics dbzMetrics;

  @BeforeEach
  void setUp() {
    config = mock(BatchConfig.class);
    dbzMetrics = mock(DebeziumMetrics.class);

    when(config.batchSizeWaitMaxWaitMs()).thenReturn(100);
    when(config.batchSizeWaitWaitIntervalMs()).thenReturn(20);
    when(config.sourceMaxBatchSize()).thenReturn(2000);
    when(config.sourceMaxQueueSize()).thenReturn(10000);
    when(config.sourceMaxQueueSizeInBytes()).thenReturn(0L);
    when(config.batchSizeWaitQueueSizeInBytesRatio()).thenReturn(0.9);

    when(dbzMetrics.snapshotRunning()).thenReturn(false);
    when(dbzMetrics.snapshotCompleted()).thenReturn(true);
    when(dbzMetrics.streamingMilliSecondsBehindSource()).thenReturn(0L);
  }

  @Test
  void testSnapshotRunningReturnsImmediately() throws InterruptedException {
    when(dbzMetrics.snapshotRunning()).thenReturn(true);

    MaxBatchSizeWait wait = new MaxBatchSizeWait();
    wait.config = config;
    wait.dbzMetrics = dbzMetrics;

    long start = System.currentTimeMillis();
    wait.waitMs(100, 10);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed < 50, "Should return immediately when snapshot is running");
    verify(dbzMetrics, never()).streamingQueueCurrentSize();
  }

  @Test
  void testStopsWhenEventCountReachesMaxBatchSize() throws InterruptedException {
    when(dbzMetrics.streamingQueueCurrentSize()).thenReturn(2000);

    MaxBatchSizeWait wait =
        new MaxBatchSizeWait() {
          @Override
          public long maxQueueSizeInBytes() {
            return 0L;
          }
        };
    wait.config = config;
    wait.dbzMetrics = dbzMetrics;

    long start = System.currentTimeMillis();
    wait.waitMs(100, 10);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed < 50, "Should return immediately when queue current size >= max batch size");
  }

  @Test
  void testStopsWhenQueueCurrentSizeInBytesReachesThreshold() throws InterruptedException {
    when(dbzMetrics.streamingQueueCurrentSize()).thenReturn(500); // well below 2000

    MaxBatchSizeWait wait =
        new MaxBatchSizeWait() {
          @Override
          public long maxQueueSizeInBytes() {
            return 1000L;
          }

          @Override
          public long streamingQueueCurrentSizeInBytes() {
            return 900L; // 900 >= 1000 * 0.9
          }
        };
    wait.config = config;
    wait.dbzMetrics = dbzMetrics;

    long start = System.currentTimeMillis();
    wait.waitMs(100, 10);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed < 50, "Should return immediately when queue size in bytes >= threshold");
  }

  @Test
  void testWaitsUntilQueueCurrentSizeInBytesReachesThreshold() throws InterruptedException {
    when(dbzMetrics.streamingQueueCurrentSize()).thenReturn(500);

    MaxBatchSizeWait wait =
        new MaxBatchSizeWait() {
          private int callCount = 0;

          @Override
          public long maxQueueSizeInBytes() {
            return 1000L;
          }

          @Override
          public long streamingQueueCurrentSizeInBytes() {
            callCount++;
            // Below threshold initially, then crosses threshold
            if (callCount <= 2) {
              return 500L;
            }
            return 950L;
          }
        };
    wait.config = config;
    wait.dbzMetrics = dbzMetrics;

    long start = System.currentTimeMillis();
    wait.waitMs(100, 10);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed >= 20, "Should have slept at least one interval");
    assertTrue(elapsed < 100, "Should exit before maxWaitMs once bytes threshold is met");
  }

  @Test
  void testWaitsFullDurationWhenNeitherThresholdReached() throws InterruptedException {
    when(dbzMetrics.streamingQueueCurrentSize()).thenReturn(500);

    MaxBatchSizeWait wait =
        new MaxBatchSizeWait() {
          @Override
          public long maxQueueSizeInBytes() {
            return 1000L;
          }

          @Override
          public long streamingQueueCurrentSizeInBytes() {
            return 200L; // always below 900
          }
        };
    wait.config = config;
    wait.dbzMetrics = dbzMetrics;

    long start = System.currentTimeMillis();
    wait.waitMs(100, 10);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed >= 90, "Should wait full duration when thresholds are not met");
  }

  @Test
  void testMaxQueueSizeInBytesZeroDisablesByteLimit() throws InterruptedException {
    when(dbzMetrics.streamingQueueCurrentSize()).thenReturn(500);

    MaxBatchSizeWait wait =
        new MaxBatchSizeWait() {
          @Override
          public long maxQueueSizeInBytes() {
            return 0L;
          }

          @Override
          public long streamingQueueCurrentSizeInBytes() {
            return 9999999L; // High value should be ignored when maxQueueSizeInBytes == 0
          }
        };
    wait.config = config;
    wait.dbzMetrics = dbzMetrics;

    long start = System.currentTimeMillis();
    wait.waitMs(100, 10);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(
        elapsed >= 90,
        "Should wait until maxWaitMs when maxQueueSizeInBytes is 0 and event count is below maxBatchSize");
  }

  @Test
  void testInitializeValidation() {
    MaxBatchSizeWait wait = new MaxBatchSizeWait();
    wait.config = config;

    // Valid configuration
    assertDoesNotThrow(wait::initizalize);

    // Invalid interval >= max-wait-ms
    when(config.batchSizeWaitWaitIntervalMs()).thenReturn(200);
    when(config.batchSizeWaitMaxWaitMs()).thenReturn(100);
    assertThrows(AssertionError.class, wait::initizalize);

    // Reset valid intervals, test invalid ratio <= 0
    when(config.batchSizeWaitWaitIntervalMs()).thenReturn(20);
    when(config.batchSizeWaitMaxWaitMs()).thenReturn(100);
    when(config.batchSizeWaitQueueSizeInBytesRatio()).thenReturn(0.0);
    assertThrows(AssertionError.class, wait::initizalize);

    // Invalid ratio > 1.0
    when(config.batchSizeWaitQueueSizeInBytesRatio()).thenReturn(1.1);
    assertThrows(AssertionError.class, wait::initizalize);

    // Valid ratio boundary 1.0
    when(config.batchSizeWaitQueueSizeInBytesRatio()).thenReturn(1.0);
    assertDoesNotThrow(wait::initizalize);
  }
}
