/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import io.debezium.DebeziumException;
import io.debezium.server.DebeziumMetrics;
import io.debezium.server.iceberg.BatchConfig;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizes batch size around 85%-90% of max,batch.size using dynamically calculated sleep(ms)
 *
 * @author Ismail Simsek
 */
@Dependent
@Named("MaxBatchSizeWait")
public class MaxBatchSizeWait implements BatchSizeWait {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);

  @Inject BatchConfig config;
  @Inject DebeziumMetrics dbzMetrics;

  @Override
  public void initizalize() throws DebeziumException {
    assert config.batchSizeWaitWaitIntervalMs() < config.batchSizeWaitMaxWaitMs()
        : "`wait-interval-ms` cannot be bigger than `max-wait-ms`";
    assert config.batchSizeWaitQueueSizeInBytesRatio() > 0
            && config.batchSizeWaitQueueSizeInBytesRatio() <= 1.0
        : "`queue-size-in-bytes-ratio` must be between 0 (exclusive) and 1.0 (inclusive)";
  }

  public long maxQueueSizeInBytes() {
    try {
      long maxBytes =
          (long)
              DebeziumMetrics.mbeanServer.getAttribute(
                  dbzMetrics.getStreamingMetricsObjectName(), "MaxQueueSizeInBytes");
      if (maxBytes > 0) {
        return maxBytes;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to read MaxQueueSizeInBytes from Debezium MBean: {}", e.getMessage());
    }
    return config.sourceMaxQueueSizeInBytes();
  }

  public long streamingQueueCurrentSizeInBytes() {
    try {
      return (long)
          DebeziumMetrics.mbeanServer.getAttribute(
              dbzMetrics.getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
    } catch (Exception e) {
      LOGGER.warn("Failed to read CurrentQueueSizeInBytes from Debezium MBean: {}", e.getMessage());
      return 0L;
    }
  }

  @Override
  public void waitMs(Integer numRecordsProcessed, Integer processingTimeMs)
      throws InterruptedException {

    // don't wait if snapshot process is running
    if (dbzMetrics.snapshotRunning()) {
      return;
    }

    final long maxQueueSizeInBytes = maxQueueSizeInBytes();
    final double maxQueueSizeInBytesThreshold =
        maxQueueSizeInBytes * config.batchSizeWaitQueueSizeInBytesRatio();

    if (LOGGER.isDebugEnabled()) {
      long currentBytes = maxQueueSizeInBytes > 0 ? streamingQueueCurrentSizeInBytes() : 0L;
      LOGGER.debug(
          "Processed {}, QueueCurrentSize:{}, QueueTotalCapacity:{}, QueueCurrentSizeInBytes:{}, MaxQueueSizeInBytes:{}, SecondsBehindSource:{}, SnapshotCompleted:{}",
          numRecordsProcessed,
          dbzMetrics.streamingQueueCurrentSize(),
          config.sourceMaxQueueSize(),
          currentBytes,
          maxQueueSizeInBytes,
          (int) (dbzMetrics.streamingMilliSecondsBehindSource() / 1000),
          dbzMetrics.snapshotCompleted());
    }

    int totalWaitMs = 0;
    while (totalWaitMs < config.batchSizeWaitMaxWaitMs()
        && dbzMetrics.streamingQueueCurrentSize() < config.sourceMaxBatchSize()
        && (maxQueueSizeInBytes <= 0
            || streamingQueueCurrentSizeInBytes() < maxQueueSizeInBytesThreshold)) {
      totalWaitMs += config.batchSizeWaitWaitIntervalMs();
      if (LOGGER.isDebugEnabled()) {
        long currentBytes = maxQueueSizeInBytes > 0 ? streamingQueueCurrentSizeInBytes() : 0L;
        LOGGER.debug(
            "Sleeping {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}, QueueCurrentSizeInBytes:{} < maxQueueSizeInBytesThreshold:{}",
            config.batchSizeWaitWaitIntervalMs(),
            dbzMetrics.streamingQueueCurrentSize(),
            config.sourceMaxBatchSize(),
            currentBytes,
            maxQueueSizeInBytesThreshold);
      }

      Thread.sleep(config.batchSizeWaitWaitIntervalMs());
    }

    if (LOGGER.isDebugEnabled()) {
      long currentBytes = maxQueueSizeInBytes > 0 ? streamingQueueCurrentSizeInBytes() : 0L;
      LOGGER.debug(
          "Total wait {} Milliseconds, QueueCurrentSize:{}, maxBatchSize:{}, QueueCurrentSizeInBytes:{}, maxQueueSizeInBytesThreshold:{}",
          totalWaitMs,
          dbzMetrics.streamingQueueCurrentSize(),
          config.sourceMaxBatchSize(),
          currentBytes,
          maxQueueSizeInBytesThreshold);
    }
  }
}
