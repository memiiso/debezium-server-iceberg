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

  @Inject
  BatchSizeWaitConfig config;
  @Inject
  DebeziumMetrics dbzMetrics;

  @Override
  public void initizalize() throws DebeziumException {
    assert config.batchSizeWaitWaitIntervalMs() < config.batchSizeWaitMaxWaitMs() : "`wait-interval-ms` cannot be bigger than `max-wait-ms`";
  }

  @Override
  public void waitMs(Integer numRecordsProcessed, Integer processingTimeMs) throws InterruptedException {

    // don't wait if snapshot process is running
    if (dbzMetrics.snapshotRunning()) {
      return;
    }

    LOGGER.debug("Processed {}, QueueCurrentSize:{}, QueueTotalCapacity:{}, SecondsBehindSource:{}, SnapshotCompleted:{}",
        numRecordsProcessed,
        dbzMetrics.streamingQueueCurrentSize(),
        config.sourceMaxQueueSize(),
        (int) (dbzMetrics.streamingMilliSecondsBehindSource() / 1000),
        dbzMetrics.snapshotCompleted()
    );

    int totalWaitMs = 0;
    while (totalWaitMs < config.batchSizeWaitMaxWaitMs() && dbzMetrics.streamingQueueCurrentSize() < config.sourceMaxBatchSize()) {
      totalWaitMs += config.batchSizeWaitWaitIntervalMs();
      LOGGER.debug("Sleeping {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}",
          config.batchSizeWaitWaitIntervalMs(), dbzMetrics.streamingQueueCurrentSize(), config.sourceMaxBatchSize());

      Thread.sleep(config.batchSizeWaitWaitIntervalMs());
    }

    LOGGER.debug("Total wait {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}",
        totalWaitMs, dbzMetrics.streamingQueueCurrentSize(), config.sourceMaxBatchSize());

  }

}
