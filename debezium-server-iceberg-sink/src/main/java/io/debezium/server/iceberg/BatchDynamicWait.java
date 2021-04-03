/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import java.util.IntSummaryStatistics;
import java.util.LinkedList;
import javax.enterprise.context.Dependent;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Dependent
public class BatchDynamicWait {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchDynamicWait.class);

  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "2048")
  Integer maxBatchSize;

  @ConfigProperty(name = "debezium.source.poll.interval.ms", defaultValue = "1000")
  Integer pollIntervalMs;

  LinkedList<Integer> batchSizeHistory = new LinkedList<Integer>();
  LinkedList<Integer> sleepMsHistory = new LinkedList<Integer>();

  public BatchDynamicWait() {
    batchSizeHistory.add(1);
    batchSizeHistory.add(1);
    batchSizeHistory.add(1);
    sleepMsHistory.add(100);
    sleepMsHistory.add(100);
    sleepMsHistory.add(100);
  }

  private double getAverage(LinkedList<Integer> linkedList) {
    IntSummaryStatistics stats = linkedList.stream()
        .mapToInt((x) -> x)
        .summaryStatistics();
    return stats.getAverage();
  }

  public int getWaitMs(Integer numRecords) {
    batchSizeHistory.add(numRecords);
    batchSizeHistory.removeFirst();
    int sleepMs = 1;

    // if batchsize > XX% decrease wait
    if ((getAverage(batchSizeHistory) / maxBatchSize) >= 0.97) {
      sleepMs = (int) (sleepMsHistory.getLast() * 0.50);
    }
    // if batchsize > XX% decrease wait
    else if ((getAverage(batchSizeHistory) / maxBatchSize) >= 0.95) {
      sleepMs = (int) (sleepMsHistory.getLast() * 0.65);
    }
    // if batchsize > XX% decrease wait
    else if ((getAverage(batchSizeHistory) / maxBatchSize) >= 0.90) {
      sleepMs = (int) (sleepMsHistory.getLast() * 0.80);
    } else if ((getAverage(batchSizeHistory) / maxBatchSize) >= 0.85) {
      return sleepMsHistory.getLast();
    }
    // else increase
    else {
      sleepMs = (sleepMsHistory.getLast() * maxBatchSize) / numRecords;
    }

    sleepMsHistory.add(Math.min(Math.max(sleepMs, 100), pollIntervalMs));
    sleepMsHistory.removeFirst();

    LOGGER.debug("Calculating Wait delay\nmax.batch.size={}\npoll.interval.ms={}\nbatchSizeHistory{}\nsleepMsHistory" +
            "{}\nval{}",
        maxBatchSize,
        pollIntervalMs,
        batchSizeHistory, sleepMsHistory, sleepMsHistory.getLast());

    return sleepMsHistory.getLast();
  }

  public void waitMs(Integer numRecords, Integer processingTimeMs) throws InterruptedException {
    int sleepMs = Math.max(getWaitMs(numRecords) - processingTimeMs, 0);
    if (sleepMs > 2000) {
      LOGGER.info("Waiting {} ms", sleepMs);
      Thread.sleep(sleepMs);
    }
  }

}
