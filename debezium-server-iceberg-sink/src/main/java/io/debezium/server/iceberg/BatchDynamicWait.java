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
 * Optimizes batch size around 85%-90% of max,batch.size using dynamically calculated sleep(ms)
 *
 * @author Ismail Simsek
 */
@Dependent
public class BatchDynamicWait {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchDynamicWait.class);

  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "2048")
  Integer maxBatchSize;

  @ConfigProperty(name = "debezium.sink.batch.dynamic-wait.max-wait-ms", defaultValue = "300000")
  Integer maxWaitMs;

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

    sleepMsHistory.add(Math.min(Math.max(sleepMs, 100), maxWaitMs));
    sleepMsHistory.removeFirst();

    LOGGER.debug("Calculating Wait delay\nmax.batch.size={}\npoll.interval.ms={}\nbatchSizeHistory{}\nsleepMsHistory" +
            "{}\nval{}",
        maxBatchSize,
        maxWaitMs,
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
