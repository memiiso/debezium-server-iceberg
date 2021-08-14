/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
public interface InterfaceBatchSizeWait {

  default void initizalize() {
  }

  default void waitMs(Integer numRecordsProcessed, Integer processingTimeMs) throws InterruptedException{
  }

}
