/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import javax.enterprise.context.Dependent;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Dependent
public interface InterfaceBatchSizeWait {

  default void initizalize() {
  }

  void waitMs(Integer numRecordsProcessed, Integer processingTimeMs) throws InterruptedException;

}
