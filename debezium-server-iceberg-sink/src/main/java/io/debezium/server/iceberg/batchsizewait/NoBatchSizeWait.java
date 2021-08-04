/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.batchsizewait;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

/**
 * Optimizes batch size around 85%-90% of max,batch.size using dynamically calculated sleep(ms)
 *
 * @author Ismail Simsek
 */
@Dependent
@Named("NoBatchSizeWait")
public class NoBatchSizeWait implements InterfaceBatchSizeWait {

  public void waitMs(Integer numRecordsProcessed, Integer processingTimeMs) {
  }

}
