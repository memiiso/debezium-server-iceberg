/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.transaction;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link BatchTransaction} implementation used when multi-table transactions are disabled.
 * Operations are committed directly per table as they are processed.
 */
public class NoOpBatchTransaction implements BatchTransaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(NoOpBatchTransaction.class);

  @Override
  public Table enlist(TableIdentifier tableId, Table icebergTable) {
    return icebergTable;
  }

  /** The table changes are already committed, the action runs right away. */
  @Override
  public void onCommit(Runnable action) {
    runAction(action);
  }

  @Override
  public void commit() {
    // No-op: each table commit is handled independently
  }

  static void runAction(Runnable action) {
    try {
      action.run();
    } catch (Exception e) {
      LOGGER.warn("Post commit action failed (non-critical)", e);
    }
  }
}
