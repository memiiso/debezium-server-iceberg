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

/**
 * Default {@link BatchTransaction} implementation used when multi-table transactions are disabled.
 * Operations are committed directly per table as they are processed.
 */
public class NoOpBatchTransaction implements BatchTransaction {

  @Override
  public Table enlist(TableIdentifier tableId, Table icebergTable) {
    return icebergTable;
  }

  @Override
  public void commit() {
    // No-op: each table commit is handled independently
  }
}
