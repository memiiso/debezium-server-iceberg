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

/** Abstraction for a batch-level transaction across one or more Iceberg tables. */
public interface BatchTransaction extends AutoCloseable {

  /**
   * Enlists the given table in this batch transaction and returns the {@link Table} view to write
   * against.
   *
   * @param tableId the table identifier in the catalog
   * @param icebergTable the loaded Iceberg table
   * @return the table instance to pass to {@code IcebergTableOperator}
   */
  Table enlist(TableIdentifier tableId, Table icebergTable);

  /**
   * Registers an action to run once the batch is committed, e.g. logging or lineage events. Not run
   * when the commit fails.
   *
   * @param action the action, its failures are logged and ignored
   */
  void onCommit(Runnable action);

  /** Commits all enlisted table changes atomically, then runs the {@link #onCommit} actions. */
  void commit();

  /** Releases the transaction, deleting the files written when it was not committed. */
  @Override
  default void close() {}
}
