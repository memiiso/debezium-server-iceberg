/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.transaction;

import io.debezium.runtime.BatchEvent;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Abstraction for a batch-level transaction across one or more Iceberg tables and offset storage.
 */
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
   * Stages or commits the batch offsets for the given Debezium batch records.
   *
   * @param records the Debezium batch events consumed in this batch
   */
  void commitOffset(List<BatchEvent> records);

  /** Commits all enlisted table changes and offset updates atomically. */
  void commit();

  @Override
  void close();
}
