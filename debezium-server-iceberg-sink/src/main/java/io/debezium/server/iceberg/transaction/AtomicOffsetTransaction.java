/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.transaction;

import io.debezium.server.iceberg.offset.IcebergOffsetBackingStore;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * {@link MultiTableTransaction} also committing the Debezium offset of the batch, in the offset
 * table of {@link IcebergOffsetBackingStore}.
 *
 * <p>The data tables and the offset are committed in one atomic catalog transaction: either the
 * batch and its offset are committed, or none of them. A failed batch is replayed on restart from
 * the previous offset, without duplicates (exactly-once).
 */
public class AtomicOffsetTransaction extends MultiTableTransaction {

  private final IcebergOffsetBackingStore offsetStore;
  private final List<SourceRecord> records;
  private IcebergOffsetBackingStore.PendingOffset pendingOffset;

  /**
   * @param restCatalog the catalog holding the data tables and the offset table
   * @param maxRetries number of retries on commit conflict
   * @param offsetStore the offset store started by the Debezium engine
   * @param records all the records of the batch in source order, their offsets are committed
   */
  public AtomicOffsetTransaction(
      RESTCatalog restCatalog,
      int maxRetries,
      IcebergOffsetBackingStore offsetStore,
      List<SourceRecord> records) {
    super(restCatalog, maxRetries);
    this.offsetStore = offsetStore;
    this.records = records;
  }

  @Override
  protected Map<TableIdentifier, Transaction> additionalTransactions(boolean reload) {
    if (pendingOffset == null) {
      pendingOffset = offsetStore.prepare(records);
      // no transaction references the offset file until it is applied, e.g. when loading the
      // offset table fails on the first attempt
      trackWrittenFiles(offsetStore.offsetTable().io(), pendingOffset.files());
    }
    // loaded from the sink catalog on every attempt, it is the catalog committing the transaction
    TableIdentifier offsetTableId = offsetStore.tableIdentifier();
    Transaction tx = restCatalog.loadTable(offsetTableId).newTransaction();
    pendingOffset.applyTo(tx);
    return Map.of(offsetTableId, tx);
  }

  @Override
  protected void afterCommitted() {
    offsetStore.committed(pendingOffset);
  }
}
