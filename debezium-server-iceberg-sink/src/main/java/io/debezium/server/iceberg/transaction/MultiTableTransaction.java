/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.transaction;

import io.debezium.DebeziumException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BatchTransaction} implementation that stages updates across multiple Iceberg tables and
 * commits them atomically via {@link RESTCatalog#commitTransaction(List)}.
 *
 * <p>On commit conflict (e.g. a concurrent compaction), the tables are reloaded and the staged
 * changes are re-applied on top of them, reusing the written data files, and the manifests of the
 * rejected attempts are deleted once committed. When the transaction is not committed, the written
 * files are deleted, unless the outcome of the commit is unknown.
 */
public class MultiTableTransaction implements BatchTransaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiTableTransaction.class);

  protected final RESTCatalog restCatalog;
  private final int maxRetries;
  private final Map<TableIdentifier, Transaction> transactions = new ConcurrentHashMap<>();
  private final List<Runnable> onCommitActions = new CopyOnWriteArrayList<>();
  // every transaction built to commit the batch, their files are deleted when not committed
  private final List<Transaction> attempts = new ArrayList<>();
  // files written for the batch outside of the transactions, deleted with the files of the attempts
  private final Map<String, FileIO> trackedFiles = new LinkedHashMap<>();
  private boolean committed = false;
  // a commit request failed without telling whether it was applied, the files must be kept
  private boolean outcomeUnknown = false;

  /**
   * @param restCatalog the catalog committing the transaction
   * @param maxRetries number of retries on commit conflict
   */
  public MultiTableTransaction(RESTCatalog restCatalog, int maxRetries) {
    this.restCatalog = restCatalog;
    this.maxRetries = maxRetries;
  }

  @Override
  public Table enlist(TableIdentifier tableId, Table icebergTable) {
    return transactions.computeIfAbsent(tableId, id -> icebergTable.newTransaction()).table();
  }

  @Override
  public void onCommit(Runnable action) {
    onCommitActions.add(action);
  }

  @Override
  public void commit() {
    Map<TableIdentifier, BaseTransaction> staged = new LinkedHashMap<>();
    for (Map.Entry<TableIdentifier, Transaction> entry : transactions.entrySet()) {
      BaseTransaction baseTx = asBaseTransaction(entry.getValue());
      attempts.add(baseTx);
      if (baseTx.startMetadata() != baseTx.currentMetadata()) {
        staged.put(entry.getKey(), baseTx);
      }
    }

    List<BaseTransaction> committedTxs = List.of();
    for (int attempt = 1; ; attempt++) {
      boolean retry = attempt > 1;
      Map<TableIdentifier, Transaction> attemptTxs = new LinkedHashMap<>();
      staged.forEach((id, tx) -> attemptTxs.put(id, retry ? rebase(id, tx) : tx));
      attemptTxs.putAll(additionalTransactions(retry));

      List<TableCommit> tableCommits = new ArrayList<>();
      List<BaseTransaction> attemptBaseTxs = new ArrayList<>();
      attemptTxs.forEach(
          (id, tx) -> {
            BaseTransaction baseTx = asBaseTransaction(tx);
            // the staged and rebased transactions are already tracked, the additional ones not
            if (!attempts.contains(baseTx)) {
              attempts.add(baseTx);
            }
            attemptBaseTxs.add(baseTx);
            tableCommits.add(
                TableCommit.create(id, baseTx.startMetadata(), baseTx.currentMetadata()));
          });
      if (tableCommits.isEmpty()) {
        break;
      }

      try {
        LOGGER.debug(
            "Committing multi-table transaction with {} table commits", tableCommits.size());
        restCatalog.commitTransaction(tableCommits);
        tableCommits.forEach(tc -> restCatalog.invalidateTable(tc.identifier()));
        committedTxs = attemptBaseTxs;
        break;
      } catch (CommitFailedException e) {
        if (attempt > maxRetries) {
          throw new DebeziumException(
              "Failed to commit multi-table transaction after " + attempt + " attempts", e);
        }
        LOGGER.warn(
            "Multi-table transaction conflict, retrying commit {}/{}: {}",
            attempt,
            maxRetries,
            e.getMessage());
      } catch (RuntimeException e) {
        // UnsupportedOperationException: the server does not support the transactions endpoint
        if (!(e instanceof CleanableFailure || e instanceof UnsupportedOperationException)) {
          outcomeUnknown = true;
        }
        throw e;
      }
    }

    committed = true;
    afterCommitted();
    onCommitActions.forEach(NoOpBatchTransaction::runAction);
    deleteRejectedAttempts(committedTxs);
  }

  @Override
  public void close() {
    if (committed) {
      return;
    }
    if (outcomeUnknown) {
      LOGGER.warn("Outcome of the multi-table transaction is unknown, keeping the written files");
      return;
    }
    // the transactions created by enlist and not committed yet
    transactions.values().stream()
        .map(MultiTableTransaction::asBaseTransaction)
        .filter(tx -> !attempts.contains(tx))
        .forEach(attempts::add);
    Map<String, FileIO> files = writtenFiles(attempts, true);
    trackedFiles.forEach(files::putIfAbsent);
    deleteFiles(files);
  }

  /**
   * Registers files written for the batch before a transaction references them. Like the files of
   * the transactions, they are deleted when the transaction is not committed, unless the outcome of
   * the commit is unknown.
   *
   * @param io the file io that wrote the files
   * @param files the written data and delete files
   */
  protected void trackWrittenFiles(FileIO io, WriteResult files) {
    Arrays.stream(files.dataFiles()).forEach(f -> trackedFiles.putIfAbsent(f.location(), io));
    Arrays.stream(files.deleteFiles()).forEach(f -> trackedFiles.putIfAbsent(f.location(), io));
  }

  /**
   * Deletes the manifest and manifest list files written by the attempts rejected before the
   * commit. Their data and delete files are kept, the committed attempt reuses them.
   */
  private void deleteRejectedAttempts(List<BaseTransaction> committedTxs) {
    List<Transaction> rejected =
        attempts.stream().filter(tx -> !committedTxs.contains(tx)).toList();
    Map<String, FileIO> rejectedFiles = writtenFiles(rejected, false);
    if (rejectedFiles.isEmpty()) {
      return;
    }
    // safety net: no-op today, retries write new manifests, but deleting a file the committed
    // attempt references would corrupt the table
    rejectedFiles.keySet().removeAll(writtenFiles(committedTxs, false).keySet());
    LOGGER.debug("Deleting {} files of the rejected commit attempts", rejectedFiles.size());
    deleteFiles(rejectedFiles);
  }

  /**
   * Tables to commit in the same transaction as the enlisted tables.
   *
   * @param reload true on retry after a commit conflict, the tables must be reloaded
   * @return transactions holding the changes, per table
   */
  protected Map<TableIdentifier, Transaction> additionalTransactions(boolean reload) {
    return Map.of();
  }

  /** Called once the transaction is committed, before the {@link #onCommit} actions. */
  protected void afterCommitted() {}

  /**
   * Re-applies the changes staged by the batch on top of the current table, reusing the written
   * data and delete files.
   */
  private Transaction rebase(TableIdentifier tableId, BaseTransaction staged) {
    TableMetadata start = staged.startMetadata();
    TableMetadata current = staged.currentMetadata();
    Table table = restCatalog.loadTable(tableId);
    BaseTransaction tx = asBaseTransaction(table.newTransaction());
    // tracked before replaying, the manifests it writes are deleted if a later step fails, e.g.
    // replaying another table or the additional transactions
    attempts.add(tx);

    // the data files are written with the field ids assigned by the staged schema changes, they
    // can only be replayed on the same schema
    boolean schemaChanged = current.currentSchemaId() != start.currentSchemaId();
    if (schemaChanged && table.schema().schemaId() != start.currentSchemaId()) {
      throw new DebeziumException(
          "Schema of table " + tableId + " changed concurrently, cannot retry the commit");
    }

    FileIO io = staged.table().io();
    for (Snapshot snapshot : stagedSnapshots(start, current)) {
      // the batch writes the events grouped by schema, each snapshot is replayed under the schema
      // it was staged with, in the same order
      if (schemaChanged && snapshot.schemaId() != null) {
        replaySchema(tx, tableId, current.schemasById().get(snapshot.schemaId()));
      }
      List<DataFile> dataFiles = Lists.newArrayList(snapshot.addedDataFiles(io));
      List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(io));
      if (!snapshot.removedDataFiles(io).iterator().hasNext()
          && !snapshot.removedDeleteFiles(io).iterator().hasNext()) {
        if (deleteFiles.isEmpty()) {
          AppendFiles append = tx.newAppend();
          dataFiles.forEach(append::appendFile);
          append.commit();
        } else {
          RowDelta rowDelta = tx.newRowDelta();
          dataFiles.forEach(rowDelta::addRows);
          deleteFiles.forEach(rowDelta::addDeletes);
          rowDelta.commit();
        }
      } else {
        throw new DebeziumException(
            "Snapshot of table " + tableId + " removes files, cannot retry the commit");
      }
    }
    if (schemaChanged) {
      // a schema change staged after the last snapshot
      replaySchema(tx, tableId, current.schema());
    }
    return tx;
  }

  /** Applies the staged schema to the transaction, with the field ids it was staged with. */
  private static void replaySchema(Transaction tx, TableIdentifier tableId, Schema stagedSchema) {
    if (tx.table().schema().sameSchema(stagedSchema)) {
      return;
    }
    tx.updateSchema()
        .unionByNameWith(stagedSchema)
        .setIdentifierFields(stagedSchema.identifierFieldNames())
        .commit();
    if (!tx.table().schema().sameSchema(stagedSchema)) {
      throw new DebeziumException(
          "Schema change of table " + tableId + " cannot be replayed, cannot retry the commit");
    }
  }

  /** The snapshots added by the transaction, oldest first. */
  private static List<Snapshot> stagedSnapshots(TableMetadata start, TableMetadata current) {
    Long startSnapshotId =
        start.currentSnapshot() == null ? null : start.currentSnapshot().snapshotId();
    Deque<Snapshot> snapshots = new ArrayDeque<>();
    Snapshot snapshot = current.currentSnapshot();
    while (snapshot != null && !Objects.equals(snapshot.snapshotId(), startSnapshotId)) {
      snapshots.addFirst(snapshot);
      snapshot = snapshot.parentId() == null ? null : current.snapshot(snapshot.parentId());
    }
    return new ArrayList<>(snapshots);
  }

  /**
   * Lists the files written by the snapshots the transactions added. The retries reuse the data and
   * delete files of the first attempt.
   *
   * @param contentFiles true to list the data and delete files too, false for the manifest and
   *     manifest list files only
   * @return the file locations, with the file io to delete them
   */
  private static Map<String, FileIO> writtenFiles(
      List<? extends Transaction> txs, boolean contentFiles) {
    Map<String, FileIO> locations = new LinkedHashMap<>();
    for (Transaction tx : txs) {
      BaseTransaction baseTx = asBaseTransaction(tx);
      FileIO io = baseTx.table().io();
      try {
        for (Snapshot snapshot :
            stagedSnapshots(baseTx.startMetadata(), baseTx.currentMetadata())) {
          if (contentFiles) {
            snapshot.addedDataFiles(io).forEach(f -> locations.putIfAbsent(f.location(), io));
            snapshot.addedDeleteFiles(io).forEach(f -> locations.putIfAbsent(f.location(), io));
          }
          snapshot.allManifests(io).stream()
              .filter(m -> Objects.equals(m.snapshotId(), snapshot.snapshotId()))
              .forEach(m -> locations.putIfAbsent(m.path(), io));
          if (snapshot.manifestListLocation() != null) {
            locations.putIfAbsent(snapshot.manifestListLocation(), io);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to list the files written for table {}", baseTx.table().name(), e);
      }
    }
    return locations;
  }

  /** Deletes the files, failures are logged and ignored, the files are then orphan files. */
  private static void deleteFiles(Map<String, FileIO> locations) {
    locations.forEach(
        (location, io) -> {
          try {
            io.deleteFile(location);
          } catch (Exception e) {
            LOGGER.warn("Failed to delete uncommitted file {}", location, e);
          }
        });
  }

  private static BaseTransaction asBaseTransaction(Transaction tx) {
    if (tx instanceof BaseTransaction baseTx) {
      return baseTx;
    }
    throw new DebeziumException(
        "Unsupported Iceberg transaction type for multi-table commit: " + tx.getClass().getName());
  }
}
