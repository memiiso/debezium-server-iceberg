/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.offset;

import io.debezium.DebeziumException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits the data files of several tables and the Debezium offset in one atomic transaction, using
 * the REST catalog endpoint {@code POST /v1/{prefix}/transactions/commit}.
 *
 * <p>Either all the tables and the offset are committed, or none of them. A batch that failed is
 * replayed on restart from the previous offset, without duplicates.
 */
public class AtomicOffsetCommitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicOffsetCommitter.class);
  private final RESTCatalog catalog;
  private final String catalogUri;
  private final int maxRetries;
  // offset store already checked to be in the sink catalog
  private IcebergOffsetBackingStore validatedStore;

  /**
   * @param catalog the sink catalog, holding the data tables and the offset table
   * @param catalogUri uri of the sink catalog
   * @param maxRetries number of retries on commit conflict
   */
  public AtomicOffsetCommitter(RESTCatalog catalog, String catalogUri, int maxRetries) {
    this.catalog = catalog;
    this.catalogUri = catalogUri;
    this.maxRetries = maxRetries;
  }

  /**
   * Files written to a table, waiting to be committed.
   *
   * @param id table identifier
   * @param table the table the files are written for
   * @param files written files, committed in order
   * @param numEvents number of events written
   */
  public record PendingTable(
      TableIdentifier id, Table table, List<WriteResult> files, int numEvents) {}

  /**
   * Commits the tables and the offsets of the records in one transaction.
   *
   * <p>On conflict, the tables are reloaded and the commit is retried with the same files. The
   * files are deleted when the commit fails for sure, and kept when its outcome is unknown.
   *
   * @param tables files to commit per table
   * @param records all the records of the batch, in source order
   */
  public void commit(Collection<PendingTable> tables, List<SourceRecord> records) {
    IcebergOffsetBackingStore offsetStore = offsetStore();
    IcebergOffsetBackingStore.PendingOffset pendingOffset;
    try {
      pendingOffset = offsetStore.prepare(records);
    } catch (RuntimeException e) {
      abort(tables);
      throw e;
    }

    for (int attempt = 1; ; attempt++) {
      try {
        List<TableCommit> commits = new ArrayList<>();
        for (PendingTable pending : tables) {
          commits.add(tableCommit(pending.id(), tx -> pending.files().forEach(f -> apply(tx, f))));
        }
        commits.add(tableCommit(offsetStore.tableIdentifier(), pendingOffset::applyTo));
        catalog.commitTransaction(commits);
        LOGGER.debug("Committed {} tables and the offset in one transaction", tables.size());
        break;
      } catch (CommitFailedException e) {
        if (attempt <= maxRetries) {
          LOGGER.warn(
              "Transaction conflict, retrying commit, attempt {}/{}: {}",
              attempt,
              maxRetries,
              e.getMessage());
          continue;
        }
        abort(tables);
        offsetStore.abort(pendingOffset);
        throw new DebeziumException("Failed to commit transaction after " + attempt + " tries", e);
      } catch (RuntimeException e) {
        // UnsupportedOperationException: the catalog server does not support transactions
        if (e instanceof CleanableFailure || e instanceof UnsupportedOperationException) {
          abort(tables);
          offsetStore.abort(pendingOffset);
        }
        throw new DebeziumException("Failed to commit transaction", e);
      }
    }
    offsetStore.committed(pendingOffset);
  }

  /**
   * Deletes the files written for tables that were not committed.
   *
   * @param tables files written per table
   */
  public void abort(Collection<PendingTable> tables) {
    for (PendingTable pending : tables) {
      pending.files().forEach(f -> deleteFiles(pending.table(), f));
    }
  }

  /**
   * @return the offset store started by the Debezium engine, checked to be in the sink catalog
   */
  private IcebergOffsetBackingStore offsetStore() {
    IcebergOffsetBackingStore offsetStore =
        IcebergOffsetBackingStore.instance()
            .orElseThrow(() -> new DebeziumException("Iceberg offset storage is not started!"));
    if (offsetStore != validatedStore) {
      if (!Objects.equals(offsetStore.catalogUri(), catalogUri)) {
        throw new DebeziumException(
            "Offset storage catalog `"
                + offsetStore.catalogUri()
                + "` must be the sink catalog `"
                + catalogUri
                + "` to commit the offset atomically with the data");
      }
      validatedStore = offsetStore;
    }
    return offsetStore;
  }

  private TableCommit tableCommit(TableIdentifier id, Consumer<Transaction> changes) {
    Table table = catalog.loadTable(id);
    Transaction tx = table.newTransaction();
    changes.accept(tx);
    BaseTransaction baseTx = (BaseTransaction) tx;
    return TableCommit.create(id, baseTx.startMetadata(), baseTx.currentMetadata());
  }

  private static void apply(Transaction tx, WriteResult files) {
    if (files.deleteFiles().length > 0) {
      RowDelta newRowDelta = tx.newRowDelta();
      Arrays.stream(files.dataFiles()).forEach(newRowDelta::addRows);
      Arrays.stream(files.deleteFiles()).forEach(newRowDelta::addDeletes);
      newRowDelta.commit();
    } else {
      AppendFiles appendFiles = tx.newAppend();
      Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
      appendFiles.commit();
    }
  }

  private static void deleteFiles(Table table, WriteResult files) {
    Stream.<ContentFile<?>>concat(
            Arrays.stream(files.dataFiles()), Arrays.stream(files.deleteFiles()))
        .forEach(
            f -> {
              try {
                table.io().deleteFile(f.location());
              } catch (Exception e) {
                LOGGER.warn("Failed to delete uncommitted file {}", f.location(), e);
              }
            });
  }
}
