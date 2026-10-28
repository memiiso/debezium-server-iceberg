/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.DebeziumException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits the data files of several tables and the Debezium offset in one atomic transaction, using
 * the REST catalog endpoint {@code POST /v1/{prefix}/transactions/commit}.
 *
 * <p>Either all the tables and the offset are committed, or none of them. A batch that failed is
 * replayed on restart from the previous offset, without duplicates.
 */
public class MultiTableCommitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiTableCommitter.class);
  private final RESTCatalog catalog;
  private final int maxRetries;

  public MultiTableCommitter(RESTCatalog catalog, int maxRetries) {
    this.catalog = catalog;
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
   * Commits the tables and replaces the content of the offset table, in one transaction.
   *
   * <p>On conflict, the tables are reloaded and the commit is retried with the same files. The
   * files are deleted when the commit fails for sure, and kept when its outcome is unknown.
   *
   * @param tables files to commit per table
   * @param offsetTable offset table identifier
   * @param offsetFiles data file holding the new offset row
   */
  public void commit(
      Collection<PendingTable> tables, TableIdentifier offsetTable, WriteResult offsetFiles) {
    for (int attempt = 1; ; attempt++) {
      try {
        List<TableCommit> commits = new ArrayList<>();
        for (PendingTable pending : tables) {
          commits.add(tableCommit(pending.id(), tx -> pending.files().forEach(f -> apply(tx, f))));
        }
        commits.add(
            tableCommit(
                offsetTable,
                tx -> {
                  OverwriteFiles overwrite = tx.newOverwrite();
                  overwrite.overwriteByRowFilter(Expressions.alwaysTrue());
                  Arrays.stream(offsetFiles.dataFiles()).forEach(overwrite::addFile);
                  overwrite.commit();
                }));
        catalog.commitTransaction(commits);
        LOGGER.debug("Committed {} tables and the offset in one transaction", tables.size());
        return;
      } catch (CommitFailedException e) {
        if (attempt <= maxRetries) {
          LOGGER.warn(
              "Transaction conflict, retrying commit, attempt {}/{}: {}",
              attempt,
              maxRetries,
              e.getMessage());
          continue;
        }
        abort(tables, offsetTable, offsetFiles);
        throw new DebeziumException("Failed to commit transaction after " + attempt + " tries", e);
      } catch (RuntimeException e) {
        // UnsupportedOperationException: the catalog server does not support transactions
        if (e instanceof CleanableFailure || e instanceof UnsupportedOperationException) {
          abort(tables, offsetTable, offsetFiles);
        }
        throw new DebeziumException("Failed to commit transaction", e);
      }
    }
  }

  /**
   * Deletes the uncommitted files.
   *
   * @param tables files written per table
   * @param offsetTable offset table identifier, null when no offset file was written
   * @param offsetFiles data file holding the new offset row, null when not written
   */
  public void abort(
      Collection<PendingTable> tables, TableIdentifier offsetTable, WriteResult offsetFiles) {
    for (PendingTable pending : tables) {
      pending.files().forEach(f -> deleteFiles(pending.table().io(), f));
    }
    if (offsetFiles != null) {
      try {
        deleteFiles(catalog.loadTable(offsetTable).io(), offsetFiles);
      } catch (Exception e) {
        LOGGER.warn("Failed to delete uncommitted offset files", e);
      }
    }
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

  private static void deleteFiles(FileIO io, WriteResult files) {
    Stream.<ContentFile<?>>concat(
            Arrays.stream(files.dataFiles()), Arrays.stream(files.deleteFiles()))
        .forEach(
            f -> {
              try {
                io.deleteFile(f.location());
              } catch (Exception e) {
                LOGGER.warn("Failed to delete uncommitted file {}", f.location(), e);
              }
            });
  }
}
