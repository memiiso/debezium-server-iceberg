/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.transaction;

import io.debezium.DebeziumException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BatchTransaction} implementation that stages updates across multiple Iceberg tables and
 * commits them atomically via {@link RESTCatalog#commitTransaction(List)}.
 */
public class MultiTableTransaction implements BatchTransaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiTableTransaction.class);

  private final RESTCatalog restCatalog;
  private final Map<TableIdentifier, Transaction> transactions = new ConcurrentHashMap<>();

  public MultiTableTransaction(RESTCatalog restCatalog) {
    this.restCatalog = restCatalog;
  }

  @Override
  public Table enlist(TableIdentifier tableId, Table icebergTable) {
    return transactions.computeIfAbsent(tableId, id -> icebergTable.newTransaction()).table();
  }

  @Override
  public void commit() {
    List<TableCommit> tableCommits = new ArrayList<>();
    for (Map.Entry<TableIdentifier, Transaction> entry : transactions.entrySet()) {
      TableIdentifier tableId = entry.getKey();
      Transaction tx = entry.getValue();
      if (tx instanceof BaseTransaction baseTx) {
        if (baseTx.startMetadata() != baseTx.currentMetadata()) {
          tableCommits.add(
              TableCommit.create(tableId, baseTx.startMetadata(), baseTx.currentMetadata()));
        }
      } else {
        throw new DebeziumException(
            "Unsupported Iceberg transaction type for multi-table commit: "
                + tx.getClass().getName());
      }
    }

    if (!tableCommits.isEmpty()) {
      LOGGER.debug("Committing multi-table transaction with {} table commits", tableCommits.size());
      restCatalog.commitTransaction(tableCommits);
      for (TableCommit tc : tableCommits) {
        restCatalog.invalidateTable(tc.identifier());
      }
    }
  }
}
