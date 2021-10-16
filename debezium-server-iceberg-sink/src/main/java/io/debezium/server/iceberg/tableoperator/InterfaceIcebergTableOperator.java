/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.engine.ChangeEvent;

import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;

public interface InterfaceIcebergTableOperator {

  void initialize();

  void addToTable(Table icebergTable, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException;

  Predicate<Record> filterEvents();

  Table createIcebergTable(Catalog catalog,
                           TableIdentifier tableIdentifier,
                           ChangeEvent<Object, Object> event);

  Optional<Table> loadIcebergTable(Catalog catalog, TableIdentifier tableId);
}
