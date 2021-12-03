/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergChangeEvent;

import java.util.List;
import java.util.function.Predicate;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

public interface InterfaceIcebergTableOperator {

  void initialize();

  void addToTable(Table icebergTable, List<IcebergChangeEvent> events);

  Predicate<Record> filterEvents();
}
