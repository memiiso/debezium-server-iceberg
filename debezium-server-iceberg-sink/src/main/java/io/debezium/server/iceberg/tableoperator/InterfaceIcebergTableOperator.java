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
import java.util.function.Predicate;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

public interface InterfaceIcebergTableOperator {

  void initialize();

  void addToTable(Table icebergTable, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException;
  Predicate<Record> filterEvents();

}
