/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperators;

import io.debezium.engine.ChangeEvent;

import java.util.ArrayList;

import org.apache.iceberg.Table;

public interface InterfaceIcebergTableOperator {

  default void initialize(){
  }
  
  void addToTable(Table icebergTable, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException;

}
