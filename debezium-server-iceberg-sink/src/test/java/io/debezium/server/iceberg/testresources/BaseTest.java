/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.IcebergChangeConsumer;

import jakarta.inject.Inject;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Integration test that uses spark to consumer data is consumed.
 *
 * @author Ismail Simsek
 */
public class BaseTest {

  @Inject
  IcebergChangeConsumer consumer;

  public CloseableIterable<Record> getTableDataV2(String table) {
    return getTableDataV2("debeziumevents", table);
  }

  public void printTableData(CloseableIterable<Record> data) {
    System.out.println("======================");
    System.out.println(data.iterator().next().struct());
    System.out.println("======================");
    data.forEach(System.out::println);
    System.out.println("======================");
  }

  public CloseableIterable<Record> getTableDataV2(String catalog, String table) {
    String tableName = "debeziumcdc_" + table.replace(".", "_");
    return getTableDataV2(TableIdentifier.of(catalog, tableName));
  }

  public CloseableIterable<Record> getTableDataV2(TableIdentifier table) {
    Table iceTable = consumer.loadIcebergTable(table, null);
    return IcebergGenerics.read(iceTable).build();
  }

}