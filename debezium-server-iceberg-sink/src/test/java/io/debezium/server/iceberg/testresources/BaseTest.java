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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;

import java.time.Duration;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;

/**
 * Integration test that uses spark to consumer data is consumed.
 *
 * @author Ismail Simsek
 */
public class BaseTest {

  @Inject
  public IcebergChangeConsumer consumer;

  @BeforeAll
  static void setup() {
    Awaitility.setDefaultTimeout(Duration.ofMinutes(3));
    Awaitility.setDefaultPollInterval(Duration.ofSeconds(6));
  }

  public CloseableIterable<Record> getTableDataV2(String table) throws InterruptedException {
    return getTableDataV2(ICEBERG_CATALOG_TABLE_NAMESPACE, table);
  }

  public void printTableData(CloseableIterable<Record> data) {
    System.out.println("======================");
    System.out.println(data.iterator().next().struct());
    System.out.println("======================");
    data.forEach(System.out::println);
    System.out.println("======================");
  }

  public CloseableIterable<Record> getTableDataV2(String catalog, String table) throws InterruptedException {
    String tableName = "debeziumcdc_" + table.replace(".", "_");
    return getTableDataV2(TableIdentifier.of(catalog, tableName));
  }

  public CloseableIterable<Record> getTableDataV2(TableIdentifier table) throws InterruptedException {
    Table iceTable = consumer.loadIcebergTable(table, null);
    return IcebergGenerics.read(iceTable).build();
  }

}