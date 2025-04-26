/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.converter.TestChangeEventFactory;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import jakarta.inject.Inject;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;
import static org.mockito.Mockito.when;

/**
 * Integration test that uses spark to consumer data is consumed.
 *
 * @author Ismail Simsek
 */
public class BaseTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  @Inject
  public IcebergChangeConsumer consumer;
  @Inject
  public IcebergConfig icebergConfig;
  @Inject
  public DebeziumConfig debeziumConfig;
  @Inject
  public GlobalConfig config;
  @Inject
  public IcebergChangeEventBuilder eventBuilder;
  @Inject
  public TestChangeEventFactory eventFactory;
  @Inject
  public IcebergTableOperator icebergTableOperator;
  @ConfigProperty(name = "debezium.sink.type")
  public String sinkType;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  public String namespace;

  @BeforeAll
  static void setupBase() {
    LOGGER.debug("Setup Base Test");
    Awaitility.setDefaultTimeout(Duration.ofMinutes(3));
    Awaitility.setDefaultPollInterval(Duration.ofSeconds(6));
  }

  @BeforeEach
  void setupBaseBeforeEach() {
    when(consumer.config.debezium()).thenReturn(debeziumConfig);
    when(consumer.config.iceberg()).thenReturn(icebergConfig);
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