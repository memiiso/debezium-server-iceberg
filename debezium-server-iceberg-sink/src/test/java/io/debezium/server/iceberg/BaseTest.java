/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import io.debezium.server.iceberg.converter.EventFactory;
import io.debezium.server.iceberg.converter.JsonBuilder;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import jakarta.inject.Inject;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
  public JsonBuilder eventBuilder;
  @Inject
  public EventFactory eventFactory;
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

  public void printTableData(CloseableIterable<Record> data) throws IOException {
    try (CloseableIterable<Record> records = data) {
      List<Record> recordList = Lists.newArrayList(records);

      if (recordList.isEmpty()) {
        System.out.println("Table is empty.");
        return;
      }

      // Get schema from the first record
      Types.StructType schema = recordList.get(0).struct();
      List<String> headers = schema.fields().stream()
          .map(field -> String.format("%s (%s)", field.name(), field.type()))
          .collect(Collectors.toList());

      // Calculate column widths
      List<Integer> columnWidths = new ArrayList<>();
      for (String header : headers) {
        columnWidths.add(header.length());
      }

      // Adjust column widths based on data length
      for (Record record : recordList) {
        for (int i = 0; i < headers.size(); i++) {
          Object value = record.get(i);
          String valueStr = (value == null) ? "null" : value.toString();
          if (valueStr.length() > columnWidths.get(i)) {
            columnWidths.set(i, valueStr.length());
          }
        }
      }

      // Prepare format strings for printing
      StringBuilder formatBuilder = new StringBuilder("|");
      StringBuilder separatorBuilder = new StringBuilder("|");
      for (int width : columnWidths) {
        formatBuilder.append(" %-").append(width).append("s |");
        separatorBuilder.append("-".repeat(width + 2)).append("|");
      }
      String rowFormat = formatBuilder.toString();
      String separator = separatorBuilder.toString();

      // Print table header and separator
      System.out.println("\n" + String.format(rowFormat, headers.toArray()));
      System.out.println(separator);

      // Print table rows
      for (Record record : recordList) {
        Object[] values = new Object[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
          values[i] = record.get(i) == null ? "null" : record.get(i).toString();
        }
        System.out.println(String.format(rowFormat, values));
      }
      System.out.println();
    }
  }

  public CloseableIterable<Record> getTableDataV2(String catalog, String table) throws InterruptedException {
    String tableName = "debeziumcdc_" + table.replace(".", "_");
    return getTableDataV2(TableIdentifier.of(catalog, tableName));
  }

  public CloseableIterable<Record> getTableDataV2(TableIdentifier table) throws InterruptedException {
    Table iceTable = consumer.loadIcebergTable(table, null);
    return IcebergGenerics.read(iceTable).build();
  }

  public List<DataFile> getDataFiles(Table table) {
    table.refresh(); // Always refresh the table to get the latest metadata.

    List<DataFile> allDataFiles = new ArrayList<>();

    // The table.snapshots() method returns an Iterable<Snapshot>
    Iterable<Snapshot> snapshots = table.snapshots();

    for (Snapshot snapshot : snapshots) {
      // Get the Iterable of added data files for the current snapshot
      Iterable<DataFile> snapshotAddedFiles = snapshot.addedDataFiles(table.io());

      // Iterate over the files for this snapshot and add them to our main list
      for (DataFile dataFile : snapshotAddedFiles) {
        allDataFiles.add(dataFile);
      }
    }
    return allDataFiles;
  }

}