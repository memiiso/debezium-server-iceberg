/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.IcebergUtil;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper to perform operations in iceberg tables
 *
 * @author Rafael Acevedo
 */
abstract class AbstractIcebergTableOperator implements InterfaceIcebergTableOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIcebergTableOperator.class);
  Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  Deserializer<JsonNode> valDeserializer;

  @Override
  public void initialize() {
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
  }

  protected byte[] getBytes(Object object) {
    if (object instanceof byte[]) {
      return (byte[]) object;
    } else if (object instanceof String) {
      return ((String) object).getBytes();
    }
    throw new DebeziumException(unsupportedTypeMessage(object));
  }

  protected String getString(Object object) {
    if (object instanceof String) {
      return (String) object;
    }
    throw new DebeziumException(unsupportedTypeMessage(object));
  }

  protected String unsupportedTypeMessage(Object object) {
    final String type = (object == null) ? "null" : object.getClass().getName();
    return "Unexpected data type '" + type + "'";
  }

  protected ArrayList<Record> toIcebergRecords(Schema schema, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException {

    ArrayList<Record> icebergRecords = new ArrayList<>();
    for (ChangeEvent<Object, Object> e : events) {
      GenericRecord icebergRecord = IcebergUtil.getIcebergRecord(schema, valDeserializer.deserialize(e.destination(),
          getBytes(e.value())));
      icebergRecords.add(icebergRecord);
    }

    return icebergRecords;
  }

  protected DataFile getDataFile(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

    FileAppender<Record> writer;
    List<Record> newRecords = icebergRecords.stream()
        .filter(this.filterEvents()).collect(Collectors.toList());
    try {
      LOGGER.debug("Writing data to file: {}!", out);
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(icebergTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        writer.addAll(newRecords);
      }

    } catch (IOException e) {
      throw new InterruptedException(e.getMessage());
    }

    LOGGER.debug("Creating iceberg DataFile!");
    return DataFiles.builder(icebergTable.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  public Optional<Table> loadTable(Catalog catalog, TableIdentifier tableId) {
    try {
      Table table = catalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.warn("table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }
}
