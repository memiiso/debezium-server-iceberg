/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.DebeziumException;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.IcebergChangeEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

/**
 * Wrapper to perform operations in iceberg tables
 *
 * @author Rafael Acevedo
 */
abstract class AbstractIcebergTableOperator implements InterfaceIcebergTableOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIcebergTableOperator.class);

  final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
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

  protected ArrayList<Record> toIcebergRecords(Schema schema, List<IcebergChangeEvent<Object, Object>> events) {

    ArrayList<Record> icebergRecords = new ArrayList<>();
    for (IcebergChangeEvent<Object, Object> e : events) {
      GenericRecord icebergRecord = e.getIcebergRecord(schema, valDeserializer.deserialize(e.destination(),
          getBytes(e.value())));
      icebergRecords.add(icebergRecord);
    }

    return icebergRecords;
  }

  FileFormat getFileFormat(Table icebergTable){
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  GenericAppenderFactory getAppender(Table icebergTable) {
    return new GenericAppenderFactory(
        icebergTable.schema(),
        icebergTable.spec(),
        Ints.toArray(icebergTable.schema().identifierFieldIds()),
        icebergTable.schema(),
        null);
  }
  
  protected DataFile getDataFile(Table icebergTable, ArrayList<Record> icebergRecords) {
    
    FileFormat fileFormat = getFileFormat(icebergTable);
    GenericAppenderFactory appender = getAppender(icebergTable);
    final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + fileFormat.name();
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));
    
    DataWriter<Record> dw = appender.newDataWriter(icebergTable.encryption().encrypt(out), fileFormat, null);
    
    icebergRecords.stream().filter(this.filterEvents()).forEach(dw::add);

    try {
      dw.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOGGER.debug("Creating iceberg DataFile!");
    return dw.toDataFile();
  }

}
