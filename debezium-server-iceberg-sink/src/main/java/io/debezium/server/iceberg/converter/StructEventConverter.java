/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import io.debezium.DebeziumException;
import io.debezium.converters.iceberg.IcebergConverterConfig;
import io.debezium.converters.iceberg.IcebergStructConverter;
import io.debezium.converters.iceberg.StructSchemaConverter;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.runtime.BatchEvent;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin sink {@link EventConverter} adapter over {@link IcebergStructConverter}
 * and {@link
 * StructSchemaConverter}. Extracts sink-specific CDC metadata ({@code __op},
 * dedup timestamp,
 * {@code isNewKey}) and wraps converted {@link GenericRecord} instances into
 * {@link RecordWrapper}.
 *
 * @author Ismail Simsek
 */
public class StructEventConverter implements EventConverter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(StructEventConverter.class);
  protected final GlobalConfig config;
  protected final String destination;
  private final Struct value;
  private final Struct key;
  private boolean newKey = false;
  private final StructSchemaConverter schemaConverter;
  private final IcebergStructConverter structConverter;

  public StructEventConverter(EmbeddedEngineChangeEvent e, GlobalConfig config) {
    this(e.destination(), e.sourceRecord(), config);
  }

  public StructEventConverter(BatchEvent e, GlobalConfig config) {
    this(e.destination(), e.record(), config);
  }

  public StructEventConverter(
      BatchEvent e,
      GlobalConfig config,
      IcebergConverterConfig converterConfig,
      IcebergStructConverter structConverter) {
    this(e.destination(), e.record(), config, converterConfig, structConverter);
  }

  public StructEventConverter(String destination, SourceRecord sourceRecord, GlobalConfig config) {
    this(
        destination,
        sourceRecord,
        config,
        SinkIcebergConverterConfig.from(Objects.requireNonNull(config, "config must not be null")),
        null);
  }

  public StructEventConverter(
      String destination,
      SourceRecord sourceRecord,
      GlobalConfig config,
      IcebergConverterConfig converterConfig,
      IcebergStructConverter structConverter) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    IcebergConverterConfig effectiveConfig =
        Objects.requireNonNull(converterConfig, "converterConfig must not be null");
    this.structConverter =
        structConverter != null ? structConverter : new IcebergStructConverter(effectiveConfig, 0);
    this.destination = destination;
    if (sourceRecord == null) {
      throw new DebeziumException(
          "Unexpected event type: sourceRecord is null!, expected SourceRecord value.");
    }
    if (sourceRecord.value() != null && !(sourceRecord.value() instanceof Struct)) {
      throw new DebeziumException(
          "Unexpected value type: "
              + sourceRecord.value().getClass().getName()
              + ", expected Struct.");
    }
    if (sourceRecord.key() != null && !(sourceRecord.key() instanceof Struct)) {
      throw new DebeziumException(
          "Unexpected key type: " + sourceRecord.key().getClass().getName() + ", expected Struct.");
    }

    this.key = (Struct) sourceRecord.key();
    this.value = (Struct) sourceRecord.value();
    this.schemaConverter =
        new StructSchemaConverter(
            sourceRecord.valueSchema(), sourceRecord.keySchema(), effectiveConfig);
  }

  @Override
  public Struct key() {
    return key;
  }

  @Override
  public boolean hasKeyData() {
    return this.key() != null;
  }

  @Override
  public Struct value() {
    return value;
  }

  @Override
  public Long cdcSourceTsValue() {
    String cdcSourceTsField = config.iceberg().cdcSourceTsField().orElse("");
    if (cdcSourceTsField.isBlank()) {
      throw new DebeziumException("Property debezium.sink.iceberg.upsert-dedup-column is not set");
    }

    if (value == null) {
      throw new DebeziumException("Value is null, cannot extract '" + cdcSourceTsField + "'");
    }

    Object tsValue = value.get(cdcSourceTsField);
    if (tsValue == null) {
      LOGGER.warn(
          "Field '{}' is null in the event for destination '{}'", cdcSourceTsField, destination);
      return null;
    }

    if (!(tsValue instanceof Long)) {
      throw new DebeziumException(
          "Expected Long type for field '"
              + cdcSourceTsField
              + "', but found "
              + tsValue.getClass().getName());
    }

    return (Long) tsValue;
  }

  @Override
  public Operation cdcOpValue() {
    if (value == null) {
      throw new DebeziumException(
          "Value is null, cannot extract '" + config.iceberg().cdcOpField() + "'");
    }

    Object opValue = value.get(config.iceberg().cdcOpField());
    if (opValue == null) {
      LOGGER.warn(
          "Field '{}' is null in the event for destination '{}', defaulting to INSERT operation.",
          config.iceberg().cdcOpField(),
          destination);
      return Operation.INSERT;
    }

    if (!(opValue instanceof String)) {
      throw new DebeziumException(
          "Expected String type for field '"
              + config.iceberg().cdcOpField()
              + "', but found "
              + opValue.getClass().getName());
    }

    final String opFieldValue = (String) opValue;

    return switch (opFieldValue.toLowerCase()) {
      case "u" -> Operation.UPDATE;
      case "d" -> Operation.DELETE;
      case "r" -> Operation.READ;
      case "c", "i" -> Operation.INSERT;
      default ->
        throw new DebeziumException(
            "Unexpected `"
                + config.iceberg().cdcOpField()
                + "="
                + opFieldValue
                + "` operation value received, expecting one of ['u','d','r','c', 'i']");
    };
  }

  @Override
  public boolean isNewKey() {
    return newKey;
  }

  @Override
  public void setNewKey(boolean newKey) {
    this.newKey = newKey;
  }

  @Override
  public StructSchemaConverter schemaConverter() {
    return this.schemaConverter;
  }

  @Override
  public boolean isSchemaChangeEvent() {
    return schemaConverter.isSchemaChangeEvent();
  }

  @Override
  public Schema icebergSchema(boolean withIdentifierFields) {
    return this.schemaConverter.icebergSchema(withIdentifierFields);
  }

  @Override
  public SortOrder sortOrder(Schema schema) {
    return schemaConverter().sortOrder(schema);
  }

  @Override
  public String destination() {
    return destination;
  }

  @Override
  public RecordWrapper convertAsAppend(Schema schema) {
    if (value == null) {
      throw new DebeziumException(
          "Cannot convert null value Struct to Iceberg Record for APPEND operation.");
    }
    GenericRecord row = convertToIcebergRecord(schema.asStruct(), value);
    return new RecordWrapper(row, Operation.INSERT, true);
  }

  @Override
  public RecordWrapper convert(Schema schema) {
    GenericRecord row = convertToIcebergRecord(schema.asStruct(), value);
    Operation op = cdcOpValue();
    return new RecordWrapper(row, op, newKey);
  }

  protected GenericRecord convertToIcebergRecord(
      Types.StructType icebergStructSchema, Struct connectStruct) {
    return structConverter.convertToIcebergRecord(icebergStructSchema, connectStruct);
  }
}
