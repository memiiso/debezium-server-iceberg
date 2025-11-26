/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts Debezium Struct events to Iceberg GenericRecord.
 * Uses StructSchemaConverter to derive Iceberg schema.
 * Conversion logic is adapted from Iceberg's RecordConverter.
 *
 * @author Ismail Simsek
 */
public class StructEventConverter extends AbstractEventConverter implements EventConverter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(StructEventConverter.class);
  protected final String destination;
  private final Struct value;
  private final Struct key;
  private final StructSchemaConverter schemaConverter;
  private static final Serde<Struct> eventSerde = DebeziumSerdes.payloadJson(Struct.class);
  private static final Serializer<Struct> structSerializer = eventSerde.serializer();

  public StructEventConverter(EmbeddedEngineChangeEvent e, GlobalConfig config) {
    super(config);
    this.destination = e.destination();
    if (e.sourceRecord() == null) {
      throw new DebeziumException("Unexpected event type: " + e.getClass().getName() + " event.sourceRecord() is null!, expected SourceRecord value.");
    }
    if (e.sourceRecord().value() != null && !(e.sourceRecord().value() instanceof Struct)) {
      throw new DebeziumException("Unexpected value type: " + e.sourceRecord().value().getClass().getName() + ", expected Struct.");
    }
    if (e.sourceRecord().key() != null && !(e.sourceRecord().key() instanceof Struct)) {
      throw new DebeziumException("Unexpected key type: " + e.sourceRecord().key().getClass().getName() + ", expected Struct.");
    }

    this.key = (Struct) e.sourceRecord().key();
    this.value = (Struct) e.sourceRecord().value();
    this.schemaConverter = new StructSchemaConverter(e.sourceRecord().valueSchema(), e.sourceRecord().keySchema(), config);
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
    if (value == null) {
      throw new DebeziumException("Value is null, cannot extract '" + config.iceberg().cdcSourceTsField() + "'");
    }

    Object tsValue = value.get(config.iceberg().cdcSourceTsField());
    if (tsValue == null) {
      LOGGER.warn("Field '{}' is null in the event for destination '{}'", config.iceberg().cdcSourceTsField(), destination);
      return null; // Or throw an exception if null is not acceptable
    }

    if (!(tsValue instanceof Long)) {
      throw new DebeziumException("Expected Long type for field '" + config.iceberg().cdcSourceTsField() + "', but found " + tsValue.getClass().getName());
    }

    return (Long) tsValue;
  }

  @Override
  public Operation cdcOpValue() {
    if (value == null) {
      throw new DebeziumException("Value is null, cannot extract '" + config.iceberg().cdcOpField() + "'");
    }

    Object opValue = value.get(config.iceberg().cdcOpField());
    if (opValue == null) {
      // Defaulting to 'c' (create/insert) if op field is null, adjust as needed
      LOGGER.warn("Field '{}' is null in the event for destination '{}', defaulting to INSERT operation.", config.iceberg().cdcOpField(), destination);
      return Operation.INSERT;
    }

    if (!(opValue instanceof String)) {
      throw new DebeziumException("Expected String type for field '" + config.iceberg().cdcOpField() + "', but found " + opValue.getClass().getName());
    }

    final String opFieldValue = (String) opValue;

    return switch (opFieldValue.toLowerCase()) {
      case "u" -> Operation.UPDATE;
      case "d" -> Operation.DELETE;
      case "r" -> Operation.READ; // Read event (snapshot)
      case "c", "i" -> Operation.INSERT; // Create event
      default ->
          throw new DebeziumException("Unexpected `" + config.iceberg().cdcOpField() + "=" + opFieldValue + "` operation value received, expecting one of ['u','d','r','c', 'i']");
    };
  }

  @Override
  public StructSchemaConverter schemaConverter() {
    return this.schemaConverter;
  }

  /**
   * Checks if the current message represents a schema change event.
   * For Struct format, schema change events might not be directly represented
   * in the value Struct after transformations. This method might need
   * adjustments based on specific connector/transform behavior.
   * Currently returns false as standard data operations are expected.
   *
   * @return False, assuming data events.
   */
  @Override
  public boolean isSchemaChangeEvent() {
    return (schemaConverter().valueSchema().field("ddl") != null &&
        schemaConverter().valueSchema().field("databaseName") != null &&
        schemaConverter().valueSchema().field("tableChanges") != null);
  }

  /**
   * Returns the derived Iceberg schema.
   *
   * @return The Iceberg schema.
   */
  @Override
  public Schema icebergSchema() {
    return this.schemaConverter.icebergSchema();
  }

  @Override
  public SortOrder sortOrder(Schema schema) {
    if (config.debezium().isHeartbeatTopic(destination())) {
      return SortOrder.unsorted();
    }
    if (config.iceberg().nestedAsVariant()) {
      return SortOrder.unsorted();
    }
    return schemaConverter().sortOrder(schema);
  }

  @Override
  public String destination() {
    return destination;
  }

  @Override
  public RecordWrapper convertAsAppend(Schema schema) {
    if (value == null) {
      throw new DebeziumException("Cannot convert null value Struct to Iceberg Record for APPEND operation.");
    }
    GenericRecord row = convertToIcebergRecord(schema.asStruct(), value);
    return new RecordWrapper(row, Operation.INSERT);
  }

  @Override
  public RecordWrapper convert(Schema schema) {
    GenericRecord row = convertToIcebergRecord(schema.asStruct(), value);
    Operation op = cdcOpValue();
    return new RecordWrapper(row, op);
  }

  /**
   * Converts a Kafka Connect Struct to an Iceberg GenericRecord based on the target Iceberg schema.
   *
   * @param icebergStructSchema The target Iceberg struct schema.
   * @param connectStruct       The source Kafka Connect Struct.
   * @return An Iceberg GenericRecord.
   */
  protected GenericRecord convertToIcebergRecord(Types.StructType icebergStructSchema, Struct connectStruct) {
    GenericRecord record = GenericRecord.create(icebergStructSchema);

    if (connectStruct == null) {
      return record;
    }

    for (Types.NestedField icebergField : icebergStructSchema.fields()) {

      Field field = connectStruct.schema().field(icebergField.name());
      if (field == null) {
        record.setField(icebergField.name(), null);
        continue;
      }
      Object connectValue = connectStruct.get(field);

      if (connectValue == null) {
        record.setField(icebergField.name(), null);
        continue;
      }

      String logicalTypeName = field.schema().name();
      Object icebergValue = convertValue(connectValue, icebergField.type(), icebergField.name(), logicalTypeName);
      record.setField(icebergField.name(), icebergValue);
    }

    return record;
  }

  /**
   * Converts a Kafka Connect value to the corresponding Iceberg type.
   * Logic adapted from Iceberg's RecordConverter.
   *
   * @param connectValue The value from the Kafka Connect Struct field.
   * @param icebergType  The target Iceberg type.
   * @return The converted value compatible with Iceberg.
   */
  private Object convertValue(Object connectValue, Type icebergType, String icebergFieldName, String logicalTypeName) {
    if (connectValue == null) {
      return null;
    }

    LOGGER.trace("Converting value {} to Iceberg type {}", connectValue, icebergType);

    switch (icebergType.typeId()) {
      case STRUCT:
        Preconditions.checkArgument(connectValue instanceof Struct,
            "Cannot convert to Struct: value is not a Struct: %s", connectValue.getClass().getName());
        return convertToIcebergRecord(icebergType.asStructType(), (Struct) connectValue);

      case VARIANT:
        Preconditions.checkArgument(connectValue instanceof Struct,
            "Cannot convert to Variant: value is not a Struct: %s", connectValue.getClass().getName());
        StructVariantObject val = new StructVariantObject((Struct) connectValue);
        return Variant.of(val.metadata(), val);

      case LIST:
        Preconditions.checkArgument(connectValue instanceof List,
            "Cannot convert to List: value is not a List: %s", connectValue.getClass().getName());
        return convertListValue((List<?>) connectValue, icebergType.asListType(), logicalTypeName);

      case MAP:
        Preconditions.checkArgument(connectValue instanceof Map,
            "Cannot convert to Map: value is not a Map: %s", connectValue.getClass().getName());
        return convertMapValue((Map<?, ?>) connectValue, icebergType.asMapType(), logicalTypeName);

      case INTEGER: // int32
        return convertInt(connectValue);
      case LONG: // int64
        return convertLong(connectValue);
      case FLOAT: // float32
        return convertFloat(connectValue);
      case DOUBLE: // float64
        return convertDouble(connectValue, logicalTypeName);
      case DECIMAL:
        return convertDecimal(connectValue, (Types.DecimalType) icebergType, logicalTypeName);
      case BOOLEAN:
        return convertBoolean(connectValue);
      case STRING:
        return convertString(connectValue);
      case UUID:
        return convertUUID(connectValue);
      case BINARY: // raw bytes
      case FIXED: // fixed length bytes
        return convertBinary(connectValue, logicalTypeName);
      case DATE:
        return convertDateValue(connectValue, logicalTypeName);
      case TIME:
        return convertTimeValue(connectValue, logicalTypeName);
      case TIMESTAMP:
        return convertTimestampValue(connectValue, (Types.TimestampType) icebergType, icebergFieldName, logicalTypeName);
      default:
        throw new UnsupportedOperationException("Unsupported Iceberg type: " + icebergType);
    }
  }


  protected List<Object> convertListValue(List<?> list, Types.ListType listType, String logicalTypeName) {
    Type elementType = listType.elementType();
    return list.stream()
        .map(element -> convertValue(element, elementType, null, null))
        .collect(Collectors.toList());
  }

  protected Map<Object, Object> convertMapValue(Map<?, ?> map, Types.MapType mapType, String logicalTypeName) {
    Type keyType = mapType.keyType();
    Type valueType = mapType.valueType();
    Map<Object, Object> result = new HashMap<>();
    map.forEach((k, v) -> result.put(convertValue(k, keyType, null, null), convertValue(v, valueType, null, null)));
    return result;
  }

}