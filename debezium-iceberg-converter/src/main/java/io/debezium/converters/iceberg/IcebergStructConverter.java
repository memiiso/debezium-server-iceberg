/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import io.debezium.DebeziumException;
import io.debezium.converters.iceberg.geospatial.GeospatialValues;
import io.debezium.converters.iceberg.internal.LruCache;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layer 1 in-memory converter translating Kafka Connect {@link SourceRecord}
 * and {@link Struct}
 * instances directly into Apache Iceberg {@link org.apache.iceberg.Schema},
 * {@link SortOrder}, and
 * {@link GenericRecord} values with zero byte-array serialization overhead.
 */
public class IcebergStructConverter {

  public static final int DEFAULT_SCHEMA_CACHE_SIZE = 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergStructConverter.class);

  private final IcebergConverterConfig config;
  private final IcebergValueConverter valueConverter;
  private final LruCache<SchemaCacheKey, DerivedSchema> derivedSchemaCache;

  public IcebergStructConverter(IcebergConverterConfig config) {
    this(config, DEFAULT_SCHEMA_CACHE_SIZE);
  }

  public IcebergStructConverter(IcebergConverterConfig config, int maxSchemaCacheSize) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    if (maxSchemaCacheSize < 0) {
      throw new IllegalArgumentException(
          "maxSchemaCacheSize must be >= 0, got: " + maxSchemaCacheSize);
    }
    this.valueConverter = new IcebergValueConverter(config);
    this.derivedSchemaCache = maxSchemaCacheSize == 0 ? null : new LruCache<>(maxSchemaCacheSize);
  }

  /**
   * Converts a full Kafka Connect {@link SourceRecord} (using both
   * {@code keySchema} and {@code
   * valueSchema}) into a {@link ConvertedIcebergRecord}.
   *
   * <p>
   * Tombstone records ({@code sourceRecord.value() == null}) return a {@link
   * ConvertedIcebergRecord} with {@code record == null}, preserving the derived
   * Iceberg schema when
   * {@code valueSchema} is present.
   */
  public ConvertedIcebergRecord convert(SourceRecord sourceRecord) {
    Objects.requireNonNull(sourceRecord, "sourceRecord must not be null");

    Object rawValue = sourceRecord.value();
    if (rawValue != null && !(rawValue instanceof Struct)) {
      throw new DebeziumException(
          "Unexpected value type: " + rawValue.getClass().getName() + ", expected Struct.");
    }
    Struct valueStruct = (Struct) rawValue;

    Schema keySchema = sourceRecord.keySchema();
    if (keySchema == null && sourceRecord.key() instanceof Struct keyStruct) {
      keySchema = keyStruct.schema();
    }

    return convert(sourceRecord.topic(), sourceRecord.valueSchema(), valueStruct, keySchema);
  }

  /**
   * Converts a Kafka Connect value {@link Struct} with explicit
   * {@code valueSchema} and optional
   * {@code keySchema} into a {@link ConvertedIcebergRecord}, reusing the bounded
   * schema cache.
   */
  public ConvertedIcebergRecord convert(
      String topic, Schema valueSchema, Struct valueStruct, Schema keySchema) {
    Schema effectiveValueSchema = valueSchema == null && valueStruct != null ? valueStruct.schema() : valueSchema;

    if (effectiveValueSchema == null) {
      return new ConvertedIcebergRecord(topic, null, SortOrder.unsorted(), null, false);
    }
    if (effectiveValueSchema.type() != Schema.Type.STRUCT) {
      throw new DebeziumException(
          "Failed to get schema from Debezium event, value schema is null or not a Struct: "
              + effectiveValueSchema);
    }

    SchemaCacheKey key = new SchemaCacheKey(effectiveValueSchema, keySchema);
    DerivedSchema derived = derivedSchemaCache == null
        ? deriveSchema(key)
        : derivedSchemaCache.getOrCompute(key, this::deriveSchema);
    GenericRecord record = valueStruct == null
        ? null
        : convertToIcebergRecord(derived.schema().asStruct(), valueStruct);
    return new ConvertedIcebergRecord(
        topic, derived.schema(), derived.sortOrder(), record, derived.schemaChangeEvent());
  }

  /**
   * Converts a Kafka Connect {@link Struct} to an Iceberg {@link GenericRecord}
   * conforming to the
   * caller-supplied target Iceberg struct schema.
   */
  public GenericRecord convertToIcebergRecord(
      Types.StructType icebergStructSchema, Struct connectStruct) {
    Objects.requireNonNull(icebergStructSchema, "icebergStructSchema must not be null");
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
      Object connectValue = config.replaceNullWithDefault()
          ? connectStruct.get(field)
          : connectStruct.getWithoutDefault(field.name());

      if (connectValue == null) {
        record.setField(icebergField.name(), null);
        continue;
      }

      Object icebergValue = convertValue(connectValue, icebergField.type(), icebergField.name(), field.schema());
      record.setField(icebergField.name(), icebergValue);
    }

    return record;
  }

  int currentSchemaCacheSize() {
    return derivedSchemaCache == null ? 0 : derivedSchemaCache.size();
  }

  private DerivedSchema deriveSchema(SchemaCacheKey key) {
    StructSchemaConverter schemaConverter =
        new StructSchemaConverter(key.valueSchema(), key.keySchema(), config);
    boolean schemaChange = schemaConverter.isSchemaChangeEvent();
    boolean hasStructKey = key.keySchema() != null && key.keySchema().type() == Schema.Type.STRUCT;
    if (config.createIdentifierFields()
        && config.nestedAsVariant()
        && !schemaChange
        && hasStructKey) {
      LOGGER.warn(
          "Identifier fields are not supported when data is converted to variant fields,"
              + " creating schema without identifier fields!");
    }
    boolean effectiveIdentifierFields =
        config.createIdentifierFields()
            && !config.nestedAsVariant()
            && !schemaChange
            && hasStructKey;
    org.apache.iceberg.Schema icebergSchema =
        schemaConverter.icebergSchema(effectiveIdentifierFields);
    SortOrder sortOrder =
        effectiveIdentifierFields ? schemaConverter.sortOrder(icebergSchema) : SortOrder.unsorted();
    return new DerivedSchema(icebergSchema, sortOrder, schemaChange);
  }

  private Object convertValue(
      Object connectValue, Type icebergType, String icebergFieldName, Schema connectSchema) {
    if (connectValue == null) {
      return null;
    }

    String logicalTypeName = connectSchema == null ? null : connectSchema.name();
    LOGGER.trace("Converting value {} to Iceberg type {}", connectValue, icebergType);

    switch (icebergType.typeId()) {
      case STRUCT:
        if (!(connectValue instanceof Struct struct)) {
          throw new IllegalArgumentException(
              "Cannot convert to Struct: value is not a Struct: "
                  + connectValue.getClass().getName());
        }
        return convertToIcebergRecord(icebergType.asStructType(), struct);

      case VARIANT:
        if (!(connectValue instanceof Struct struct)) {
          throw new IllegalArgumentException(
              "Cannot convert to Variant: value is not a Struct: "
                  + connectValue.getClass().getName());
        }
        StructVariantObject val = new StructVariantObject(struct);
        return Variant.of(val.metadata(), val);

      case LIST:
        if (!(connectValue instanceof List<?> list)) {
          throw new IllegalArgumentException(
              "Cannot convert to List: value is not a List: " + connectValue.getClass().getName());
        }
        Schema elementSchema = connectSchema != null && connectSchema.type() == Schema.Type.ARRAY
            ? connectSchema.valueSchema()
            : null;
        return convertListValue(list, icebergType.asListType(), icebergFieldName, elementSchema);

      case MAP:
        if (!(connectValue instanceof Map<?, ?> map)) {
          throw new IllegalArgumentException(
              "Cannot convert to Map: value is not a Map: " + connectValue.getClass().getName());
        }
        Schema mapKeySchema = connectSchema != null && connectSchema.type() == Schema.Type.MAP
            ? connectSchema.keySchema()
            : null;
        Schema mapValueSchema = connectSchema != null && connectSchema.type() == Schema.Type.MAP
            ? connectSchema.valueSchema()
            : null;
        return convertMapValue(
            map, icebergType.asMapType(), icebergFieldName, mapKeySchema, mapValueSchema);

      case INTEGER:
        return valueConverter.convertInt(connectValue);
      case LONG:
        return valueConverter.convertLong(connectValue);
      case FLOAT:
        return valueConverter.convertFloat(connectValue);
      case DOUBLE:
        return valueConverter.convertDouble(connectValue);
      case DECIMAL:
        return valueConverter.convertDecimal(connectValue, (Types.DecimalType) icebergType);
      case BOOLEAN:
        return valueConverter.convertBoolean(connectValue);
      case STRING:
        return valueConverter.convertString(connectValue);
      case UUID:
        return valueConverter.convertUUID(connectValue);
      case BINARY:
      case FIXED:
        return valueConverter.convertBinary(connectValue);
      case GEOMETRY:
      case GEOGRAPHY:
        return GeospatialValues.fromStruct(connectValue, icebergFieldName, icebergType, config);
      case DATE:
        return valueConverter.convertDateValue(connectValue, logicalTypeName);
      case TIME:
        return valueConverter.convertTimeValue(connectValue, logicalTypeName);
      case TIMESTAMP:
        return valueConverter.convertTimestampValue(
            connectValue, (Types.TimestampType) icebergType, icebergFieldName, logicalTypeName);
      default:
        throw new UnsupportedOperationException("Unsupported Iceberg type: " + icebergType);
    }
  }

  private List<Object> convertListValue(
      List<?> list, Types.ListType listType, String fieldName, Schema elementSchema) {
    Type elementType = listType.elementType();
    String elementName = fieldName == null ? null : fieldName + "[]";
    return list.stream()
        .map(element -> convertValue(element, elementType, elementName, elementSchema))
        .collect(Collectors.toList());
  }

  private Map<Object, Object> convertMapValue(
      Map<?, ?> map,
      Types.MapType mapType,
      String fieldName,
      Schema keySchema,
      Schema valueSchema) {
    Type keyType = mapType.keyType();
    Type valueType = mapType.valueType();
    String keyFieldName = fieldName == null ? null : fieldName + ".key";
    String valueFieldName = fieldName == null ? null : fieldName + ".value";
    Map<Object, Object> result = new HashMap<>();
    map.forEach(
        (k, v) -> result.put(
            convertValue(k, keyType, keyFieldName, keySchema),
            convertValue(v, valueType, valueFieldName, valueSchema)));
    return result;
  }

  private record SchemaCacheKey(Schema valueSchema, Schema keySchema) {
  }

  private record DerivedSchema(
      org.apache.iceberg.Schema schema, SortOrder sortOrder, boolean schemaChangeEvent) {
  }
}
