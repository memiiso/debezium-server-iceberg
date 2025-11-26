package io.debezium.server.iceberg.converter;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts Kafka Connect Schemas (org.apache.kafka.connect.data.Schema)
 * used in Debezium Struct events into an Iceberg Schema.
 *
 * @author Ismail Simsek
 */
public class StructSchemaConverter implements SchemaConverter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(StructSchemaConverter.class);

  private final Schema valueSchema;
  private final Schema keySchema;
  private final GlobalConfig config;

  public StructSchemaConverter(Schema valueSchema, Schema keySchema, GlobalConfig config) {
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
    this.config = config;
  }

  private static List<Field> getSchemaFields(Schema schema) {
    if (schema != null && schema.type() == Schema.Type.STRUCT) {
      return schema.fields();
    }
    return Collections.emptyList();
  }

  public Schema valueSchema() {
    return valueSchema;
  }


  private static List<Field> getSchemaFields(Field field) {
    if (field == null) {
      return Collections.emptyList();
    }
    return getSchemaFields(field.schema());
  }

  public Schema keySchema() {
    return keySchema;
  }

  /**
   * Converts a Kafka Connect Field to its Iceberg representation and adds it to the schema data.
   * Handles nested structures (Struct, Map, Array) recursively.
   * Mirrors JsonSchemaConverter.debeziumFieldToIcebergField(JsonNode, String, IcebergSchemaInfo, JsonNode).
   *
   * @param connectField The Kafka Connect Field to convert.
   * @param schemaData   The helper object tracking Iceberg schema construction state.
   * @param connectKeyField  Equivalent field in the Key schema
   */
  private void debeziumFieldToIcebergField(Field connectField, IcebergSchemaInfo schemaData, Field connectKeyField) {
    final Schema connectSchema = connectField.schema();
    final String fieldName = connectField.name();
    final String fieldTypeName = connectSchema.name();
    final Schema.Type fieldType = connectSchema.type();

    if (fieldType == null) {
      throw new DebeziumException("Unexpected schema field, field type is null or empty, fieldSchema:" + connectSchema + " fieldName:" + fieldName);
    }

    boolean isPkField = connectKeyField != null;
    boolean isOptional = config.iceberg().preserveRequiredProperty() ? connectField.schema().isOptional() : !isPkField;
    LOGGER.trace("Converting field: '{}', Type: '{}', LogicalType: '{}', PK: {}", fieldName, fieldType, fieldTypeName, isPkField);

    switch (fieldType) {
      case STRUCT:
        if (config.iceberg().nestedAsVariant()) {
          // here we are keeping nested fields in variant
          int variantFieldId = schemaData.nextFieldId().getAndIncrement();
          final Types.NestedField variantField = Types.NestedField.optional(variantFieldId, fieldName, Types.VariantType.get(), connectSchema.doc());
          schemaData.fields().add(variantField);
          break;
        }

        int rootStructId = schemaData.nextFieldId().getAndIncrement();
        final IcebergSchemaInfo subSchemaData = schemaData.copyPreservingMetadata();
        for (Field subField : connectSchema.fields()) {
          debeziumFieldToIcebergField(subField, subSchemaData, equivalentKeyField(connectKeyField, subField.name()));
        }
        // Create it as struct, nested type
        final Types.StructType structType = Types.StructType.of(subSchemaData.fields());
        final Types.NestedField structField = Types.NestedField.of(rootStructId, isOptional, fieldName, structType, connectSchema.doc());
        schemaData.fields().add(structField);
        // Add struct field ID to PK list if the struct itself is a PK (though generally disallowed)
        if (isPkField) {
          // Note: Iceberg typically disallows complex types as identifier fields.
          // This check might be better placed before adding the field.
          LOGGER.warn("Struct field '{}' is marked as PK. Complex types as identifiers might cause issues.", fieldName);
          schemaData.identifierFieldIds().add(rootStructId);
        }
        break; // Added break

      case MAP:
        if (isPkField) {
          throw new DebeziumException("Map field '" + fieldName + "' cannot be part of the primary key.");
        }
        int rootMapId = schemaData.nextFieldId().getAndIncrement();
        int keyFieldId = schemaData.nextFieldId().getAndIncrement();
        int valFieldId = schemaData.nextFieldId().getAndIncrement();
        // Convert key schema
        final IcebergSchemaInfo keySchemaData = schemaData.copyPreservingMetadata();
        // Create a temporary Field object for the key schema to pass to the recursive call
        // Note: Map keys don't have a "field name" in the traditional sense, using a placeholder.
        // PK status is false for map keys/values.
        Field keyTempField = new Field(fieldName + "_key", -1, connectSchema.keySchema());
        debeziumFieldToIcebergField(keyTempField, keySchemaData, null);
        Type keyType = keySchemaData.fields().get(0).type(); // Assuming the recursive call adds one field
        if (keyType.isNestedType()) {
          throw new DebeziumException("Invalid key type for map: " + keyType.typeId() + ". Map keys cannot be List, Map, or Struct.");
        }

        // Convert value schema
        final IcebergSchemaInfo valSchemaData = schemaData.copyPreservingMetadata();
        Field valueTempField = new Field(fieldName + "_value", -1, connectSchema.valueSchema());
        debeziumFieldToIcebergField(valueTempField, valSchemaData, null);
        Type valueType = valSchemaData.fields().get(0).type(); // Assuming the recursive call adds one field

        final Types.MapType mapField = Types.MapType.ofOptional(keyFieldId, valFieldId, keyType, valueType);
        schemaData.fields().add(Types.NestedField.of(rootMapId, isOptional, fieldName, mapField, connectSchema.doc()));
        break; // Added break

      case ARRAY:
        if (isPkField) {
          throw new DebeziumException("Array field '" + fieldName + "' cannot be part of the primary key.");
        }
        int rootArrayId = schemaData.nextFieldId().getAndIncrement();
        int elementFieldId = schemaData.nextFieldId().getAndIncrement(); // ID for the list element

        // Convert element schema
        final IcebergSchemaInfo arraySchemaData = schemaData.copyPreservingMetadata();
        // Create a temporary Field object for the element schema
        Field elementTempField = new Field(fieldName + "_element", -1, connectSchema.valueSchema());
        debeziumFieldToIcebergField(elementTempField, arraySchemaData, null);
        Type elementType = arraySchemaData.fields().get(0).type(); // Assuming the recursive call adds one field

        final Types.ListType listField = Types.ListType.ofOptional(elementFieldId, elementType);
        schemaData.fields().add(Types.NestedField.of(rootArrayId, isOptional, fieldName, listField, connectSchema.doc()));
        break; // Added break

      default:
        // It's a primitive field
        int primitiveFieldId = schemaData.nextFieldId().getAndIncrement();
        final Type.PrimitiveType primitiveType = icebergPrimitiveField(fieldName, connectSchema);
        final Types.NestedField field = Types.NestedField.of(primitiveFieldId, isOptional, fieldName, primitiveType, connectSchema.doc());
        schemaData.fields().add(field);
        if (isPkField) schemaData.identifierFieldIds().add(field.fieldId());
        break; // Added break
    }
  }

  private Schema keyFieldSchema() {
    if (!config.debezium().isEventFlatteningEnabled() && keySchema != null) {
      // For unflattened events, the key fields are expected inside the 'before' or 'after' struct.
      // However, determining PKs reliably this way is problematic. JsonSchemaConverter throws an exception later.
      // We'll get the key names but the check below will likely fail if PKs are found.
      LOGGER.warn("Processing unflattened event schema. Identifier field determination might be unreliable.");
      Field nestedAfterField = equivalentKeyField(keySchema, "after");
      return nestedAfterField == null ? null : nestedAfterField.schema();
    }
    return keySchema;
  }

  /**
   * Converts the fields of a Connect schema to Iceberg fields.
   */
  private IcebergSchemaInfo icebergSchemaFields(Schema valueSchema, Schema keySchema, IcebergSchemaInfo schemaData) {
    LOGGER.debug("Converting Connect schema fields to Iceberg fields for schema: {}", valueSchema);

    List<String> excludedColumns = this.config.iceberg()
            .excludedColumns()
            .orElse(Collections.emptyList());

    for (Field field : getSchemaFields(valueSchema)) {
      String fieldName = field.name();
      if(excludedColumns.contains(fieldName)) {
        continue;
      }
      // convert each field to iceberg equivalent field
      debeziumFieldToIcebergField(field, schemaData, equivalentKeyField(keySchema, fieldName));
    }
    return schemaData;
  }

  public Field equivalentKeyField(Schema keySchema, String fieldName) {
    // Find the matching Field object from the keyFields list, if it exists
    List<Field> keyFields = getSchemaFields(keySchema);
    return keyFields.stream()
        .filter(keyField -> keyField.name().equals(fieldName))
        .findFirst()
        .orElse(null);
  }

  public Field equivalentKeyField(Field ParentKeyField, String fieldName) {
    if (ParentKeyField == null) {
      return null;
    }

    return equivalentKeyField(ParentKeyField.schema(), fieldName);
  }

  /**
   * Converts the Kafka Connect value and key schemas to an Iceberg Schema.
   * Mirrors JsonSchemaConverter.icebergSchema().
   *
   * @return The corresponding Iceberg Schema.
   */
  @Override
  public org.apache.iceberg.Schema icebergSchema() {
    if (this.valueSchema == null || this.valueSchema.type() != Schema.Type.STRUCT) {
      throw new DebeziumException("Failed to get schema from Debezium event, value schema is null or not a Struct: " + valueSchema);
    }

    IcebergSchemaInfo schemaData = new IcebergSchemaInfo();
    final Schema keyFieldSchema = this.keyFieldSchema();

    icebergSchemaFields(valueSchema, keyFieldSchema, schemaData);

    if (!config.debezium().isEventFlatteningEnabled() && !schemaData.identifierFieldIds().isEmpty()) {
      // While Iceberg supports nested key fields, they cannot be set with nested events(unwrapped events, Without event flattening)
      // due to inconsistency in the after and before fields.
      // For insert events, only the `before` field is NULL, while for delete events after field is NULL.
      // This inconsistency prevents using either field as a reliable key.
      throw new DebeziumException("Debezium events are unnested, Identifier fields are not supported for unnested events! " +
          "Pleas enable event flattening SMT see: https://debezium.io/documentation/reference/stable/transformations/event-flattening.html " +
          " Or disable identifier field creation `debezium.sink.iceberg.create-identifier-fields=false`");
    }

    if (schemaData.fields().isEmpty()) {
      throw new RuntimeException("Failed to get schema from debezium event, event schema has no fields!");
    }

    if (!config.iceberg().createIdentifierFields()) {
      LOGGER.warn("Creating identifier fields is disabled, creating schema without identifier fields!");
      return new org.apache.iceberg.Schema(schemaData.fields());
    }
    if (config.iceberg().nestedAsVariant()) {
      LOGGER.warn("Identifier fields are not supported when data consumed to variant fields, creating schema without identifier fields!");
      return new org.apache.iceberg.Schema(schemaData.fields());
    }

    // @TODO validate key fields are correctly set!?
    return new org.apache.iceberg.Schema(schemaData.fields(), schemaData.identifierFieldIds());
  }

  @Override
  public SortOrder sortOrder(org.apache.iceberg.Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    List<String> excludedColumns = config.iceberg().excludedColumns().orElse(Collections.emptyList());
    for (Field field : getSchemaFields(keyFieldSchema())) {
      if (excludedColumns.contains(field.name())) continue;
      sob = sob.asc(field.name());
    }
    return sob.build();
  }

  /**
   * Maps Kafka Connect primitive and logical types to Iceberg primitive types.
   * Mirrors JsonSchemaConverter.icebergPrimitiveField(String, String, String, JsonNode).
   *
   * @param fieldName     The name of the field (used for specific logical type checks like ts_ms).
   * @param connectSchema The Kafka Connect schema for the primitive or logical type.
   * @return The corresponding Iceberg PrimitiveType.
   */
  private Type.PrimitiveType icebergPrimitiveField(String fieldName, Schema connectSchema) {
    String logicalTypeName = connectSchema.name();
    Schema.Type fieldType = connectSchema.type();

    // Debezium Temporal types: https://debezium.io/documentation//reference/connectors/postgresql.html#postgresql-temporal-types
    switch (fieldType) {
      case INT8:
      case INT16:
      case INT32: // int 4 bytes
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(logicalTypeName) ||
            io.debezium.time.Date.SCHEMA_NAME.equals(logicalTypeName)) {
          return Types.DateType.get();
        }
        // NOTE: Time type is disabled for the moment, it's not supported by spark
        // if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(logicalTypeName) || ...) { return Types.TimeType.get(); }
        return Types.IntegerType.get();

      case INT64: // long 8 bytes
        // Handle Debezium's special __ts_ms field
        if (fieldName != null && DebeziumConfig.TS_MS_FIELDS.contains(fieldName)) {
          return Types.TimestampType.withZone();
        }
        // Handle standard Timestamp logical types
        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(logicalTypeName) ||
            io.debezium.time.Timestamp.SCHEMA_NAME.equals(logicalTypeName) ||
            io.debezium.time.MicroTimestamp.SCHEMA_NAME.equals(logicalTypeName) ||
            io.debezium.time.NanoTimestamp.SCHEMA_NAME.equals(logicalTypeName)) {
          // Debezium's timestamps are typically UTC, hence representable as withoutZone.
          return Types.TimestampType.withoutZone();
        }
        // NOTE: Time type is disabled for the moment, it's not supported by spark
        // if (io.debezium.time.MicroTime.SCHEMA_NAME.equals(logicalTypeName) || ...) { return Types.TimeType.get(); }
        return Types.LongType.get();

      case FLOAT32: // float is represented in 32 bits,
        return Types.FloatType.get();
      case FLOAT64: // double is represented in 64 bits
        return Types.DoubleType.get();
      case BOOLEAN:
        return Types.BooleanType.get();
      case STRING:
        if (io.debezium.data.Uuid.LOGICAL_NAME.equals(logicalTypeName)) {
          return Types.UUIDType.get();
        }
        if (io.debezium.time.ZonedTimestamp.SCHEMA_NAME.equals(logicalTypeName)) {
          // ZonedTimestamp is string ISO format with offset, maps to Iceberg's timestamptz
          return Types.TimestampType.withZone();
        }
        if (io.debezium.time.IsoDate.SCHEMA_NAME.equals(logicalTypeName)) {
          return Types.DateType.get();
        }
        if (io.debezium.time.IsoTimestamp.SCHEMA_NAME.equals(logicalTypeName)) {
          return Types.TimestampType.withoutZone();
        }
        // NOTE: Time type is disabled for the moment, it's not supported by spark
        // if (io.debezium.time.ZonedTime.SCHEMA_NAME.equals(logicalTypeName) || ...) { return Types.TimeType.get(); }
        return Types.StringType.get();

      case BYTES:
        // With `decimal.handling.mode` set to `precise` debezium relational source connector would encode decimals
        // as byte arrays. We want to write them out as Iceberg decimals.
        if (org.apache.kafka.connect.data.Decimal.LOGICAL_NAME.equals(logicalTypeName)) {
          Map<String, String> params = connectSchema.parameters();
          if (params != null && params.containsKey(org.apache.kafka.connect.data.Decimal.SCALE_FIELD)) {
            int scale = Integer.parseInt(params.get(org.apache.kafka.connect.data.Decimal.SCALE_FIELD));
            // Attempt to get precision, default if missing
            int precision = Integer.parseInt(params.getOrDefault("connect.decimal.precision", "38"));
            // Basic validation
            if (precision <= 0) {
              LOGGER.warn("Invalid precision {} found for Decimal field '{}', using default 38.", precision, fieldName);
              precision = 38;
            }
            if (scale < 0 || scale > precision) {
              LOGGER.warn("Invalid scale {} found for Decimal field '{}' with precision {}, using default 10.", scale, fieldName, precision);
              scale = Math.min(10, precision);
            }
            return Types.DecimalType.of(precision, scale);
          } else {
            LOGGER.warn("Decimal logical type for field '{}' is missing scale parameter. Defaulting to Decimal(38, 10).", fieldName);
            return Types.DecimalType.of(38, 10);
          }
        }
        return Types.BinaryType.get();
      default:
        // default to String type
        LOGGER.warn("Unsupported Kafka Connect schema type '{}' for field '{}'. Defaulting to String.", fieldType, fieldName);
        return Types.StringType.get();
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StructSchemaConverter that = (StructSchemaConverter) o;
    // Schema comparison in Kafka Connect can be complex. Using Objects.equals might not be
    // sufficient for deep comparison or logical equivalence. For now, rely on standard equals.
    return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueSchema, keySchema);
  }
}