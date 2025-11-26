package io.debezium.server.iceberg.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.DebeziumException;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class JsonSchemaConverter implements io.debezium.server.iceberg.converter.SchemaConverter {
  private final JsonNode valueSchema;
  private final JsonNode keySchema;
  private final GlobalConfig config;
  protected static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaConverter.class);
  protected static final ObjectMapper mapper = new ObjectMapper();


  public JsonSchemaConverter(JsonNode valueSchema, JsonNode keySchema, GlobalConfig config) {
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
    this.config = config;
  }

  protected JsonNode valueSchema() {
    return valueSchema;
  }

  protected JsonNode keySchema() {
    return keySchema;
  }

  private static String getFieldName(JsonNode fieldSchema) {
    JsonNode nameNode = fieldSchema.get("name");
    if (nameNode == null || nameNode.isNull()) {
      return "";
    }

    return nameNode.textValue();
  }

  private static boolean getFieldIsOptional(JsonNode fieldSchema) {
    JsonNode nameNode = fieldSchema.get("optional");
    if (nameNode == null || nameNode.isNull()) {
      return true;
    }

    return nameNode.booleanValue();
  }

  /***
   * converts given debezium filed to iceberg field equivalent. does recursion in case of complex/nested types.
   *
   * @param fieldSchema JsonNode representation of debezium field schema.
   * @param fieldName name of the debezium field
   * @param schemaData keeps information of iceberg schema like fields, nextFieldId and identifier fields
   * @return map entry Key being the last id assigned to the iceberg field, Value being the converted iceberg NestedField.
   */
  private IcebergSchemaInfo debeziumFieldToIcebergField(JsonNode fieldSchema, String fieldName, IcebergSchemaInfo schemaData, JsonNode keySchemaNode) {
    String fieldType = fieldSchema.get("type").textValue();
    String fieldTypeName = getFieldName(fieldSchema);
    boolean fieldIsOptional = getFieldIsOptional(fieldSchema);

    if (fieldType == null || fieldType.isBlank()) {
      throw new DebeziumException("Unexpected schema field, field type is null or empty, fieldSchema:" + fieldSchema + " fieldName:" + fieldName);
    }

    boolean isPkField = !(keySchemaNode == null || keySchemaNode.isNull());
    boolean isOptional = config.iceberg().preserveRequiredProperty() ? fieldIsOptional : !isPkField;
    switch (fieldType) {
      case "struct":
        if (config.iceberg().nestedAsVariant()) {
          // here we are keeping nested fields in variant
          int variantFieldId = schemaData.nextFieldId().getAndIncrement();
          final Types.NestedField variantField = Types.NestedField.optional(variantFieldId, fieldName, Types.VariantType.get());
          schemaData.fields().add(variantField);
          return schemaData;
        }

        int rootStructId = schemaData.nextFieldId().getAndIncrement();
        final IcebergSchemaInfo subSchemaData = schemaData.copyPreservingMetadata();
        for (JsonNode subFieldSchema : fieldSchema.get("fields")) {
          String subFieldName = subFieldSchema.get("field").textValue();
          JsonNode equivalentNestedKeyField = findNodeFieldByName(subFieldName, keySchemaNode);
          debeziumFieldToIcebergField(subFieldSchema, subFieldName, subSchemaData, equivalentNestedKeyField);
        }
        // create it as struct, nested type
        final Types.StructType structType = Types.StructType.of(subSchemaData.fields());
        final Types.NestedField structField = Types.NestedField.of(rootStructId, isOptional, fieldName, structType);
        schemaData.fields().add(structField);
        return schemaData;
      case "map":
        if (isPkField) {
          throw new DebeziumException("Map field '" + fieldName + "' cannot be part of the primary key.");
        }
        int rootMapId = schemaData.nextFieldId().getAndIncrement();
        int keyFieldId = schemaData.nextFieldId().getAndIncrement();
        int valFieldId = schemaData.nextFieldId().getAndIncrement();
        // Convert key schema
        final IcebergSchemaInfo keySchemaData = schemaData.copyPreservingMetadata();
        debeziumFieldToIcebergField(fieldSchema.get("keys"), fieldName + "_key", keySchemaData, null);

        // Convert value schema
        final IcebergSchemaInfo valSchemaData = schemaData.copyPreservingMetadata();
        debeziumFieldToIcebergField(fieldSchema.get("values"), fieldName + "_val", valSchemaData, null);
        final Types.MapType mapField = Types.MapType.ofOptional(keyFieldId, valFieldId, keySchemaData.fields().get(0).type(), valSchemaData.fields().get(0).type());
        schemaData.fields().add(Types.NestedField.of(rootMapId, isOptional, fieldName, mapField));
        return schemaData;

      case "array":
        if (isPkField) {
          throw new DebeziumException("Cannot set array field '" + fieldName + "' as a identifier field, array types are not supported as an identifier field!");
        }
        int rootArrayId = schemaData.nextFieldId().getAndIncrement();
        final IcebergSchemaInfo arraySchemaData = schemaData.copyPreservingMetadata();
        debeziumFieldToIcebergField(fieldSchema.get("items"), fieldName + "_items", arraySchemaData, null);
        final Types.ListType listField = Types.ListType.ofOptional(schemaData.nextFieldId().getAndIncrement(), arraySchemaData.fields().get(0).type());
        schemaData.fields().add(Types.NestedField.of(rootArrayId, isOptional, fieldName, listField));
        return schemaData;
      default:
        // its primitive field
        final Types.NestedField field = Types.NestedField.of(schemaData.nextFieldId().getAndIncrement(), isOptional, fieldName, icebergPrimitiveField(fieldName, fieldType, fieldTypeName, fieldSchema));
        schemaData.fields().add(field);
        if (isPkField) schemaData.identifierFieldIds().add(field.fieldId());
        return schemaData;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueSchema, keySchema);
  }

  private static JsonNode getNodeFieldsArray(JsonNode node) {
    if (node != null && !node.isNull() && node.has("fields") && node.get("fields").isArray()) {
      return node.get("fields");
    }
    return mapper.createObjectNode();
  }

  private static JsonNode findNodeFieldByName(String fieldName, JsonNode node) {
    for (JsonNode field : getNodeFieldsArray(node)) {
      if (field.has("field") && Objects.equals(field.get("field").textValue(), fieldName)) {
        return field;
      }
    }
    return null;
  }

  private JsonNode keySchemaNode() {
    if (!config.debezium().isEventFlatteningEnabled() && keySchema != null) {
      ObjectNode nestedKeySchema = mapper.createObjectNode();
      nestedKeySchema.put("type", "struct");
      nestedKeySchema.putArray("fields").add(((ObjectNode) keySchema).put("field", "after"));
      return nestedKeySchema;
    }
    return keySchema;
  }

  /***
   * Converts debezium event fields to iceberg equivalent and returns list of iceberg fields.
   * @param schemaNode
   * @return
   */
  private IcebergSchemaInfo icebergSchemaFields(JsonNode schemaNode, JsonNode keySchemaNode, IcebergSchemaInfo schemaData) {
    LOGGER.debug("Converting iceberg schema to debezium:{}", schemaNode);
    List<String> excludedColumns = this.config.iceberg()
          .excludedColumns()
          .orElse(Collections.emptyList());

    for (JsonNode field : getNodeFieldsArray(schemaNode)) {
      String fieldName = field.get("field").textValue();
        if(excludedColumns.contains(fieldName)) {
            continue;
        }

      JsonNode equivalentKeyFieldNode = findNodeFieldByName(fieldName, keySchemaNode);
      debeziumFieldToIcebergField(field, fieldName, schemaData, equivalentKeyFieldNode);
    }

    return schemaData;
  }

  @Override
  public Schema icebergSchema() {

    if (this.valueSchema.isNull()) {
      throw new DebeziumException("Failed to get schema from debezium event, event schema is null");
    }

    IcebergSchemaInfo schemaData = new IcebergSchemaInfo();
    final JsonNode keySchemaNode = this.keySchemaNode();

    icebergSchemaFields(valueSchema, keySchemaNode, schemaData);

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
      return new Schema(schemaData.fields());
    }
    if (config.iceberg().nestedAsVariant()) {
      LOGGER.warn("Identifier fields are not supported when data consumed to variant fields, creating schema without identifier fields!");
      return new Schema(schemaData.fields());
    }

    // @TODO validate key fields are correctly set!?
    return new Schema(schemaData.fields(), schemaData.identifierFieldIds());
  }

  @Override
  public SortOrder sortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    List<String> excludedColumns = config.iceberg().excludedColumns().orElse(Collections.emptyList());
    for (JsonNode field : getNodeFieldsArray(keySchemaNode())) {
      if (!field.has("field")) continue;
      String fieldName = field.get("field").textValue();
      if (excludedColumns.contains(fieldName)) continue;
      sob = sob.asc(fieldName);
    }
    return sob.build();
  }

  private Type.PrimitiveType icebergPrimitiveField(String fieldName, String fieldType, String fieldTypeName, JsonNode fieldSchema) {
    // Debezium Temporal types: https://debezium.io/documentation//reference/connectors/postgresql.html#postgresql-temporal-types
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32": // int 4 bytes
        return switch (fieldTypeName) {
          case "io.debezium.time.Date", "org.apache.kafka.connect.data.Date" -> Types.DateType.get();
// NOTE: Time type is disable for the moment, it's not supported by spark
//          //"Represents the number of milliseconds"
//          case "io.debezium.time.Time" -> Types.TimeType.get();
//          //"Represents the time value in microseconds
//          case "io.debezium.time.MicroTime" -> Types.TimeType.get();
//          //"Represents the time value in nanoseconds"
//          case "io.debezium.time.NanoTime" -> Types.TimeType.get();
//          //"Represents the time value in microseconds"
//          case "org.apache.kafka.connect.data.Time" -> Types.TimeType.get();
          default -> Types.IntegerType.get();
        };
      case "int64": // long 8 bytes
        if (DebeziumConfig.TS_MS_FIELDS.contains(fieldName)) {
          return Types.TimestampType.withZone();
        }
        if (config.debezium().isAdaptiveTemporalMode()) {
          return Types.LongType.get();
        }
        return switch (fieldTypeName) {
          case "io.debezium.time.Timestamp" -> Types.TimestampType.withoutZone();
          case "io.debezium.time.MicroTimestamp" -> Types.TimestampType.withoutZone();
          case "io.debezium.time.NanoTimestamp" -> Types.TimestampType.withoutZone();
          case "org.apache.kafka.connect.data.Timestamp" -> Types.TimestampType.withoutZone();
// NOTE: Time type is disable for the moment, it's not supported by spark
//          //"Represents the number of milliseconds"
//          case "io.debezium.time.Time" -> Types.TimeType.get();
//          //"Represents the time value in microseconds
//          case "io.debezium.time.MicroTime" -> Types.TimeType.get();
//          //"Represents the time value in nanoseconds"
//          case "io.debezium.time.NanoTime" -> Types.TimeType.get();
//          //"Represents the time value in microseconds"
//          case "org.apache.kafka.connect.data.Time" -> Types.TimeType.get();
          default -> Types.LongType.get();
        };
      case "float8":
      case "float16":
      case "float32": // float is represented in 32 bits,
        return Types.FloatType.get();
      case "double":
      case "float64": // double is represented in 64 bits
        return Types.DoubleType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "string":
        return switch (fieldTypeName) {
          case "io.debezium.data.Uuid" -> Types.UUIDType.get();
          case "io.debezium.time.IsoDate" -> Types.DateType.get();
          case "io.debezium.time.IsoTimestamp" -> Types.TimestampType.withoutZone();
          case "io.debezium.time.ZonedTimestamp" -> Types.TimestampType.withZone();
          // NOTE: Time type is disable for the moment, it's not supported by spark
          // case "io.debezium.time.IsoTime" -> Types.TimeType.get();
          // case "io.debezium.time.ZonedTime" -> Types.TimeType.get();
          default -> Types.StringType.get();
        };
      case "uuid":
        return Types.UUIDType.get();
      case "bytes":
        // With `decimal.handling.mode` set to `precise` debezium relational source connector would encode decimals
        // as byte arrays. We want to write them out as Iceberg decimals.
        if (fieldTypeName.equals("org.apache.kafka.connect.data.Decimal")) {
          JsonNode params = fieldSchema.get("parameters");
          if (!params.isNull() && !params.isEmpty()) {
            int precision = params.get("connect.decimal.precision").asInt();
            int scale = params.get("scale").asInt();
            return Types.DecimalType.of(precision, scale);
          }
        }
        return Types.BinaryType.get();
      default:
        // default to String type
        LOGGER.warn("Unsupported schema type '{}' for field '{}'. Defaulting to String.", fieldType, fieldName);
        return Types.StringType.get();
      //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JsonSchemaConverter that = (JsonSchemaConverter) o;
    return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
  }


}
