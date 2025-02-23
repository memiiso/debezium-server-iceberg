package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.DebeziumException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.Objects;

public class SchemaConverter {
  private final JsonNode valueSchema;
  private final JsonNode keySchema;
  private final IcebergConsumerConfig config;

  public SchemaConverter(JsonNode valueSchema, JsonNode keySchema, IcebergConsumerConfig config) {
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

    if (fieldType == null || fieldType.isBlank()) {
      throw new DebeziumException("Unexpected schema field, field type is null or empty, fieldSchema:" + fieldSchema + " fieldName:" + fieldName);
    }

    boolean isPkField = !(keySchemaNode == null || keySchemaNode.isNull());
    switch (fieldType) {
      case "struct":
        int rootStructId = schemaData.nextFieldId().getAndIncrement();
        final IcebergSchemaInfo subSchemaData = schemaData.copyPreservingMetadata();
        for (JsonNode subFieldSchema : fieldSchema.get("fields")) {
          String subFieldName = subFieldSchema.get("field").textValue();
          JsonNode equivalentNestedKeyField = findNodeFieldByName(subFieldName, keySchemaNode);
          debeziumFieldToIcebergField(subFieldSchema, subFieldName, subSchemaData, equivalentNestedKeyField);
        }
        // create it as struct, nested type
        final Types.NestedField structField = Types.NestedField.of(rootStructId, !isPkField, fieldName, Types.StructType.of(subSchemaData.fields()));
        schemaData.fields().add(structField);
        return schemaData;
      case "map":
        if (isPkField) {
          throw new DebeziumException("Cannot set map field '" + fieldName + "' as a identifier field, map types are not supported as an identifier field!");
        }
        int rootMapId = schemaData.nextFieldId().getAndIncrement();
        int keyFieldId = schemaData.nextFieldId().getAndIncrement();
        int valFieldId = schemaData.nextFieldId().getAndIncrement();
        final IcebergSchemaInfo keySchemaData = schemaData.copyPreservingMetadata();
        debeziumFieldToIcebergField(fieldSchema.get("keys"), fieldName + "_key", keySchemaData, null);
        schemaData.nextFieldId().incrementAndGet();
        final IcebergSchemaInfo valSchemaData = schemaData.copyPreservingMetadata();
        debeziumFieldToIcebergField(fieldSchema.get("values"), fieldName + "_val", valSchemaData, null);
        final Types.MapType mapField = Types.MapType.ofOptional(keyFieldId, valFieldId, keySchemaData.fields().get(0).type(), valSchemaData.fields().get(0).type());
        schemaData.fields().add(Types.NestedField.optional(rootMapId, fieldName, mapField));
        return schemaData;

      case "array":
        if (isPkField) {
          throw new DebeziumException("Cannot set array field '" + fieldName + "' as a identifier field, array types are not supported as an identifier field!");
        }
        int rootArrayId = schemaData.nextFieldId().getAndIncrement();
        final IcebergSchemaInfo arraySchemaData = schemaData.copyPreservingMetadata();
        debeziumFieldToIcebergField(fieldSchema.get("items"), fieldName + "_items", arraySchemaData, null);
        final Types.ListType listField = Types.ListType.ofOptional(schemaData.nextFieldId().getAndIncrement(), arraySchemaData.fields().get(0).type());
        schemaData.fields().add(Types.NestedField.optional(rootArrayId, fieldName, listField));
        return schemaData;
      default:
        // its primitive field
        final Types.NestedField field = Types.NestedField.of(schemaData.nextFieldId().getAndIncrement(), !isPkField, fieldName, icebergPrimitiveField(fieldName, fieldType, fieldTypeName));
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
    return RecordConverter.mapper.createObjectNode();
  }

  private static JsonNode findNodeFieldByName(String fieldName, JsonNode node) {
    for (JsonNode field : getNodeFieldsArray(node)) {
      if (field.has("field") && Objects.equals(field.get("field").textValue(), fieldName)) {
        return field;
      }
    }
    return null;
  }


  /***
   * Converts debezium event fields to iceberg equivalent and returns list of iceberg fields.
   * @param schemaNode
   * @return
   */
  private IcebergSchemaInfo icebergSchemaFields(JsonNode schemaNode, JsonNode keySchemaNode, IcebergSchemaInfo schemaData) {
    RecordConverter.LOGGER.debug("Converting iceberg schema to debezium:{}", schemaNode);
    for (JsonNode field : getNodeFieldsArray(schemaNode)) {
      String fieldName = field.get("field").textValue();
      JsonNode equivalentKeyFieldNode = findNodeFieldByName(fieldName, keySchemaNode);
      debeziumFieldToIcebergField(field, fieldName, schemaData, equivalentKeyFieldNode);
    }

    return schemaData;
  }

  public Schema icebergSchema() {

    if (this.valueSchema.isNull()) {
      throw new RuntimeException("Failed to get schema from debezium event, event schema is null");
    }

    IcebergSchemaInfo schemaData = new IcebergSchemaInfo();
    final JsonNode keySchemaNode;
    if (!config.createIdentifierFields()) {
      RecordConverter.LOGGER.warn("Creating identifier fields is disabled, creating table without identifier fields!");
      keySchemaNode = null;
    } else if (!RecordConverter.eventsAreUnwrapped && keySchema != null) {
      ObjectNode nestedKeySchema = RecordConverter.mapper.createObjectNode();
      nestedKeySchema.put("type", "struct");
      nestedKeySchema.putArray("fields").add(((ObjectNode) keySchema).put("field", "after"));
      keySchemaNode = nestedKeySchema;
    } else {
      keySchemaNode = keySchema;
    }

    icebergSchemaFields(valueSchema, keySchemaNode, schemaData);

    if (!RecordConverter.eventsAreUnwrapped && !schemaData.identifierFieldIds().isEmpty()) {
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

    // @TODO validate key fields are correctly set!?
    return new Schema(schemaData.fields(), schemaData.identifierFieldIds());

  }

  private Type.PrimitiveType icebergPrimitiveField(String fieldName, String fieldType, String fieldTypeName) {
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
        if (RecordConverter.TS_MS_FIELDS.contains(fieldName)) {
          return Types.TimestampType.withZone();
        }
        if (config.isAdaptiveTemporalMode()) {
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
        return Types.BinaryType.get();
      default:
        // default to String type
        return Types.StringType.get();
      //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaConverter that = (SchemaConverter) o;
    return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
  }


}
