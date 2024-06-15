/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static io.debezium.server.iceberg.IcebergChangeConsumer.keyDeserializer;
import static io.debezium.server.iceberg.IcebergChangeConsumer.valDeserializer;

/**
 * Converts iceberg json event to Iceberg GenericRecord. Extracts event schema and key fields. Converts event schema to Iceberg Schema.
 *
 * @author Ismail Simsek
 */
public class IcebergChangeEvent {

  protected static final ObjectMapper mapper = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeEvent.class);
  public static final List<String> TS_MS_FIELDS = List.of("__ts_ms", "__source_ts_ms");
  protected final String destination;
  protected final byte[] valueData;
  protected final byte[] keyData;
  private JsonNode value;
  private JsonNode key;

  public IcebergChangeEvent(String destination, byte[] valueData, byte[] keyData) {
    this.destination = destination;
    this.valueData = valueData;
    this.keyData = keyData;
  }

  public JsonNode key() {
    if (key == null && keyData != null) {
      key = keyDeserializer.deserialize(destination, keyData);
    }

    return key;
  }

  public JsonNode value() {
    if (value == null && valueData != null) {
      value = valDeserializer.deserialize(destination, valueData);
    }

    return value;
  }

  public ChangeEventSchema changeEventSchema() {
    try {
      return new ChangeEventSchema(mapper.readTree(valueData).get("schema"), keyData == null ? null : mapper.readTree(keyData).get("schema"));
    } catch (IOException e) {
      throw new DebeziumException("Failed to get event schema", e);
    }
  }

  public Schema icebergSchema() {
    return changeEventSchema().icebergSchema(this.isUnwrapped());
  }

  public String destination() {
    return destination;
  }

  public boolean isUnwrapped() {
    return !(
        this.value().has("after") &&
            this.value().has("source") &&
            this.value().has("before") &&
            this.value().get("after").isObject() &&
            this.value().get("source").isObject()
    );
  }

  public GenericRecord asIcebergRecord(Schema schema) {
    return asIcebergRecord(schema.asStruct(), value());
  }

  private static GenericRecord asIcebergRecord(Types.StructType tableFields, JsonNode data) {
    LOGGER.debug("Processing nested field:{}", tableFields);
    GenericRecord record = GenericRecord.create(tableFields);

    for (Types.NestedField field : tableFields.fields()) {
      // Set value to null if json event don't have the field
      if (data == null || !data.has(field.name()) || data.get(field.name()) == null) {
        record.setField(field.name(), null);
        continue;
      }
      // get the value of the field from json event, map it to iceberg value
      record.setField(field.name(), jsonValToIcebergVal(field, data.get(field.name())));
    }

    return record;
  }

  private static Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
    LOGGER.debug("Processing Field:{} Type:{}", field.name(), field.type());
    final Object val;
    switch (field.type().typeId()) {
      case INTEGER: // int 4 bytes
        val = node.isNull() ? null : node.asInt();
        break;
      case LONG: // long 8 bytes
        val = node.isNull() ? null : node.asLong();
        break;
      case FLOAT: // float is represented in 32 bits,
        val = node.isNull() ? null : node.floatValue();
        break;
      case DOUBLE: // double is represented in 64 bits
        val = node.isNull() ? null : node.asDouble();
        break;
      case BOOLEAN:
        val = node.isNull() ? null : node.asBoolean();
        break;
      case STRING:
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
      case UUID:
        val = node.isValueNode() ? UUID.fromString(node.asText(null)) : UUID.fromString(node.toString());
        break;
      case TIMESTAMP:
        if ((node.isLong() || node.isNumber()) && TS_MS_FIELDS.contains(field.name())) {
          val = OffsetDateTime.ofInstant(Instant.ofEpochMilli(node.longValue()), ZoneOffset.UTC);
        } else if (node.isTextual()) {
          val = OffsetDateTime.parse(node.asText());
        } else {
          throw new RuntimeException("Failed to convert timestamp value, field: " + field.name() + " value: " + node);
        }
        break;
      case BINARY:
        try {
          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          throw new RuntimeException("Failed to convert binary value to iceberg value, field: " + field.name(), e);
        }
        break;
      case LIST:
        Types.NestedField listItemsType = field.type().asListType().fields().get(0);
        // recursive value mapping when list elements are nested type
        if (listItemsType.type().isNestedType()) {
          ArrayList<Object> listVal = new ArrayList<>();
          node.elements().forEachRemaining(element -> {
            listVal.add(jsonValToIcebergVal(field.type().asListType().fields().get(0), element));
          });
          val = listVal;
          break;
        }

        val = mapper.convertValue(node, ArrayList.class);
        break;
      case MAP:
        Type keyType = field.type().asMapType().keyType();
        Type valType = field.type().asMapType().valueType();
        if (keyType.isPrimitiveType() && valType.isPrimitiveType()) {
          val = mapper.convertValue(node, Map.class);
          break;
        }
        // convert complex/nested map value with recursion
        HashMap<Object, Object> mapVal = new HashMap<>();
        node.fields().forEachRemaining(f -> {
          if (valType.isStructType()) {
            mapVal.put(f.getKey(), asIcebergRecord(valType.asStructType(), f.getValue()));
          } else {
            mapVal.put(f.getKey(), f.getValue());
          }
        });
        val = mapVal;
        break;
      case STRUCT:
        // create it as struct, nested type
        // recursive call to get nested data/record
        val = asIcebergRecord(field.type().asStructType(), node);
        break;
      default:
        // default to String type
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
    }

    return val;
  }


  public static class ChangeEventSchema {
    private final JsonNode valueSchema;
    private final JsonNode keySchema;

    ChangeEventSchema(JsonNode valueSchema, JsonNode keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    protected JsonNode valueSchema() {
      return valueSchema;
    }

    protected JsonNode keySchema() {
      return keySchema;
    }

    /***
     * converts given debezium filed to iceberg field equivalent. does recursion in case of complex/nested types.
     *
     * @param fieldSchema JsonNode representation of debezium field schema.
     * @param fieldName name of the debezium field
     * @param schemaData keeps information of iceberg schema like fields, nextFieldId and identifier fields
     * @return map entry Key being the last id assigned to the iceberg field, Value being the converted iceberg NestedField.
     */
    private static IcebergChangeEventSchemaData debeziumFieldToIcebergField(JsonNode fieldSchema, String fieldName, IcebergChangeEventSchemaData schemaData) {
      String fieldType = fieldSchema.get("type").textValue();
      switch (fieldType) {
        case "struct":
          int rootStructId = schemaData.nextFieldId().getAndIncrement();
          IcebergChangeEventSchemaData subSchemaData = schemaData.copyKeepNextFieldId();
          for (JsonNode subFieldSchema : fieldSchema.get("fields")) {
            String subFieldName = subFieldSchema.get("field").textValue();
            debeziumFieldToIcebergField(subFieldSchema, subFieldName, subSchemaData);
          }
          // create it as struct, nested type
          schemaData.fields().add(Types.NestedField.optional(rootStructId, fieldName, Types.StructType.of(subSchemaData.fields())));
          return schemaData;
        case "map":
          int rootMapId = schemaData.nextFieldId().getAndIncrement();
          int keyFieldId = schemaData.nextFieldId().getAndIncrement();
          int valFieldId = schemaData.nextFieldId().getAndIncrement();
          IcebergChangeEventSchemaData keySchemaData = schemaData.copyKeepNextFieldId();
          debeziumFieldToIcebergField(fieldSchema.get("keys"), fieldName + "_key", keySchemaData);
          schemaData.nextFieldId().incrementAndGet();
          IcebergChangeEventSchemaData valSchemaData = schemaData.copyKeepNextFieldId();
          debeziumFieldToIcebergField(fieldSchema.get("values"), fieldName + "_val", valSchemaData);
          Types.MapType mapField = Types.MapType.ofOptional(keyFieldId, valFieldId, keySchemaData.fields().get(0).type(), valSchemaData.fields().get(0).type());
          schemaData.fields().add(Types.NestedField.optional(rootMapId, fieldName, mapField));
          return schemaData;

        case "array":
          int rootArrayId = schemaData.nextFieldId().getAndIncrement();
          IcebergChangeEventSchemaData arraySchemaData = schemaData.copyKeepNextFieldId();
          debeziumFieldToIcebergField(fieldSchema.get("items"), fieldName + "_items", arraySchemaData);
          Types.ListType listField = Types.ListType.ofOptional(schemaData.nextFieldId().getAndIncrement(), arraySchemaData.fields().get(0).type());
          schemaData.fields().add(Types.NestedField.optional(rootArrayId, fieldName, listField));
          return schemaData;
        default:
          // its primitive field
          schemaData.fields().add(Types.NestedField.optional(schemaData.nextFieldId().getAndIncrement(), fieldName, icebergPrimitiveField(fieldName, fieldType)));
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

        if (Objects.equals(field.get("field").textValue(), fieldName)) {
          return field;
        }
      }

      return null;
    }

    private Schema icebergSchema(boolean isUnwrapped) {

      if (this.valueSchema.isNull()) {
        throw new RuntimeException("Failed to get schema from debezium event, event schema is null");
      }

      final IcebergChangeEventSchemaData tableColumns = icebergSchemaFields(valueSchema);

      if (tableColumns.fields().isEmpty()) {
        throw new RuntimeException("Failed to get schema from debezium event, event schema has no fields!");
      }

      final IcebergChangeEventSchemaData keyColumns = icebergSchemaFields(keySchema);
      Set<Integer> identifierFieldIds = new HashSet<>();

      for (Types.NestedField ic : keyColumns.fields()) {
        boolean found = false;

        ListIterator<Types.NestedField> colsIterator = tableColumns.fields().listIterator();
        while (colsIterator.hasNext()) {
          Types.NestedField tc = colsIterator.next();
          if (Objects.equals(tc.name(), ic.name())) {
            identifierFieldIds.add(tc.fieldId());
            // set column as required its part of identifier filed
            colsIterator.set(tc.asRequired());
            found = true;
            break;
          }
        }

        if (!found) {
          throw new ValidationException("Debezium key/identifier field `" + ic.name() + "` not found in event columns!");
        }

      }

      return new Schema(tableColumns.fields(), identifierFieldIds);
    }

    /***
     * Converts debezium event fields to iceberg equivalent and returns list of iceberg fields.
     * @param schemaNode
     * @return
     */
    private static IcebergChangeEventSchemaData icebergSchemaFields(JsonNode schemaNode) {
      //List<Types.NestedField> schemaColumns = new ArrayList<>();
      //AtomicReference<Integer> fieldId = new AtomicReference<>(1);
      IcebergChangeEventSchemaData schemaData = new IcebergChangeEventSchemaData();
      LOGGER.debug("Converting iceberg schema to debezium:{}", schemaNode);
      for (JsonNode field : getNodeFieldsArray(schemaNode)) {
        schemaData = debeziumFieldToIcebergField(field, field.get("field").textValue(), schemaData);
        //fieldId.set(df.getKey() + 1);
        //schemaColumns.add(df.getValue());
      }

      return schemaData;
    }

    private static Type.PrimitiveType icebergPrimitiveField(String fieldName, String fieldType) {
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32": // int 4 bytes
          return Types.IntegerType.get();
        case "int64": // long 8 bytes
          if (TS_MS_FIELDS.contains(fieldName)) {
            return Types.TimestampType.withZone();
          } else {
            return Types.LongType.get();
          }
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
          return Types.StringType.get();
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
      ChangeEventSchema that = (ChangeEventSchema) o;
      return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
    }


  }


}
