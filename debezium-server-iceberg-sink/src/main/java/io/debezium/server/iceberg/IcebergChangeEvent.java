/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Ismail Simsek
 */
public class IcebergChangeEvent {

  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeEvent.class);
  public static final List<String> TS_MS_FIELDS = List.of("__ts_ms", "__source_ts_ms");
  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  final JsonSchema jsonSchema;

  public IcebergChangeEvent(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.jsonSchema = new JsonSchema(valueSchema, keySchema);
  }

  public JsonNode key() {
    return key;
  }

  public JsonNode value() {
    return value;
  }

  public JsonSchema jsonSchema() {
    return jsonSchema;
  }

  public Schema icebergSchema() {
    return jsonSchema.icebergSchema();
  }

  public String destination() {
    return destination;
  }

  public GenericRecord asIcebergRecord(Schema schema) {
    return asIcebergRecord(schema.asStruct(), value);
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
        if (node.isLong() && TS_MS_FIELDS.contains(field.name())) {
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

        val = IcebergChangeConsumer.mapper.convertValue(node, ArrayList.class);
        break;
      case MAP:
        Type keyType = field.type().asMapType().keyType();
        Type valType = field.type().asMapType().valueType();
        if (keyType.isPrimitiveType() && valType.isPrimitiveType()) {
          val = IcebergChangeConsumer.mapper.convertValue(node, Map.class);
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

  /***
   * converts given debezium filed to iceberg field equivalent. does recursion in case of complex/nested types.
   *
   * @param fieldSchema JsonNode representation of debezium field schema.
   * @param fieldName name of the debezium field
   * @param fieldId id sequence to assign iceberg field, after the conversion.
   * @return map entry Key being the last id assigned to the iceberg field, Value being the converted iceberg NestedField.
   */
  private static Map.Entry<Integer, Types.NestedField> debeziumFieldToIcebergField(JsonNode fieldSchema, String fieldName, int fieldId) {
    String fieldType = fieldSchema.get("type").textValue();
    switch (fieldType) {
      case "struct":
        // struct type
        int rootStructId = fieldId;
        List<Types.NestedField> subFields = new ArrayList<>();
        for (JsonNode subFieldSchema : fieldSchema.get("fields")) {
          fieldId += 1;
          String subFieldName = subFieldSchema.get("field").textValue();
          Map.Entry<Integer, Types.NestedField> subField = debeziumFieldToIcebergField(subFieldSchema, subFieldName, fieldId);
          subFields.add(subField.getValue());
          fieldId = subField.getKey();
        }
        // create it as struct, nested type
        return new AbstractMap.SimpleEntry<>(fieldId, Types.NestedField.optional(rootStructId, fieldName, Types.StructType.of(subFields)));
      case "map":
        int rootMapId = fieldId;
        int keyFieldId = fieldId + 1;
        int valFieldId = fieldId + 2;
        fieldId = fieldId + 3;
        Map.Entry<Integer, Types.NestedField> keyField = debeziumFieldToIcebergField(fieldSchema.get("keys"), fieldName + "_key", fieldId);
        fieldId = keyField.getKey() + 1;
        Map.Entry<Integer, Types.NestedField> valField = debeziumFieldToIcebergField(fieldSchema.get("values"), fieldName + "_val", fieldId);
        fieldId = valField.getKey();
        Types.MapType mapField = Types.MapType.ofOptional(keyFieldId, valFieldId, keyField.getValue().type(), valField.getValue().type());
        return new AbstractMap.SimpleEntry<>(fieldId, Types.NestedField.optional(rootMapId, fieldName, mapField));

      case "array":
        int rootArrayId = fieldId;
        fieldId += 1;
        Map.Entry<Integer, Types.NestedField> listItemsField = debeziumFieldToIcebergField(fieldSchema.get("items"), fieldName + "_items", fieldId);
        fieldId = listItemsField.getKey() + 1;
        Types.ListType listField = Types.ListType.ofOptional(fieldId, listItemsField.getValue().type());
        return new AbstractMap.SimpleEntry<>(fieldId, Types.NestedField.optional(rootArrayId, fieldName, listField));
      default:
        // its primitive field
        return new AbstractMap.SimpleEntry<>(fieldId, Types.NestedField.optional(fieldId, fieldName, icebergPrimitiveField(fieldName, fieldType)));
    }
  }

  /***
   * Converts debezium event fields to iceberg equivalent and returns list of iceberg fields.
   * @param schemaNode
   * @return
   */
  private static List<Types.NestedField> icebergSchemaFields(JsonNode schemaNode) {
    List<Types.NestedField> schemaColumns = new ArrayList<>();
    AtomicReference<Integer> fieldId = new AtomicReference<>(1);
    if (schemaNode != null && schemaNode.has("fields") && schemaNode.get("fields").isArray()) {
      LOGGER.debug("Converting iceberg schema to debezium:{}", schemaNode);
      schemaNode.get("fields").forEach(field -> {
        Map.Entry<Integer, Types.NestedField> df = debeziumFieldToIcebergField(field, field.get("field").textValue(), fieldId.get());
        fieldId.set(df.getKey() + 1);
        schemaColumns.add(df.getValue());
      });
    }
    return schemaColumns;
  }


  public static class JsonSchema {
    private final JsonNode valueSchema;
    private final JsonNode keySchema;

    JsonSchema(JsonNode valueSchema, JsonNode keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    public JsonNode valueSchema() {
      return valueSchema;
    }

    public JsonNode keySchema() {
      return keySchema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JsonSchema that = (JsonSchema) o;
      return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueSchema, keySchema);
    }

    public Schema icebergSchema() {

      if (this.valueSchema == null) {
        throw new RuntimeException("Failed to get event schema, event schema is null");
      }

      final List<Types.NestedField> tableColumns = icebergSchemaFields(valueSchema);

      if (tableColumns.isEmpty()) {
        throw new RuntimeException("Failed to get event schema, event schema has no fields!");
      }

      final List<Types.NestedField> keyColumns = icebergSchemaFields(keySchema);
      Set<Integer> identifierFieldIds = new HashSet<>();

      for (Types.NestedField ic : keyColumns) {
        boolean found = false;

        ListIterator<Types.NestedField> colsIterator = tableColumns.listIterator();
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
          throw new ValidationException("Table Row identifier field `" + ic.name() + "` not found in table columns");
        }

      }

      return new Schema(tableColumns, identifierFieldIds);
    }
  }

}
