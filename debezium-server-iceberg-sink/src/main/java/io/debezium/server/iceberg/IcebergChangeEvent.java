/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class IcebergChangeEvent {

  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeEvent.class);
  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  JsonSchema jsonSchema;

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
    final GenericRecord record = asIcebergRecord(schema.asStruct(), value);

    if (value != null && value.has("__source_ts_ms") && value.get("__source_ts_ms") != null) {
      final long source_ts_ms = value.get("__source_ts_ms").longValue();
      final OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(source_ts_ms), ZoneOffset.UTC);
      record.setField("__source_ts", odt);
    } else {
      record.setField("__source_ts", null);
    }
    return record;
  }

  private GenericRecord asIcebergRecord(Types.StructType tableFields, JsonNode data) {
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

  private Type.PrimitiveType icebergFieldType(String fieldType) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32": // int 4 bytes
        return Types.IntegerType.get();
      case "int64": // long 8 bytes
        return Types.LongType.get();
      case "float8":
      case "float16":
      case "float32": // float is represented in 32 bits,
        return Types.FloatType.get();
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

  private Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
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
      case BINARY:
        try {
          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          throw new RuntimeException("Failed to convert binary value to iceberg value, field: " + field.name(), e);
        }
        break;
      case LIST:
        val = IcebergChangeConsumer.mapper.convertValue(node, ArrayList.class);
        break;
      case MAP:
        val = IcebergChangeConsumer.mapper.convertValue(node, Map.class);
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

  public class JsonSchema {
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

    //getIcebergFieldsFromEventSchema
    private List<Types.NestedField> KeySchemaFields() {
      if (keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray()) {
        LOGGER.debug(keySchema.toString());
        return icebergSchema(keySchema, "", 0);
      }
      LOGGER.trace("Key schema not found!");
      return new ArrayList<>();
    }

    private List<Types.NestedField> valueSchemaFields() {
      if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
        LOGGER.debug(valueSchema.toString());
        return icebergSchema(valueSchema, "", 0, true);
      }
      LOGGER.trace("Event schema not found!");
      return new ArrayList<>();
    }

    public Schema icebergSchema() {

      if (this.valueSchema == null) {
        throw new RuntimeException("Failed to get event schema, event schema is null");
      }

      final List<Types.NestedField> tableColumns = valueSchemaFields();

      if (tableColumns.isEmpty()) {
        throw new RuntimeException("Failed to get event schema, event schema has no fields!");
      }

      final List<Types.NestedField> keyColumns = KeySchemaFields();
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

    private List<Types.NestedField> icebergSchema(JsonNode eventSchema, String schemaName, int columnId) {
      return icebergSchema(eventSchema, schemaName, columnId, false);
    }

    private List<Types.NestedField> icebergSchema(JsonNode eventSchema, String schemaName, int columnId, boolean addSourceTsField) {
      List<Types.NestedField> schemaColumns = new ArrayList<>();
      String schemaType = eventSchema.get("type").textValue();
      LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
      for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
        columnId++;
        String fieldName = jsonSchemaFieldNode.get("field").textValue();
        String fieldType = jsonSchemaFieldNode.get("type").textValue();
        LOGGER.debug("Processing Field: [{}] {}.{}::{}", columnId, schemaName, fieldName, fieldType);
        switch (fieldType) {
          case "array":
            JsonNode items = jsonSchemaFieldNode.get("items");
            if (items != null && items.has("type")) {
              String listItemType = items.get("type").textValue();

              if (listItemType.equals("struct") || listItemType.equals("array") || listItemType.equals("map")) {
                throw new RuntimeException("Complex nested array types are not supported," + " array[" + listItemType + "], field " + fieldName);
              }

              Type.PrimitiveType item = icebergFieldType(listItemType);
              schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.ListType.ofOptional(++columnId, item)));
            } else {
              throw new RuntimeException("Unexpected Array type for field " + fieldName);
            }
            break;
          case "map":
            throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
            //break;
          case "struct":
            // create it as struct, nested type
            List<Types.NestedField> subSchema = icebergSchema(jsonSchemaFieldNode, fieldName, columnId);
            schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StructType.of(subSchema)));
            columnId += subSchema.size();
            break;
          default: //primitive types
            schemaColumns.add(Types.NestedField.optional(columnId, fieldName, icebergFieldType(fieldType)));
            break;
        }
      }

      if (addSourceTsField) {
        columnId++;
        schemaColumns.add(Types.NestedField.optional(columnId, "__source_ts", Types.TimestampType.withZone()));
      }
      return schemaColumns;
    }

  }

}
