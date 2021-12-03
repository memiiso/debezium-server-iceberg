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
  protected final JsonNode valueSchema;
  protected final JsonNode keySchema;

  public IcebergChangeEvent(String destination,
                            JsonNode value,
                            JsonNode key,
                            JsonNode valueSchema,
                            JsonNode keySchema) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
  }

  public JsonNode key() {
    return key;
  }

  public String destinationTable() {
    return destination.replace(".", "_");
  }

  public GenericRecord getIcebergRecord(Schema schema) {
    return getIcebergRecord(schema.asStruct(), value);
  }

  public String schemaHashCode() {
    return valueSchema.hashCode() + "-" + keySchema.hashCode();
  }

  public Schema getSchema() {

    if (this.valueSchema == null) {
      throw new RuntimeException("Failed to get event schema, event value is null, destination:" + this.destination);
    }

    final List<Types.NestedField> tableColumns = getValueFields();

    if (tableColumns.isEmpty()) {
      throw new RuntimeException("Failed to get schema destination:" + this.destination);
    }

    final List<Types.NestedField> keyColumns = getKeyFields();
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

  private GenericRecord getIcebergRecord(Types.StructType tableFields, JsonNode data) {
    Map<String, Object> mappedResult = new HashMap<>();
    LOGGER.debug("Processing nested field:{}", tableFields);

    for (Types.NestedField field : tableFields.fields()) {
      // Set value to null if json event don't have the field
      if (data == null || !data.has(field.name()) || data.get(field.name()) == null) {
        mappedResult.put(field.name(), null);
        continue;
      }
      // get the value of the field from json event, map it to iceberg value
      mappedResult.put(field.name(), jsonToGenericRecordVal(field, data.get(field.name())));
    }

    return GenericRecord.create(tableFields).copy(mappedResult);
  }

  //getIcebergFieldsFromEventSchema
  private List<Types.NestedField> getKeyFields() {
    if (keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray()) {
      LOGGER.debug(keySchema.toString());
      return getIcebergSchema(keySchema, "", 0);
    }
    LOGGER.trace("Key schema not found!");
    return new ArrayList<>();
  }

  private List<Types.NestedField> getValueFields() {
    if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
      LOGGER.debug(valueSchema.toString());
      return getIcebergSchema(valueSchema, "", 0);
    }
    LOGGER.trace("Event schema not found!");
    return new ArrayList<>();
  }

  private Type.PrimitiveType getIcebergFieldType(String fieldType) {
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
      case "bytes":
        return Types.BinaryType.get();
      default:
        // default to String type
        return Types.StringType.get();
      //throw new RuntimeException("'" + fieldName + "' has "+fieldType+" type, "+fieldType+" not supported!");
    }
  }

  private List<Types.NestedField> getIcebergSchema(JsonNode eventSchema, String schemaName, int columnId) {
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
              throw new RuntimeException("Complex Array types are not supported array[" + listItemType + "], field " + fieldName);
            }
            Type.PrimitiveType item = getIcebergFieldType(listItemType);
            schemaColumns.add(Types.NestedField.optional(
                columnId, fieldName, Types.ListType.ofOptional(++columnId, item)));
            //throw new RuntimeException("'" + fieldName + "' has Array type, Array type not supported!");
          } else {
            throw new RuntimeException("Unexpected Array type for field " + fieldName);
          }
          break;
        case "map":
          throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
          //schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          //break;
        case "struct":
          // create it as struct, nested type
          List<Types.NestedField> subSchema = getIcebergSchema(jsonSchemaFieldNode, fieldName, columnId);
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StructType.of(subSchema)));
          columnId += subSchema.size();
          break;
        default: //primitive types
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, getIcebergFieldType(fieldType)));
          break;
      }
    }
    return schemaColumns;
  }

  private Object jsonToGenericRecordVal(Types.NestedField field,
                                        JsonNode node) {
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
      case BINARY:
        try {
          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          LOGGER.error("Failed to convert binary value to iceberg value, field:" + field.name(), e);
          throw new RuntimeException("Failed Processing Event!", e);
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
        val = getIcebergRecord(field.type().asStructType(), node);
        break;
      default:
        // default to String type
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        val = node.isValueNode() ? node.asText(null) : node.toString();
        break;
    }

    return val;
  }

}
