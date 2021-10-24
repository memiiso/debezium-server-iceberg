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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static List<Types.NestedField> getIcebergSchema(JsonNode eventSchema) {
    LOGGER.debug(eventSchema.toString());
    return getIcebergSchema(eventSchema, "", 0);
  }

  public static List<Types.NestedField> getIcebergSchema(JsonNode eventSchema, String schemaName, int columnId) {
    List<Types.NestedField> schemaColumns = new ArrayList<>();
    String schemaType = eventSchema.get("type").textValue();
    LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
    for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
      columnId++;
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      String fieldType = jsonSchemaFieldNode.get("type").textValue();
      LOGGER.debug("Processing Field: [{}] {}.{}::{}", columnId, schemaName, fieldName, fieldType);
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32": // int 4 bytes
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.IntegerType.get()));
          break;
        case "int64": // long 8 bytes
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.LongType.get()));
          break;
        case "float8":
        case "float16":
        case "float32": // float is represented in 32 bits,
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.FloatType.get()));
          break;
        case "float64": // double is represented in 64 bits
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.DoubleType.get()));
          break;
        case "boolean":
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.BooleanType.get()));
          break;
        case "string":
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          break;
        case "bytes":
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.BinaryType.get()));
          break;
        case "array":
          throw new RuntimeException("'" + fieldName + "' has Array type, Array type not supported!");
          //schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.ListType.ofOptional()));
          //break;
        case "map":
          throw new RuntimeException("'" + fieldName + "' has Map type, Map type not supported!");
          //schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          //break;
        case "struct":
          // create it as struct, nested type
          List<Types.NestedField> subSchema = IcebergUtil.getIcebergSchema(jsonSchemaFieldNode, fieldName, columnId);
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StructType.of(subSchema)));
          columnId += subSchema.size();
          break;
        default:
          // default to String type
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          break;
      }
    }
    return schemaColumns;
  }

  public static boolean hasSchema(JsonNode jsonNode) {
    return jsonNode != null
        && jsonNode.has("schema")
        && jsonNode.get("schema").has("fields")
        && jsonNode.get("schema").get("fields").isArray();
  }

  public static GenericRecord getIcebergRecord(Schema schema, JsonNode data) {
    return IcebergUtil.getIcebergRecord(schema.asStruct(), data);
  }

  public static GenericRecord getIcebergRecord(Types.StructType tableFields, JsonNode data) {
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

  static Object jsonToGenericRecordVal(Types.NestedField field,
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
        val = node.asText(null);
        break;
      case BINARY:
        try {
          val = node.isNull() ? null : ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          LOGGER.error("Failed converting '" + field.name() + "' binary value to iceberg record", e);
          throw new RuntimeException("Failed Processing Event!", e);
        }
        break;
      case LIST:
        val = jsonObjectMapper.convertValue(node, List.class);
        break;
      case MAP:
        val = jsonObjectMapper.convertValue(node, Map.class);
        break;
      case STRUCT:
        // create it as struct, nested type
        // recursive call to get nested data/record
        val = getIcebergRecord(field.type().asStructType(), node);
        break;
      default:
        // default to String type
        val = node.asText(null);
        break;
    }

    return val;
  }

  public static Map<String, String> getConfigSubset(Config config, String prefix) {
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getValue(propName, String.class));
      }
    }

    return ret;
  }


}
