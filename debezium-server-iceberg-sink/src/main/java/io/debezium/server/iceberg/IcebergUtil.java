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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static List<Types.NestedField> getIcebergSchema(JsonNode eventSchema) {
    LOGGER.debug(eventSchema.toString());
    return getIcebergSchema(eventSchema, "", -1);
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
          throw new RuntimeException("Event schema containing nested data '" + fieldName + "' cannot process nested" +
              " data!");
//          //recursive call
//          Schema subSchema = SchemaUtil.getIcebergSchema(jsonSchemaFieldNode, fieldName, ++columnId);
//          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StructType.of(subSchema.columns())));
//          columnId += subSchema.columns().size();
//          break;
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

  public static GenericRecord getIcebergRecord(Schema schema, JsonNode data) throws InterruptedException {
    return IcebergUtil.getIcebergRecord(schema.asStruct(), data);
  }

  public static GenericRecord getIcebergRecord(Types.StructType nestedField, JsonNode data) throws InterruptedException {
    Map<String, Object> mappedResult = new HashMap<>();
    LOGGER.debug("Processing nested field : " + nestedField);

    for (Types.NestedField field : nestedField.fields()) {
      if (data == null || !data.has(field.name()) || data.get(field.name()) == null) {
        mappedResult.put(field.name(), null);
        continue;
      }
      jsonToGenericRecord(mappedResult, field, data.get(field.name()));
    }
    return GenericRecord.create(nestedField).copy(mappedResult);
  }

  private static void jsonToGenericRecord(Map<String, Object> mappedResult, Types.NestedField field,
                                          JsonNode node) throws InterruptedException {
    LOGGER.debug("Processing Field:" + field.name() + " Type:" + field.type());

    switch (field.type().typeId()) {
      case INTEGER: // int 4 bytes
        mappedResult.put(field.name(), node.isNull() ? null : node.asInt());
        break;
      case LONG: // long 8 bytes
        mappedResult.put(field.name(), node.isNull() ? null : node.asLong());
        break;
      case FLOAT: // float is represented in 32 bits,
        mappedResult.put(field.name(), node.isNull() ? null : node.floatValue());
        break;
      case DOUBLE: // double is represented in 64 bits
        mappedResult.put(field.name(), node.isNull() ? null : node.asDouble());
        break;
      case BOOLEAN:
        mappedResult.put(field.name(), node.isNull() ? null : node.asBoolean());
        break;
      case STRING:
        mappedResult.put(field.name(), node.asText(null));
        break;
      case BINARY:
        try {
          mappedResult.put(field.name(), node.isNull() ? null : ByteBuffer.wrap(node.binaryValue()));
        } catch (IOException e) {
          LOGGER.error("Failed converting '" + field.name() + "' binary value to iceberg record", e);
          throw new InterruptedException("Failed Processing Event!" + e.getMessage());
        }
        break;
      case LIST:
        mappedResult.put(field.name(), jsonObjectMapper.convertValue(node, List.class));
        break;
      case MAP:
        mappedResult.put(field.name(), jsonObjectMapper.convertValue(node, Map.class));
        break;
      case STRUCT:
        throw new RuntimeException("Cannot process recursive records!");
        // Disabled because cannot recursively process StructType/NestedField
//        // recursive call to get nested data/record
//        Types.StructType nestedField = Types.StructType.of(field);
//        GenericRecord r = getIcebergRecord(schema, nestedField, node.get(field.name()));
//        mappedResult.put(field.name(), r);
//        // throw new RuntimeException("Cannot process recursive record!");
//        break;
      default:
        // default to String type
        mappedResult.put(field.name(), node.asText(null));
        break;
    }

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

  public static Map<String, String> getConfigurationAsMap(Configuration conf) {
    Map<String, String> config = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : conf) {
      config.put(entry.getKey(), entry.getValue());
    }
    return config;
  }


}
