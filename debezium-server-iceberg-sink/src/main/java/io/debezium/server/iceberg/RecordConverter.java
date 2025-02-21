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
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.debezium.server.iceberg.IcebergChangeConsumer.keyDeserializer;
import static io.debezium.server.iceberg.IcebergChangeConsumer.valDeserializer;

/**
 * Converts iceberg json event to Iceberg GenericRecord. Extracts event schema and key fields. Converts event schema to Iceberg Schema.
 *
 * @author Ismail Simsek
 */
public class RecordConverter {

  protected static final ObjectMapper mapper = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(RecordConverter.class);
  public static final List<String> TS_MS_FIELDS = List.of("__ts_ms", "__source_ts_ms");
  static final boolean eventsAreUnwrapped = IcebergUtil.configIncludesUnwrapSmt();
  protected final String destination;
  protected final byte[] valueData;
  protected final byte[] keyData;
  private JsonNode value;
  private JsonNode key;

  public RecordConverter(String destination, byte[] valueData, byte[] keyData) {
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

  public Long cdcSourceTsMsValue(String cdcSourceTsMsField) {

    final JsonNode element = value().get(cdcSourceTsMsField);
    if (element == null) {
      throw new DebeziumException("Field '" + cdcSourceTsMsField + "' not found in JSON object: " + value());
    }

    try {
      return element.asLong();
    } catch (NumberFormatException e) {
      throw new DebeziumException("Error converting field '" + cdcSourceTsMsField + "' value '" + element + "' to Long: " + e.getMessage(), e);
    }
  }

  public Operation cdcOpValue(String cdcOpField) {
    if (!value().has(cdcOpField)) {
      throw new DebeziumException("The value for field `" + cdcOpField + "` is missing. " +
          "This field is required when updating or deleting data, when running in upsert mode."
      );
    }

    final String opFieldValue = value().get(cdcOpField).asText("c");

    return switch (opFieldValue) {
      case "u" -> Operation.UPDATE;
      case "d" -> Operation.DELETE;
      case "r" -> Operation.READ;
      case "c" -> Operation.INSERT;
      case "i" -> Operation.INSERT;
      default ->
          throw new DebeziumException("Unexpected `" + cdcOpField + "=" + opFieldValue + "` operation value received, expecting one of ['u','d','r','c', 'i']");
    };
  }

  public SchemaConverter schemaConverter() {
    try {
      return new SchemaConverter(mapper.readTree(valueData).get("schema"), keyData == null ? null : mapper.readTree(keyData).get("schema"));
    } catch (IOException e) {
      throw new DebeziumException("Failed to get event schema", e);
    }
  }


  /**
   * Checks if the current message represents a schema change event.
   * Schema change events are identified by the presence of "ddl", "databaseName", and "tableChanges" fields.
   *
   * @return True if it's a schema change event, false otherwise.
   */
  private boolean isSchemaChangeEvent() {
    return value().has("ddl") && value().has("databaseName") && value().has("tableChanges");
  }


  /**
   * Converts the Kafka Connect schema to an Iceberg schema.
   *
   * @param createIdentifierFields Whether to include identifier fields in the Iceberg schema.
   *                               Identifier fields are typically used for primary keys and are
   *                               required for upsert/merge operations.  They should be *excluded*
   *                               for schema change topic messages to ensure append-only mode.
   * @return The Iceberg schema.
   */
  public Schema icebergSchema(boolean createIdentifierFields) {
    // Check if the message is a schema change event (DDL statement).
    // Schema change events are identified by the presence of "ddl", "databaseName", and "tableChanges" fields.
    // "schema change topic" https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-schema-change-topic
    if (isSchemaChangeEvent()) {
      LOGGER.warn("Schema change topic detected. Creating Iceberg schema without identifier fields for append-only mode.");
      return schemaConverter().icebergSchema(false); // Force no identifier fields for schema changes
    }

    return schemaConverter().icebergSchema(createIdentifierFields);
  }

  public String destination() {
    return destination;
  }

  public RecordWrapper convertAsAppend(Schema schema) {
    GenericRecord row = convert(schema.asStruct(), value());
    return new RecordWrapper(row, Operation.INSERT);
  }

  public RecordWrapper convert(Schema schema, String cdcOpField) {
    GenericRecord row = convert(schema.asStruct(), value());
    Operation op = cdcOpValue(cdcOpField);
    return new RecordWrapper(row, op);
  }

  private static GenericRecord convert(Types.StructType tableFields, JsonNode data) {
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
            mapVal.put(f.getKey(), convert(valType.asStructType(), f.getValue()));
          } else {
            mapVal.put(f.getKey(), f.getValue());
          }
        });
        val = mapVal;
        break;
      case STRUCT:
        // create it as struct, nested type
        // recursive call to get nested data/record
        val = convert(field.type().asStructType(), node);
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
