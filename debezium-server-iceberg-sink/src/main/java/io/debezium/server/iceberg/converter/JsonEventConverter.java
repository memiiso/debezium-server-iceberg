/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTime;
import io.debezium.time.ZonedTime;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Converts iceberg json event to Iceberg GenericRecord. Extracts event schema and key fields. Converts event schema to Iceberg Schema.
 *
 * @author Ismail Simsek
 */
public class JsonEventConverter extends AbstractEventConverter implements EventConverter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(JsonEventConverter.class);
  protected final String destination;
  protected final byte[] valueData;
  protected final byte[] keyData;
  private final JsonNode value;
  private final JsonNode key;

  public JsonEventConverter(EmbeddedEngineChangeEvent e, GlobalConfig config) {
    this(e.destination(), e.value(), e.key(), config);
  }

  // Testing only
  public JsonEventConverter(String destination, Object valueData, Object keyData, GlobalConfig config) {
    super(config);
    this.destination = destination;
    this.valueData = getBytes(valueData);
    this.keyData = getBytes(keyData);
    this.key = keyDeserializer.deserialize(destination, this.keyData);
    this.value = valDeserializer.deserialize(destination, this.valueData);
  }

  protected byte[] getBytes(Object object) {
    if (object instanceof byte[]) {
      return (byte[]) object;
    } else if (object instanceof String) {
      return ((String) object).getBytes(StandardCharsets.UTF_8);
    } else if (object == null) {
      return null;
    } else {
      String type = object == null ? "null" : object.getClass().getName();
      throw new DebeziumException("Unexpected data type '" + type + "'");
    }
  }

  @Override
  public JsonNode key() {
    return key;
  }

  @Override
  public boolean hasKeyData() {
    return this.key() != null && !this.key().isNull();
  }

  @Override
  public JsonNode value() {
    return value;
  }

  @Override
  public Long cdcSourceTsValue() {

    final JsonNode element = value().get(config.iceberg().cdcSourceTsField());
    if (element == null) {
      throw new DebeziumException("Field '" + config.iceberg().cdcSourceTsField() + "' not found in JSON object: " + value());
    }

    try {
      return element.asLong();
    } catch (NumberFormatException e) {
      throw new DebeziumException("Error converting field '" + config.iceberg().cdcSourceTsField() + "' value '" + element + "' to Long: " + e.getMessage(), e);
    }
  }

  @Override
  public Operation cdcOpValue() {
    if (!value().has(config.iceberg().cdcOpField())) {
      throw new DebeziumException("The value for field `" + config.iceberg().cdcOpField() + "` is missing. " +
          "This field is required when updating or deleting data, when running in upsert mode."
      );
    }

    final String opFieldValue = value().get(config.iceberg().cdcOpField()).asText("c");

    return switch (opFieldValue) {
      case "u" -> Operation.UPDATE;
      case "d" -> Operation.DELETE;
      case "r" -> Operation.READ;
      case "c" -> Operation.INSERT;
      case "i" -> Operation.INSERT;
      default ->
          throw new DebeziumException("Unexpected `" + config.iceberg().cdcOpField() + "=" + opFieldValue + "` operation value received, expecting one of ['u','d','r','c', 'i']");
    };
  }

  @Override
  public JsonSchemaConverter schemaConverter() {
    try {
      return new JsonSchemaConverter(
          mapper.readTree(valueData).get("schema"),
          keyData == null ? null : mapper.readTree(keyData).get("schema"),
          config
      );
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
  @Override
  public boolean isSchemaChangeEvent() {
    return value().has("ddl") && value().has("databaseName") && value().has("tableChanges");
  }


  /**
   * Converts the Kafka Connect schema to an Iceberg schema.
   *
   * @return The Iceberg schema.
   */
  @Override
  public Schema icebergSchema() {
    return schemaConverter().icebergSchema();
  }

  @Override
  public SortOrder sortOrder(Schema schema) {
    return schemaConverter().sortOrder(schema);
  }

  @Override
  public String destination() {
    return destination;
  }

  @Override
  public RecordWrapper convertAsAppend(Schema schema) {
    if (value == null) {
      throw new DebeziumException("Cannot convert null value Struct to Iceberg Record for APPEND operation.");
    }
    GenericRecord row = convert(schema.asStruct(), value());
    return new RecordWrapper(row, Operation.INSERT);
  }

  @Override
  public RecordWrapper convert(Schema schema) {
    GenericRecord row = convert(schema.asStruct(), value());
    Operation op = cdcOpValue();
    return new RecordWrapper(row, op);
  }


  private GenericRecord convert(Types.StructType tableFields, JsonNode data) {
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

  private Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
    LOGGER.debug("Processing Field:{} Type:{}", field.name(), field.type());

    if (node.isNull()) {
      return null;
    }

    switch (field.type().typeId()) {
      case INTEGER: // int 4 bytes
        return node.asInt();
      case LONG: // long 8 bytes
        return node.asLong();
      case FLOAT: // float is represented in 32 bits,
        return node.floatValue();
      case DOUBLE: // double is represented in 64 bits
        return node.asDouble();
      case DECIMAL: {
        BigDecimal decimalVal = null;
        try {
          int scale = ((Types.DecimalType) field.type()).scale();
          decimalVal = new BigDecimal(new BigInteger(node.binaryValue()), scale);
        } catch (IOException e) {
          throw new RuntimeException("Failed to convert decimal value to iceberg value, field: " + field.name(), e);
        }
        return decimalVal;
      }
      case BOOLEAN:
        return node.asBoolean();
      case UUID:
        if (node.isTextual()) {
          return UUID.fromString(node.textValue());
        }
        throw new RuntimeException("Failed to convert date value, field: " + field.name() + " value: " + node);
      case DATE:
        if ((node.isInt())) {
          // io.debezium.time.Date
          // org.apache.kafka.connect.data.Date
          // Represents the number of days since the epoch.
          return LocalDate.ofEpochDay(node.longValue());
        }
        if (node.isTextual()) {
          // io.debezium.time.IsoDate
          // Represents date values in UTC format, according to the ISO 8601 standard, for example, 2017-09-15Z.
          return LocalDate.parse(node.asText(), IsoDate.FORMATTER);
        }
        throw new RuntimeException("Failed to convert date value, field: " + field.name() + " value: " + node);

      case TIME:
        if (node.isTextual()) {
          return switch (config.debezium().temporalPrecisionMode()) {
            // io.debezium.time.IsoTime
            case ISOSTRING -> LocalTime.parse(node.asText(), IsoTime.FORMATTER);
            // io.debezium.time.ZonedTime
            // A string representation of a time value with timezone information,
            // Iceberg using LocalTime for time values
            default -> OffsetTime.parse(node.asText(), ZonedTime.FORMATTER).toLocalTime();
          };
        }
        if (node.isNumber()) {
          return switch (config.debezium().temporalPrecisionMode()) {
            // io.debezium.time.MicroTime
            // Represents the time value in microseconds
            case MICROSECONDS -> LocalTime.ofNanoOfDay(node.asLong() * 1000);
            // io.debezium.time.NanoTime
            // Represents the time value in nanoseconds
            case NANOSECONDS -> LocalTime.ofNanoOfDay(node.asLong());
            //org.apache.kafka.connect.data.Time
            //Represents the number of milliseconds since midnight,
            case CONNECT -> LocalTime.ofNanoOfDay(node.asLong() * 1_000_000);
            default ->
                throw new RuntimeException("Failed to convert time value, field: " + field.name() + " value: " + node);
          };
        }
        throw new RuntimeException("Failed to convert time value, field: " + field.name() + " value: " + node);
      case TIMESTAMP:
        boolean isTsWithZone = ((Types.TimestampType) field.type()).shouldAdjustToUTC();
        if (node.isNumber()) {
          if (isTsWithZone) {
            return convertOffsetDateTime(node.numberValue(), null);
          }
          if (DebeziumConfig.TS_MS_FIELDS.contains(field.name())) {
            return DateTimeUtils.timestamptzFromMillis(node.asLong());
          }
          return convertLocalDateTime(node.numberValue(), null);
        }
        if (node.isTextual()) {
          if (isTsWithZone) {
            return convertOffsetDateTime(node.asText(), null);
          }
          return convertLocalDateTime(node.asText(), null);
        }
      case BINARY:
        try {
          return ByteBuffer.wrap(node.binaryValue());
        } catch (IOException e) {
          throw new RuntimeException("Failed to convert binary value to iceberg value, field: " + field.name(), e);
        }
      case LIST:
        Types.NestedField listItemsType = field.type().asListType().fields().get(0);
        // recursive value mapping when list elements are nested type
        if (listItemsType.type().isNestedType()) {
          ArrayList<Object> listVal = new ArrayList<>();
          node.elements().forEachRemaining(element -> {
            listVal.add(jsonValToIcebergVal(field.type().asListType().fields().get(0), element));
          });
          return listVal;
        }
        return mapper.convertValue(node, ArrayList.class);
      case MAP:
        Type keyType = field.type().asMapType().keyType();
        Type valType = field.type().asMapType().valueType();
        if (keyType.isPrimitiveType() && valType.isPrimitiveType()) {
          return mapper.convertValue(node, Map.class);
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
        return mapVal;
      case VARIANT:
        JsonVariantObject val = new JsonVariantObject(node);
        return Variant.of(val.metadata, val);
      case STRUCT:
        // create it as struct, nested type
        // recursive call to get nested data/record
        return convert(field.type().asStructType(), node);
      case STRING:
      default:
        // default to String type
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        return node.isValueNode() ? node.textValue() : node.toString();
    }
  }

}
