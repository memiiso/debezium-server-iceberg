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
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTime;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.debezium.server.iceberg.IcebergChangeConsumer.keyDeserializer;

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
  private final JsonNode value;
  private final JsonNode key;
  private final IcebergConsumerConfig config;

  public RecordConverter(String destination, byte[] valueData, byte[] keyData, IcebergConsumerConfig config) {
    this.destination = destination;
    this.valueData = valueData;
    this.keyData = keyData;
    this.config = config;
    this.key = keyDeserializer.deserialize(destination, keyData);
    this.value = keyDeserializer.deserialize(destination, valueData);
  }

  public JsonNode key() {
    return key;
  }

  public JsonNode value() {
    return value;
  }

  public Long cdcSourceTsMsValue() {

    final JsonNode element = value().get(config.iceberg().cdcSourceTsMsField());
    if (element == null) {
      throw new DebeziumException("Field '" + config.iceberg().cdcSourceTsMsField() + "' not found in JSON object: " + value());
    }

    try {
      return element.asLong();
    } catch (NumberFormatException e) {
      throw new DebeziumException("Error converting field '" + config.iceberg().cdcSourceTsMsField() + "' value '" + element + "' to Long: " + e.getMessage(), e);
    }
  }

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

  public SchemaConverter schemaConverter() {
    try {
      return new SchemaConverter(
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
  public boolean isSchemaChangeEvent() {
    return value().has("ddl") && value().has("databaseName") && value().has("tableChanges");
  }


  /**
   * Converts the Kafka Connect schema to an Iceberg schema.
   *
   * @return The Iceberg schema.
   */
  public Schema icebergSchema() {
    return schemaConverter().icebergSchema();
  }

  public String destination() {
    return destination;
  }

  public RecordWrapper convertAsAppend(Schema schema) {
    GenericRecord row = convert(schema.asStruct(), value());
    return new RecordWrapper(row, Operation.INSERT);
  }

  public RecordWrapper convert(Schema schema) {
    GenericRecord row = convert(schema.asStruct(), value());
    Operation op = cdcOpValue();
    return new RecordWrapper(row, op);
  }

  public static LocalDateTime timestampFromMillis(long millisFromEpoch) {
    return ChronoUnit.MILLIS.addTo(DateTimeUtil.EPOCH, millisFromEpoch).toLocalDateTime();
  }

  public static OffsetDateTime timestamptzFromNanos(long nanosFromEpoch) {
    return ChronoUnit.NANOS.addTo(DateTimeUtil.EPOCH, nanosFromEpoch);
  }

  public static OffsetDateTime timestamptzFromMillis(long millisFromEpoch) {
    return ChronoUnit.MILLIS.addTo(DateTimeUtil.EPOCH, millisFromEpoch);
  }

  private GenericRecord convert(Types.StructType tableFields, JsonNode data) {
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
        if (node.isNumber() && TS_MS_FIELDS.contains(field.name())) {
          return timestamptzFromMillis(node.asLong());
        }
        boolean isTsWithZone = ((Types.TimestampType) field.type()).shouldAdjustToUTC();
        if (isTsWithZone) {
          return convertOffsetDateTimeValue(field, node, config.debezium().temporalPrecisionMode());
        }
        return convertLocalDateTimeValue(field, node, config.debezium().temporalPrecisionMode());
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

  public LocalDateTime convertLocalDateTimeValue(Types.NestedField field, JsonNode node, TemporalPrecisionMode temporalPrecisionMode) {

    final String eexMessage = "Failed to convert timestamp value, field: " + field.name() + " value: " + node + " temporalPrecisionMode: " + temporalPrecisionMode;
    if (node.isNumber()) {
      return switch (temporalPrecisionMode) {
        case MICROSECONDS -> DateTimeUtil.timestampFromMicros(node.asLong());
        case NANOSECONDS -> DateTimeUtil.timestampFromNanos(node.asLong());
        case CONNECT -> timestampFromMillis(node.asLong());
        default -> throw new RuntimeException(eexMessage);
      };
    }

    if (node.isTextual()) {
      return switch (temporalPrecisionMode) {
        case ISOSTRING -> LocalDateTime.parse(node.asText(), IsoTimestamp.FORMATTER);
        default -> throw new RuntimeException(eexMessage);
      };
    }
    throw new RuntimeException(eexMessage);
  }

  private OffsetDateTime convertOffsetDateTimeValue(Types.NestedField field, JsonNode node, TemporalPrecisionMode temporalPrecisionMode) {
    final String eexMessage = "Failed to convert timestamp value, field: " + field.name() + " value: " + node + " temporalPrecisionMode: " + temporalPrecisionMode;

    if (node.isNumber()) {
      // non Timezone
      return switch (temporalPrecisionMode) {
        case MICROSECONDS -> DateTimeUtil.timestamptzFromMicros(node.asLong());
        case NANOSECONDS -> timestamptzFromNanos(node.asLong());
        case CONNECT -> timestamptzFromMillis(node.asLong());
        default -> throw new RuntimeException(eexMessage);
      };
    }

    if (node.isTextual()) {
      return switch (temporalPrecisionMode) {
        default -> OffsetDateTime.parse(node.asText(), ZonedTimestamp.FORMATTER);
      };
    }

    throw new RuntimeException(eexMessage);
  }

}
