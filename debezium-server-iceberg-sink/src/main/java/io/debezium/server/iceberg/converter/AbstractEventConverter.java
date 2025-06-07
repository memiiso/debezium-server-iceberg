/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTime;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;

import static io.debezium.jdbc.TemporalPrecisionMode.ISOSTRING;


public class AbstractEventConverter {
  protected final GlobalConfig config;
  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventConverter.class);

  public AbstractEventConverter(GlobalConfig config) {
    this.config = config;
  }

  protected BigDecimal convertDecimal(Object value, Types.DecimalType decimalType, String logicalTypeName) {
    BigDecimal bd;
    if (value instanceof BigDecimal) {
      bd = (BigDecimal) value;
    } else if (value instanceof Number) {
      // Handle potential precision issues if converting from double/float
      bd = new BigDecimal(value.toString());
    } else if (value instanceof String) {
      bd = new BigDecimal((String) value);
    } else if (value instanceof byte[]) { // Debezium precise decimal handling
      bd = new BigDecimal(new BigInteger((byte[]) value), decimalType.scale());
    } else if (value instanceof ByteBuffer) { // Handle ByteBuffer as well
      bd = new BigDecimal(new BigInteger(ByteBuffers.toByteArray((ByteBuffer) value)), decimalType.scale());
    } else {
      throw new IllegalArgumentException("Cannot convert to BigDecimal: " + value.getClass().getName());
    }
    // Ensure the scale matches the Iceberg type definition
    return bd.setScale(decimalType.scale(), RoundingMode.HALF_UP);
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalDateTime convertLocalDateTime(Object value, String logicalTypeName) {
    return switch (logicalTypeName) {
      case ZonedTimestamp.SCHEMA_NAME -> LocalDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
      case IsoTimestamp.SCHEMA_NAME -> LocalDateTime.parse((String) value, IsoTimestamp.FORMATTER);
      case MicroTimestamp.SCHEMA_NAME -> DateTimeUtil.timestampFromMicros(((Number) value).longValue());
      case NanoTimestamp.SCHEMA_NAME -> DateTimeUtil.timestampFromNanos(((Number) value).longValue());
      case Timestamp.SCHEMA_NAME, org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME ->
          DateTimeUtils.timestampFromMillis(((Number) value).longValue());
      case null, default -> {
        if (value instanceof Long) {
          long longVal = ((Long) value);
          yield switch (config.debezium().temporalPrecisionMode()) {
            case ISOSTRING -> DateTimeUtil.timestampFromMicros(longVal);
            case NANOSECONDS -> DateTimeUtil.timestampFromNanos(longVal);
            case CONNECT -> DateTimeUtils.timestampFromMillis(longVal);
            case null, default -> DateTimeUtil.timestampFromMicros(longVal);
          };
        } else if (value instanceof String) {
          if (config.debezium().temporalPrecisionMode() == ISOSTRING) {
            yield LocalDateTime.parse((String) value, IsoTimestamp.FORMATTER);
          }
          try {
            yield LocalDateTime.parse((String) value);
          } catch (DateTimeParseException e) {
            // Attempt fallback parsing if needed, e.g., with offset and convert
            try {
              OffsetDateTime odt = OffsetDateTime.parse((String) value);
              yield odt.toLocalDateTime();
            } catch (DateTimeParseException e2) {
              throw new IllegalArgumentException("Cannot parse timestamp string: " + value, e);
            }
          }
        } else if (value instanceof LocalDateTime) {
          yield (LocalDateTime) value;
        } else if (value instanceof Date) { // Connect Timestamp (millis)
          yield DateTimeUtil.timestampFromMicros(((Date) value).getTime() * 1000);
        } else if (value instanceof OffsetDateTime) { // Handle OffsetDateTime by converting
          yield ((OffsetDateTime) value).toLocalDateTime();
        }

        throw new IllegalArgumentException("Cannot convert to timestamp (LocalDateTime): " + value.getClass().getName());
      }
    };
  }

  @SuppressWarnings("JavaUtilDate")
  protected OffsetDateTime convertOffsetDateTime(Object value, String logicalTypeName) {

    return switch (logicalTypeName) {
      case ZonedTimestamp.SCHEMA_NAME -> OffsetDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
      case IsoTimestamp.SCHEMA_NAME -> OffsetDateTime.parse((String) value, IsoTimestamp.FORMATTER);
      case NanoTimestamp.SCHEMA_NAME -> DateTimeUtils.timestamptzFromNanos((Long) value);
      case MicroTimestamp.SCHEMA_NAME -> DateTimeUtil.timestamptzFromMicros((Long) value);
      case Timestamp.SCHEMA_NAME, org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME ->
          DateTimeUtils.timestamptzFromMillis((Long) value);
      case null, default -> {
        if (value instanceof Long) {
          yield switch (config.debezium().temporalPrecisionMode()) {
            case MICROSECONDS -> DateTimeUtil.timestamptzFromMicros((Long) value);
            case NANOSECONDS -> DateTimeUtils.timestamptzFromNanos((Long) value);
            case CONNECT -> DateTimeUtils.timestamptzFromMillis((Long) value);
            case null, default ->DateTimeUtils.timestamptzFromMillis(((Long) value));
          };
        } else if (value instanceof String) { // Debezium ZonedTimestamp
          try {
            yield OffsetDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
          } catch (DateTimeParseException e) {
            // ignore and continue trying
          }
          try {
            LocalDateTime ldt = LocalDateTime.parse((String) value);
            yield ldt.atOffset(ZoneOffset.UTC);
          } catch (DateTimeParseException e2) {
            throw new IllegalArgumentException("Cannot parse timestamp string: " + value);
          }
        } else if (value instanceof OffsetDateTime) {
          yield (OffsetDateTime) value;
        } else if (value instanceof Date) {
          yield DateTimeUtils.timestamptzFromMillis(((Date) value).getTime());
        } else if (value instanceof LocalDateTime) { // Handle LocalDateTime by assuming UTC
          yield ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
        }
        throw new IllegalArgumentException("Cannot convert to timestamptz (OffsetDateTime): " + value.getClass().getName());
      }
    };

  }

  // Conversion methods adapted from Iceberg's RecordConverter
  protected int convertInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
  }

  protected long convertLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
  }

  protected float convertFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
  }

  protected double convertDouble(Object value, String logicalTypeName) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
  }

  protected boolean convertBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
  }

  protected String convertString(Object value) {
    // Handle various connect types that might map to String
    if (value instanceof CharSequence) {
      return value.toString();
    } else if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    } else if (value instanceof byte[]) { // Handle bytes specifically if needed (e.g., Base64 or UTF8)
      return new String((byte[]) value, StandardCharsets.UTF_8); // Assuming UTF-8
    } else if (value instanceof ByteBuffer) {
      return StandardCharsets.UTF_8.decode((ByteBuffer) value).toString();
    }
    // Add other conversions if necessary (e.g., Date to ISO string)
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected Object convertUUID(Object value) {
    if (value instanceof String) {
      return UUID.fromString((String) value);
    } else if (value instanceof UUID) {
      return value;
    }

    throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
  }

  protected ByteBuffer convertBinary(Object value, String logicalTypeName) {
    if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof String) { // Assuming Base64 encoded string for binary
      try {
        return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Cannot convert Base64 string to binary: invalid format", e);
      }
    }

    throw new IllegalArgumentException("Cannot convert to binary (ByteBuffer): " + value.getClass().getName());
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalDate convertDateValue(Object value, String logicalTypeName) {
    return switch (logicalTypeName) {
      case io.debezium.time.Date.SCHEMA_NAME -> DateTimeUtil.dateFromDays((Integer) value);
      case IsoDate.SCHEMA_NAME -> LocalDate.parse((String) value, IsoDate.FORMATTER);
      case org.apache.kafka.connect.data.Date.LOGICAL_NAME -> DateTimeUtils.toLocalDateFromDate((Date) value);
      case null, default -> {
        if (value instanceof Integer) { // Correct handling for Debezium Date
          yield DateTimeUtil.dateFromDays((Integer) value);
        } else if (value instanceof Number) { // Fallback for other numeric types
          yield DateTimeUtil.dateFromDays(((Number) value).intValue());
        } else if (value instanceof LocalDate) {
          yield ((LocalDate) value);
        } else if (value instanceof Date) {
          yield DateTimeUtils.toLocalDateFromDate((Date) value);
        } else if (value instanceof String) { // Handle ISO date string
          yield LocalDate.parse((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to date (LocalDate): " + value.getClass().getName());
      }
    };
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalTime convertTimeValue(Object value, String logicalTypeName) {
    return switch (logicalTypeName) {
      case org.apache.kafka.connect.data.Time.LOGICAL_NAME, Time.SCHEMA_NAME ->
          DateTimeUtils.toLocalTimeFromDurationMilliseconds(((Number) value).longValue());
      case MicroTime.SCHEMA_NAME -> DateTimeUtils.toLocalTimeFromDurationMicroseconds(((Number) value).longValue());
      case NanoTime.SCHEMA_NAME -> DateTimeUtils.toLocalTimeFromDurationNanoseconds(((Number) value).longValue());
      case IsoTime.SCHEMA_NAME -> LocalTime.parse((String) value, IsoTime.FORMATTER);
      case ZonedTime.SCHEMA_NAME -> LocalTime.parse((String) value, ZonedTime.FORMATTER);
      case null, default -> {
        if (value instanceof Number) {
          // its microseconds ?
          yield DateTimeUtils.toLocalTimeFromDurationMicroseconds(((Number) value).longValue());
        } else if (value instanceof String) {
          try {
            yield LocalTime.parse((String) value);
          } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Cannot parse time string: " + value, e);
          }
        } else if (value instanceof LocalTime) {
          yield (LocalTime) value;
        } else if (value instanceof Date) {
          yield DateTimeUtils.toLocalTimeFromDurationMilliseconds(((Date) value).getTime());
        }
        throw new ConnectException("Cannot convert time (LocalTime): " + value);
      }
    };
  }

  protected Temporal convertTimestampValue(Object value, Types.TimestampType timestampType, String icebergFieldName, String logicalTypeName) {
    if (DebeziumConfig.TS_MS_FIELDS.contains(icebergFieldName)) {
      return DateTimeUtils.timestamptzFromMillis((Long) value);
    } else if (timestampType.shouldAdjustToUTC()) {
      return convertOffsetDateTime(value, logicalTypeName); // Timestamp with timezone
    } else {
      return convertLocalDateTime(value, logicalTypeName); // Timestamp without timezone
    }
  }

}