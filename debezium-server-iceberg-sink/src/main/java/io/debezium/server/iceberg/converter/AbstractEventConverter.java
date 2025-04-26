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
import io.debezium.time.ZonedTimestamp;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;


public class AbstractEventConverter {
  protected final GlobalConfig config;
  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventConverter.class);

  public AbstractEventConverter(GlobalConfig config) {
    this.config = config;
  }

  protected static LocalDateTime timestampFromMillis(long millisFromEpoch) {
    return ChronoUnit.MILLIS.addTo(DateTimeUtil.EPOCH, millisFromEpoch).toLocalDateTime();
  }

  protected static OffsetDateTime timestamptzFromNanos(long nanosFromEpoch) {
    return ChronoUnit.NANOS.addTo(DateTimeUtil.EPOCH, nanosFromEpoch);
  }

  protected static OffsetDateTime timestamptzFromMillis(long millisFromEpoch) {
    return ChronoUnit.MILLIS.addTo(DateTimeUtil.EPOCH, millisFromEpoch);
  }

  protected BigDecimal convertDecimal(Object value, Types.DecimalType decimalType) {
    BigDecimal bd;
    if (value instanceof BigDecimal) {
      bd = (BigDecimal) value;
    } else if (value instanceof Number) {
      // Handle potential precision issues if converting from double/float
      bd = new BigDecimal(value.toString());
    } else if (value instanceof String) {
      bd = new BigDecimal((String) value);
    } else if (value instanceof byte[]) { // Debezium precise decimal handling
      bd = new BigDecimal(new java.math.BigInteger((byte[]) value), decimalType.scale());
    } else if (value instanceof ByteBuffer) { // Handle ByteBuffer as well
      bd = new BigDecimal(new java.math.BigInteger(ByteBuffers.toByteArray((ByteBuffer) value)), decimalType.scale());
    } else {
      throw new IllegalArgumentException("Cannot convert to BigDecimal: " + value.getClass().getName());
    }
    // Ensure the scale matches the Iceberg type definition
    return bd.setScale(decimalType.scale(), RoundingMode.HALF_UP);
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalDateTime convertLocalDateTime(Object value) {
    if (value instanceof Number) {
      long longVal = ((Number) value).longValue();
      return switch (config.debezium().temporalPrecisionMode()) {
        case MICROSECONDS -> DateTimeUtil.timestampFromMicros(longVal);
        case NANOSECONDS -> DateTimeUtil.timestampFromNanos(longVal);
        case CONNECT -> timestampFromMillis(longVal);
        default -> DateTimeUtil.timestampFromMicros(longVal);
      };
    }
    else if (value instanceof String) {
      return switch (config.debezium().temporalPrecisionMode()) {
        case ISOSTRING -> LocalDateTime.parse((String) value, IsoTimestamp.FORMATTER);
        default -> {
          try {
            yield LocalDateTime.parse((String) value);
          } catch (java.time.format.DateTimeParseException e) {
            // Attempt fallback parsing if needed, e.g., with offset and convert
            try {
              OffsetDateTime odt = OffsetDateTime.parse((String) value);
              yield odt.toLocalDateTime();
            } catch (java.time.format.DateTimeParseException e2) {
              throw new IllegalArgumentException("Cannot parse timestamp string: " + value, e);
            }
          }
        }
      };
    }
    else if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    }
    else if (value instanceof Date) { // Connect Timestamp (millis)
      return DateTimeUtil.timestampFromMicros(((Date) value).getTime() * 1000);
    }
    else if (value instanceof OffsetDateTime) { // Handle OffsetDateTime by converting
      return ((OffsetDateTime) value).toLocalDateTime();
    }
    throw new IllegalArgumentException("Cannot convert to timestamp (LocalDateTime): " + value.getClass().getName());
  }

  @SuppressWarnings("JavaUtilDate")
  protected OffsetDateTime convertOffsetDateTime(Object value) {
    if (value instanceof Long) {
      return switch (config.debezium().temporalPrecisionMode()) {
        case MICROSECONDS -> DateTimeUtil.timestamptzFromMicros((Long) value);
        case NANOSECONDS -> timestamptzFromNanos((Long) value);
        case CONNECT -> timestamptzFromMillis((Long) value);
        default -> DateTimeUtil.timestamptzFromMicros((Long) value);
      };
    }
    else if (value instanceof String) { // Debezium ZonedTimestamp
      try {
        return OffsetDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
      } catch (java.time.format.DateTimeParseException e) {
        // ignore and continue trying
      }
      try {
        LocalDateTime ldt = LocalDateTime.parse((String) value);
        return ldt.atOffset(ZoneOffset.UTC);
      } catch (java.time.format.DateTimeParseException e2) {
        throw new IllegalArgumentException("Cannot parse timestamp string: " + value);
      }
    }
    else if (value instanceof OffsetDateTime) {
      return (OffsetDateTime) value;
    } else if (value instanceof Date) {
      return DateTimeUtil.timestamptzFromMicros(((Date) value).getTime() * 1000);
    }
    else if (value instanceof LocalDateTime) { // Handle LocalDateTime by assuming UTC
      return ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
    }
    throw new IllegalArgumentException("Cannot convert to timestamptz (OffsetDateTime): " + value.getClass().getName());
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

  protected double convertDouble(Object value) {
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
      return new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8); // Assuming UTF-8
    } else if (value instanceof ByteBuffer) {
      return java.nio.charset.StandardCharsets.UTF_8.decode((ByteBuffer) value).toString();
    }
    // Add other conversions if necessary (e.g., Date to ISO string)
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected Object convertUUID(Object value) {
    UUID uuid;
    if (value instanceof String) {
      uuid = UUID.fromString((String) value);
    } else if (value instanceof UUID) {
      uuid = (UUID) value;
    } else {
      throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
    }
    return uuid;
  }

  protected ByteBuffer convertBinary(Object value) {
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
  protected LocalDate convertDateValue(Object value) {
    if (value instanceof Integer) { // Correct handling for Debezium Date
      return DateTimeUtil.dateFromDays((Integer) value);
    }
    else if (value instanceof Number) { // Fallback for other numeric types
      return DateTimeUtil.dateFromDays(((Number) value).intValue());
    }
    else if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    else if (value instanceof Date) {
      int days = (int) (((Date) value).getTime() / 1000 / 60 / 60 / 24);
      return DateTimeUtil.dateFromDays(days);
    }
    else if (value instanceof String) { // Handle ISO date string
      try {
        return switch (config.debezium().temporalPrecisionMode()) {
          case ISOSTRING -> LocalDate.parse((String) value, IsoDate.FORMATTER);
          default -> LocalDate.parse((String) value);
        };
      } catch (java.time.format.DateTimeParseException e) {
        throw new IllegalArgumentException("Cannot parse date string: " + value, e);
      }
    }
    throw new IllegalArgumentException("Cannot convert to date (LocalDate): " + value.getClass().getName());
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalTime convertTimeValue(Object value) {
    // TODO: Refine based on actual Debezium logical type name if available
    if (value instanceof Number) {
      long longValue = ((Number) value).longValue();
      return switch (config.debezium().temporalPrecisionMode()) {
        case MICROSECONDS -> DateTimeUtil.timeFromMicros(longValue);
        case NANOSECONDS -> LocalTime.ofNanoOfDay(longValue);
        case CONNECT -> LocalTime.ofNanoOfDay(longValue * 1_000_000L); // Millisecond
        default -> LocalTime.ofNanoOfDay(longValue * 1_000_000L); // Millisecond
      };
    } else if (value instanceof String) {
      return switch (config.debezium().temporalPrecisionMode()) {
        case ISOSTRING -> LocalTime.parse((String) value, IsoTime.FORMATTER);
        default -> {
          try {
            yield  LocalTime.parse((String) value);
          } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("Cannot parse time string: " + value, e);
          }
        }
      };
    } else if (value instanceof LocalTime) {
      return (LocalTime) value;
    } else if (value instanceof Date) {
      long millis = ((Date) value).getTime();
      return DateTimeUtil.timeFromMicros(millis * 1000);
    }
    throw new ConnectException("Cannot convert time (LocalTime): " + value);
  }

  protected Temporal convertTimestampValue(Object value, Types.TimestampType timestampType, String icebergFieldName) {
    if (DebeziumConfig.TS_MS_FIELDS.contains(icebergFieldName)) {
      return timestamptzFromMillis((Long) value);
    }
    else if (timestampType.shouldAdjustToUTC()) {
      return convertOffsetDateTime(value); // Timestamp with timezone
    } else {
      return convertLocalDateTime(value); // Timestamp without timezone
    }
  }

}