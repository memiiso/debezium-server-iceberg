/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTime;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergValueConverterTest {

  private final IcebergValueConverter converter =
      new IcebergValueConverter(ConnectIcebergConverterConfig.defaults());

  @Test
  void testPrimitivesAndBinaryAndUuid() {
    assertEquals(42, converter.convertInt(42L));
    assertEquals(42, converter.convertInt("42"));
    assertThrows(IllegalArgumentException.class, () -> converter.convertInt(Boolean.TRUE));

    assertEquals(99L, converter.convertLong(99));
    assertEquals(99L, converter.convertLong("99"));
    assertThrows(IllegalArgumentException.class, () -> converter.convertLong(Boolean.TRUE));

    assertEquals(1.5f, converter.convertFloat(1.5d));
    assertEquals(1.5f, converter.convertFloat("1.5"));
    assertEquals(2.5d, converter.convertDouble(2.5f, null));
    assertEquals(2.5d, converter.convertDouble("2.5", null));

    assertTrue(converter.convertBoolean(true));
    assertTrue(converter.convertBoolean("true"));
    assertThrows(IllegalArgumentException.class, () -> converter.convertBoolean(1));

    assertEquals("hello", converter.convertString("hello"));
    assertEquals("123", converter.convertString(123));
    assertEquals("abc", converter.convertString("abc".getBytes(StandardCharsets.UTF_8)));
    ByteBuffer utf8Buf = ByteBuffer.wrap("xyz".getBytes(StandardCharsets.UTF_8));
    assertEquals("xyz", converter.convertString(utf8Buf));
    assertEquals("xyz", converter.convertString(utf8Buf));
    assertEquals(0, utf8Buf.position());
    assertThrows(IllegalArgumentException.class, () -> converter.convertString(new Object()));

    UUID uuid = UUID.randomUUID();
    assertEquals(uuid, converter.convertUUID(uuid));
    assertEquals(uuid, converter.convertUUID(uuid.toString()));
    assertThrows(IllegalArgumentException.class, () -> converter.convertUUID(123));

    byte[] raw = new byte[] {1, 2, 3};
    assertEquals(ByteBuffer.wrap(raw), converter.convertBinary(raw, null));
    assertEquals(ByteBuffer.wrap(raw), converter.convertBinary(ByteBuffer.wrap(raw), null));
    assertEquals(
        ByteBuffer.wrap(raw),
        converter.convertBinary(Base64.getEncoder().encodeToString(raw), null));
    assertThrows(
        IllegalArgumentException.class, () -> converter.convertBinary("%%invalid%%", null));
    assertThrows(IllegalArgumentException.class, () -> converter.convertBinary(123, null));
  }

  @Test
  void testDecimalConversions() {
    Types.DecimalType type = Types.DecimalType.of(10, 2);
    assertEquals(
        new BigDecimal("12.35"), converter.convertDecimal(new BigDecimal("12.345"), type, null));
    assertEquals(new BigDecimal("12.30"), converter.convertDecimal(12.3d, type, null));
    assertEquals(new BigDecimal("99.95"), converter.convertDecimal("99.95", type, null));
    byte[] unscaled = BigInteger.valueOf(12345).toByteArray();
    assertEquals(new BigDecimal("123.45"), converter.convertDecimal(unscaled, type, null));
    assertEquals(
        new BigDecimal("123.45"), converter.convertDecimal(ByteBuffer.wrap(unscaled), type, null));
    assertThrows(
        IllegalArgumentException.class, () -> converter.convertDecimal(Boolean.TRUE, type, null));
  }

  @Test
  void testDateAndTimeConversions() {
    LocalDate expectedDate = LocalDate.of(2024, 6, 15);
    int epochDays = (int) expectedDate.toEpochDay();
    assertEquals(
        expectedDate, converter.convertDateValue(epochDays, io.debezium.time.Date.SCHEMA_NAME));
    assertEquals(expectedDate, converter.convertDateValue("2024-06-15Z", IsoDate.SCHEMA_NAME));
    assertEquals(
        expectedDate,
        converter.convertDateValue(
            Date.from(expectedDate.atStartOfDay().toInstant(ZoneOffset.UTC)),
            org.apache.kafka.connect.data.Date.LOGICAL_NAME));
    assertEquals(expectedDate, converter.convertDateValue(expectedDate, null));
    assertEquals(expectedDate, converter.convertDateValue("2024-06-15", null));
    assertThrows(
        IllegalArgumentException.class, () -> converter.convertDateValue(Boolean.TRUE, null));

    LocalTime expectedTime = LocalTime.of(10, 20, 30);
    assertEquals(
        expectedTime,
        converter.convertTimeValue(expectedTime.toNanoOfDay() / 1_000_000L, Time.SCHEMA_NAME));
    assertEquals(
        expectedTime,
        converter.convertTimeValue(expectedTime.toNanoOfDay() / 1_000L, MicroTime.SCHEMA_NAME));
    assertEquals(
        expectedTime, converter.convertTimeValue(expectedTime.toNanoOfDay(), NanoTime.SCHEMA_NAME));
    assertEquals(expectedTime, converter.convertTimeValue("10:20:30Z", IsoTime.SCHEMA_NAME));
    assertEquals(expectedTime, converter.convertTimeValue(expectedTime, null));
    assertThrows(
        IllegalArgumentException.class, () -> converter.convertTimeValue(Boolean.TRUE, null));
  }

  @Test
  void testTimestampConversionsAcrossModes() {
    long millis = 1700000000123L;
    LocalDateTime expectedLdt =
        Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).toLocalDateTime();
    OffsetDateTime expectedOdt = Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC);

    assertEquals(expectedLdt, converter.convertLocalDateTime(millis, Timestamp.SCHEMA_NAME));
    assertEquals(
        expectedLdt, converter.convertLocalDateTime(millis * 1000L, MicroTimestamp.SCHEMA_NAME));
    assertEquals(
        expectedLdt,
        converter.convertLocalDateTime(millis * 1_000_000L, NanoTimestamp.SCHEMA_NAME));
    assertEquals(
        expectedLdt,
        converter.convertLocalDateTime(
            new Date(millis), org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME));
    assertEquals(
        LocalDateTime.of(2024, 1, 2, 3, 4, 5),
        converter.convertLocalDateTime("2024-01-02T03:04:05Z", IsoTimestamp.SCHEMA_NAME));

    assertEquals(
        OffsetDateTime.parse("2024-01-02T03:04:05+02:00"),
        converter.convertOffsetDateTime("2024-01-02T03:04:05+02:00", ZonedTimestamp.SCHEMA_NAME));
    assertEquals(expectedOdt, converter.convertOffsetDateTime(millis, Timestamp.SCHEMA_NAME));
    assertEquals(
        expectedOdt,
        converter.convertTimestampValue(
            millis, Types.TimestampType.withoutZone(), "__ts_ms", null));
    assertEquals(
        expectedLdt,
        converter.convertTimestampValue(
            millis, Types.TimestampType.withoutZone(), null, Timestamp.SCHEMA_NAME));
    assertEquals(
        expectedOdt,
        converter.convertTimestampValue(
            millis, Types.TimestampType.withZone(), null, Timestamp.SCHEMA_NAME));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.convertTimestampValue(
                "not-a-number", Types.TimestampType.withZone(), "__ts_ms", null));

    IcebergValueConverter connectMode =
        new IcebergValueConverter(
            ConnectIcebergConverterConfig.fromMap(
                Map.of("temporal.precision.mode", TemporalPrecisionMode.CONNECT.getValue())));
    assertEquals(expectedLdt, connectMode.convertLocalDateTime(millis, null));
    assertEquals(expectedOdt, connectMode.convertOffsetDateTime(millis, null));

    IcebergValueConverter nanoMode =
        new IcebergValueConverter(
            ConnectIcebergConverterConfig.fromMap(
                Map.of("temporal.precision.mode", TemporalPrecisionMode.NANOSECONDS.getValue())));
    assertEquals(expectedLdt, nanoMode.convertLocalDateTime(millis * 1_000_000L, null));
    assertEquals(expectedOdt, nanoMode.convertOffsetDateTime(millis * 1_000_000L, null));
  }
}
