/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.iceberg;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import org.apache.iceberg.util.DateTimeUtil;

public class DateTimeUtils {

  private DateTimeUtils() {}

  public static LocalDate toLocalDateFromDate(Date date) {
    return LocalDate.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneOffset.UTC);
  }

  public static LocalTime toLocalTimeFromDurationMilliseconds(long durationMillis) {
    return LocalTime.ofNanoOfDay(Duration.of(durationMillis, ChronoUnit.MILLIS).toNanos());
  }

  public static LocalTime toLocalTimeFromDurationMicroseconds(long durationMicros) {
    return LocalTime.ofNanoOfDay(Duration.of(durationMicros, ChronoUnit.MICROS).toNanos());
  }

  public static LocalTime toLocalTimeFromDurationNanoseconds(long durationNanos) {
    return LocalTime.ofNanoOfDay(Duration.of(durationNanos, ChronoUnit.NANOS).toNanos());
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
}
