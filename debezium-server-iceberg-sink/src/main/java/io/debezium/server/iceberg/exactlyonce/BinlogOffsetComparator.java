/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.exactlyonce;

import io.debezium.DebeziumException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Orders MySQL offsets by {@code (file, pos, event, row)}.
 *
 * <p>{@code pos} is the position of the current transaction BEGIN, {@code event} the number of
 * events already processed in it and {@code row} the number of rows already processed in the
 * current event. Binlog files are ordered by their numeric extension; offsets from binlogs with a
 * different base name cannot be ordered.
 *
 * <p>Binlog positions are specific to a server. With GTIDs Debezium can resume on another server
 * (failover) where positions are unrelated, and replayed events not committed yet could be seen as
 * committed. GTID offsets are rejected until GTID sets are compared.
 */
public class BinlogOffsetComparator implements SourceOffsetComparator {

  static final String FILE_KEY = "file";
  static final String POS_KEY = "pos";
  static final String EVENT_KEY = "event";
  static final String ROW_KEY = "row";
  static final String GTIDS_KEY = "gtids";

  @Override
  public List<String> positionKeys() {
    return List.of(FILE_KEY, POS_KEY, EVENT_KEY, ROW_KEY);
  }

  @Override
  public Map<String, Object> position(Map<String, ?> offset) {
    rejectGtids(offset);
    return SourceOffsetComparator.super.position(offset);
  }

  @Override
  public OptionalInt compare(Map<String, ?> offset, Map<String, ?> other) {
    rejectGtids(offset);
    rejectGtids(other);
    Object file = offset.get(FILE_KEY);
    Object otherFile = other.get(FILE_KEY);
    Long pos = SourceOffsetComparator.longValue(offset, POS_KEY);
    Long otherPos = SourceOffsetComparator.longValue(other, POS_KEY);
    if (file == null || otherFile == null || pos == null || otherPos == null) {
      return OptionalInt.empty();
    }

    OptionalInt fileResult = compareFiles(file.toString(), otherFile.toString());
    if (fileResult.isEmpty() || fileResult.getAsInt() != 0) {
      return fileResult;
    }
    int result = Long.compare(pos, otherPos);
    if (result == 0) {
      result = Long.compare(longOrZero(offset, EVENT_KEY), longOrZero(other, EVENT_KEY));
    }
    if (result == 0) {
      result = Long.compare(longOrZero(offset, ROW_KEY), longOrZero(other, ROW_KEY));
    }
    return OptionalInt.of(result);
  }

  static OptionalInt compareFiles(String file, String other) {
    if (file.equals(other)) {
      return OptionalInt.of(0);
    }
    int dot = file.lastIndexOf('.');
    int otherDot = other.lastIndexOf('.');
    if (dot < 0
        || otherDot < 0
        || !Objects.equals(file.substring(0, dot), other.substring(0, otherDot))) {
      return OptionalInt.empty();
    }
    try {
      return OptionalInt.of(
          Long.compare(
              Long.parseLong(file.substring(dot + 1)),
              Long.parseLong(other.substring(otherDot + 1))));
    } catch (NumberFormatException e) {
      return OptionalInt.empty();
    }
  }

  private static void rejectGtids(Map<String, ?> offset) {
    if (offset.get(GTIDS_KEY) != null) {
      throw new DebeziumException(
          "Exactly-once delivery doesn't support MySQL with GTIDs enabled: binlog positions are not "
              + "comparable after a failover. Disable `debezium.sink.iceberg.exactly-once`.");
    }
  }

  private static long longOrZero(Map<String, ?> offset, String key) {
    Long value = SourceOffsetComparator.longValue(offset, key);
    return value == null ? 0L : value;
  }
}
