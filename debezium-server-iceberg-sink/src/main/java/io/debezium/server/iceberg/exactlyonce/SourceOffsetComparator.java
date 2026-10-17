/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.exactlyonce;

import io.debezium.DebeziumException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Orders Debezium source offsets of a single connector. Used by exactly-once delivery to decide if
 * a replayed event is already committed to an Iceberg table.
 */
public interface SourceOffsetComparator {

  String SNAPSHOT_KEY = "snapshot";

  /**
   * Compares two source offsets.
   *
   * @return negative, zero or positive like {@link java.util.Comparator}, or empty when the offsets
   *     cannot be ordered (missing keys, different binlog, ...). Events with incomparable offsets
   *     are never skipped.
   */
  OptionalInt compare(Map<String, ?> offset, Map<String, ?> other);

  /** Offset keys used by {@link #compare}, the only ones stored in the snapshot summary. */
  List<String> positionKeys();

  /**
   * Keeps only the offset keys used by {@link #compare}. The full offset can hold the transaction
   * and incremental snapshot context, which don't need to be stored in every snapshot summary.
   */
  default Map<String, Object> position(Map<String, ?> offset) {
    Map<String, Object> position = new LinkedHashMap<>();
    for (String key : positionKeys()) {
      if (offset.get(key) != null) {
        position.put(key, offset.get(key));
      }
    }
    return position;
  }

  static SourceOffsetComparator forConnector(String connectorClass) {
    if (connectorClass == null || connectorClass.isBlank()) {
      throw new DebeziumException(
          "Exactly-once delivery requires `debezium.source.connector.class` to be set.");
    }
    return switch (connectorClass) {
      case "io.debezium.connector.postgresql.PostgresConnector" -> new PostgresOffsetComparator();
      case "io.debezium.connector.mysql.MySqlConnector" -> new BinlogOffsetComparator();
      default ->
          throw new DebeziumException(
              "Exactly-once delivery is not supported for connector '"
                  + connectorClass
                  + "'. Supported connectors: PostgresConnector, MySqlConnector.");
    };
  }

  /**
   * Snapshot events (initial, blocking or incremental) carry the snapshot type in their offset.
   * They all share the same position, so they cannot be ordered.
   */
  static boolean isSnapshot(Map<String, ?> offset) {
    Object snapshot = offset.get(SNAPSHOT_KEY);
    return snapshot != null && !"false".equalsIgnoreCase(snapshot.toString());
  }

  static Long longValue(Map<String, ?> offset, String key) {
    Object value = offset.get(key);
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String string && !string.isBlank()) {
      try {
        return Long.parseLong(string.trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
