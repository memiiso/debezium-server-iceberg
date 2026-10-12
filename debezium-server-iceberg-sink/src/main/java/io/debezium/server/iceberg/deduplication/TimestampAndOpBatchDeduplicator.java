/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.deduplication;

import com.google.common.collect.ImmutableMap;
import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.tableoperator.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deduplication strategy that compares events using a configured timestamp column and operation
 * priority (DELETE > UPDATE > READ > INSERT).
 *
 * <p>When no timestamp column is configured, it falls back to retaining the latest event in the
 * batch.
 *
 * @author Ismail Simsek
 */
public class TimestampAndOpBatchDeduplicator implements BatchDeduplicator {

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY =
      ImmutableMap.of(
          Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);

  private final IcebergConfig config;

  public TimestampAndOpBatchDeduplicator(IcebergConfig config) {
    this.config = config;
  }

  @Override
  public List<EventConverter> deduplicate(List<EventConverter> events) {
    ConcurrentHashMap<Object, EventConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(
        e -> {
          if (!e.hasKeyData()) {
            throw new DebeziumException(
                "Cannot deduplicate data with null key! destination:'"
                    + e.destination()
                    + "' event: '"
                    + e.value().toString()
                    + "'");
          }

          try {
            e.setNewKey(e.cdcOpValue() == Operation.INSERT);
            // deduplicate using key(PK)
            deduplicatedEvents.merge(
                e.key(),
                e,
                (oldValue, newValue) -> {
                  if (this.compareByTsThenOp(oldValue, newValue) <= 0) {
                    return newValue;
                  } else {
                    return oldValue;
                  }
                });
          } catch (Exception ex) {
            throw new DebeziumException("Failed to deduplicate events", ex);
          }
        });

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(EventConverter lhs, EventConverter rhs) {
    if (config == null || config.cdcSourceTsField().orElse("").isBlank()) {
      rhs.setNewKey(lhs.isNewKey());
      return -1;
    }

    Long lhsTs = lhs.cdcSourceTsValue();
    Long rhsTs = rhs.cdcSourceTsValue();
    int result;
    if (lhsTs == null && rhsTs == null) {
      result = 0;
    } else if (lhsTs == null) {
      result = -1;
    } else if (rhsTs == null) {
      result = 1;
    } else {
      result = Long.compare(lhsTs, rhsTs);
    }

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result =
          CDC_OPERATION_PRIORITY
              .getOrDefault(lhs.cdcOpValue(), -1)
              .compareTo(CDC_OPERATION_PRIORITY.getOrDefault(rhs.cdcOpValue(), -1));
    }

    return result;
  }
}
