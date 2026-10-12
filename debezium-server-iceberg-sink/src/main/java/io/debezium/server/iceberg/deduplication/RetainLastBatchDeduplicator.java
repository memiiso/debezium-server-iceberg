/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.deduplication;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.tableoperator.Operation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplication strategy that retains the last event for each primary key (PK).
 *
 * <p>Because Debezium delivers change events in chronological commit order from the source database
 * log, retaining the latest encountered event accurately represents the final state of the row
 * within the batch with minimal computational overhead.
 *
 * <p>Preserves encounter order of unique keys using {@link LinkedHashMap} and propagates the {@code
 * isNewKey} optimization flag if the key was first inserted in this batch.
 *
 * @author Ismail Simsek
 */
public class RetainLastBatchDeduplicator implements BatchDeduplicator {

  @Override
  public List<EventConverter> deduplicate(List<EventConverter> events) {
    Map<Object, EventConverter> deduplicatedEvents = new LinkedHashMap<>();

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

          e.setNewKey(e.cdcOpValue() == Operation.INSERT);
          deduplicatedEvents.merge(
              e.key(),
              e,
              (oldVal, newVal) -> {
                newVal.setNewKey(oldVal.isNewKey());
                return newVal;
              });
        });

    return new ArrayList<>(deduplicatedEvents.values());
  }
}
