/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.deduplication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.tableoperator.Operation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BatchDeduplicator} implementations and resolution.
 *
 * @author Ismail Simsek
 */
class BatchDeduplicatorTest {

  private static EventConverter createRecord(Object key, Operation op, Long ts) {
    EventConverter event = mock(EventConverter.class);
    lenient().when(event.hasKeyData()).thenReturn(key != null);
    lenient().when(event.key()).thenReturn(key);
    lenient().when(event.destination()).thenReturn("test_table");
    lenient().when(event.value()).thenReturn("{\"val\":\"mock\"}");
    lenient().when(event.cdcOpValue()).thenReturn(op);
    if (ts != null) {
      lenient().when(event.cdcSourceTsValue()).thenReturn(ts);
    }
    final boolean[] newKeyHolder = new boolean[] {false};
    lenient()
        .doAnswer(
            inv -> {
              newKeyHolder[0] = inv.getArgument(0);
              return null;
            })
        .when(event)
        .setNewKey(anyBoolean());
    lenient().doAnswer(inv -> newKeyHolder[0]).when(event).isNewKey();
    return event;
  }

  @Test
  void retainLastKeepsLatestEncounteredAndPreservesOrder() {
    RetainLastBatchDeduplicator deduplicator = new RetainLastBatchDeduplicator();

    EventConverter c1 = createRecord(1, Operation.INSERT, 100L);
    EventConverter u1 = createRecord(1, Operation.UPDATE, 50L); // earlier ts, but later in batch
    EventConverter c2 = createRecord(2, Operation.INSERT, 200L);
    EventConverter d1 = createRecord(1, Operation.DELETE, 300L);

    List<EventConverter> deduped = deduplicator.deduplicate(List.of(c1, u1, c2, d1));
    assertEquals(2, deduped.size());

    // Preserves encounter order of unique keys: key 1 encountered first, key 2 second
    assertEquals(Integer.valueOf(1), deduped.get(0).key());
    assertEquals(Operation.DELETE, deduped.get(0).cdcOpValue());
    // isNewKey is preserved from initial INSERT
    assertTrue(deduped.get(0).isNewKey());

    assertEquals(Integer.valueOf(2), deduped.get(1).key());
    assertEquals(Operation.INSERT, deduped.get(1).cdcOpValue());
    assertTrue(deduped.get(1).isNewKey());
  }

  @Test
  void retainLastDoesNotMarkNewKeyWhenInitialEventIsNotInsert() {
    RetainLastBatchDeduplicator deduplicator = new RetainLastBatchDeduplicator();

    EventConverter u1 = createRecord(1, Operation.UPDATE, 100L);
    EventConverter u2 = createRecord(1, Operation.UPDATE, 200L);

    List<EventConverter> deduped = deduplicator.deduplicate(List.of(u1, u2));
    assertEquals(1, deduped.size());
    assertFalse(deduped.get(0).isNewKey());
  }

  @Test
  void retainLastPreservesExistingKeyStatusWhenUpdatePrecedesInsert() {
    RetainLastBatchDeduplicator deduplicator = new RetainLastBatchDeduplicator();

    // Key existed before this batch (started with an UPDATE), was deleted, then re-inserted
    EventConverter u1 = createRecord(1, Operation.UPDATE, 100L);
    EventConverter d1 = createRecord(1, Operation.DELETE, 200L);
    EventConverter c1 = createRecord(1, Operation.INSERT, 300L);

    List<EventConverter> deduped = deduplicator.deduplicate(List.of(u1, d1, c1));
    assertEquals(1, deduped.size());
    assertEquals(Operation.INSERT, deduped.get(0).cdcOpValue());
    // Since it already existed prior to this batch, it must not be treated as a pure brand new key
    assertFalse(deduped.get(0).isNewKey());
  }

  @Test
  void retainLastHandlesEmptyList() {
    RetainLastBatchDeduplicator deduplicator = new RetainLastBatchDeduplicator();
    assertTrue(deduplicator.deduplicate(Collections.emptyList()).isEmpty());
  }

  @Test
  void retainLastRejectsNullKey() {
    RetainLastBatchDeduplicator deduplicator = new RetainLastBatchDeduplicator();
    EventConverter nullKeyRecord = createRecord(null, Operation.INSERT, 100L);

    DebeziumException ex =
        assertThrows(
            DebeziumException.class, () -> deduplicator.deduplicate(List.of(nullKeyRecord)));
    assertTrue(ex.getMessage().contains("Cannot deduplicate data with null key!"));
  }

  @Test
  void timestampAndOpDeduplicatorComparesTimestampAndOp() {
    IcebergConfig config = mock(IcebergConfig.class);
    when(config.cdcSourceTsField()).thenReturn(Optional.of("__source_ts_ns"));
    TimestampAndOpBatchDeduplicator deduplicator = new TimestampAndOpBatchDeduplicator(config);

    // Higher timestamp wins regardless of position
    EventConverter laterTs = createRecord(1, Operation.INSERT, 200L);
    EventConverter earlierTs = createRecord(1, Operation.UPDATE, 100L);

    List<EventConverter> result1 = deduplicator.deduplicate(List.of(earlierTs, laterTs));
    assertEquals(1, result1.size());
    assertEquals(200L, result1.get(0).cdcSourceTsValue());

    List<EventConverter> result2 = deduplicator.deduplicate(List.of(laterTs, earlierTs));
    assertEquals(1, result2.size());
    assertEquals(200L, result2.get(0).cdcSourceTsValue());

    // Identical timestamp breaks tie using op priority: DELETE > UPDATE > READ > INSERT
    EventConverter insertOp = createRecord(2, Operation.INSERT, 100L);
    EventConverter updateOp = createRecord(2, Operation.UPDATE, 100L);
    EventConverter deleteOp = createRecord(2, Operation.DELETE, 100L);

    List<EventConverter> opResult = deduplicator.deduplicate(List.of(insertOp, deleteOp, updateOp));
    assertEquals(1, opResult.size());
    assertEquals(Operation.DELETE, opResult.get(0).cdcOpValue());
  }

  @Test
  void timestampAndOpDeduplicatorFallsBackToRetainLastWhenNoTsConfigured() {
    IcebergConfig config = mock(IcebergConfig.class);
    when(config.cdcSourceTsField()).thenReturn(Optional.empty());
    TimestampAndOpBatchDeduplicator deduplicator = new TimestampAndOpBatchDeduplicator(config);

    EventConverter c1 = createRecord(1, Operation.INSERT, null);
    EventConverter u1 = createRecord(1, Operation.UPDATE, null);

    List<EventConverter> deduped = deduplicator.deduplicate(List.of(c1, u1));
    assertEquals(1, deduped.size());
    assertEquals(Operation.UPDATE, deduped.get(0).cdcOpValue());
    assertTrue(deduped.get(0).isNewKey());
  }
}
