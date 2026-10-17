/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.exactlyonce;

import static io.debezium.server.iceberg.exactlyonce.ExactlyOnceHandler.SOURCE_OFFSET_PROP;
import static io.debezium.server.iceberg.exactlyonce.ExactlyOnceHandler.SOURCE_PARTITION_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.debezium.server.iceberg.converter.EventConverter;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExactlyOnceHandlerTest {

  static final Map<String, ?> PARTITION = Map.of("server", "testc");
  final ExactlyOnceHandler handler = new ExactlyOnceHandler(new PostgresOffsetComparator());
  Table table;

  @BeforeEach
  void setup() {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    table =
        catalog.createTable(
            TableIdentifier.of("ns", "tbl"),
            new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
  }

  static EventConverter event(long lsn) {
    return event(PARTITION, Map.of("lsn_commit", 100L, "lsn", lsn));
  }

  static EventConverter event(Map<String, ?> partition, Map<String, ?> offset) {
    EventConverter event = mock(EventConverter.class);
    lenient().when(event.sourcePartition()).thenAnswer(i -> partition);
    lenient().when(event.sourceOffset()).thenAnswer(i -> offset);
    return event;
  }

  void commit(Map<String, String> summary) {
    AppendFiles append = table.newAppend();
    summary.forEach(append::set);
    append.commit();
  }

  @Test
  void testNothingSkippedWithoutStoredOffset() {
    List<EventConverter> events = List.of(event(1), event(2));
    assertEquals(events, handler.skipCommitted(table, events));
    commit(Map.of());
    assertEquals(events, handler.skipCommitted(table, events));
  }

  @Test
  void testSkipCommitted() {
    List<EventConverter> batch = List.of(event(1), event(2), event(3));
    commit(handler.newBatch(batch, batch).commit(batch));

    EventConverter e4 = event(4);
    EventConverter e5 = event(5);
    // replay: events at or below the stored offset are skipped
    assertEquals(
        List.of(e4, e5), handler.skipCommitted(table, List.of(event(2), event(3), e4, e5)));

    // snapshot events, events of another source and events without offset are never skipped
    EventConverter snapshot = event(PARTITION, Map.of("lsn", 1L, "snapshot", "INITIAL"));
    EventConverter otherSource = event(Map.of("server", "other"), Map.of("lsn", 1L));
    EventConverter noOffset = event(null, null);
    List<EventConverter> notSkipped = List.of(snapshot, otherSource, noOffset);
    assertEquals(notSkipped, handler.skipCommitted(table, notSkipped));
  }

  @Test
  void testResetStopsWalkBack() {
    List<EventConverter> batch = List.of(event(1), event(2));
    commit(handler.newBatch(batch, batch).commit(batch));
    commit(Map.of(SOURCE_OFFSET_PROP, ExactlyOnceHandler.SOURCE_OFFSET_RESET));
    commit(Map.of());

    assertTrue(handler.lastCommittedOffset(table).isEmpty());
    assertEquals(batch, handler.skipCommitted(table, batch));

    // the next commit stores the offset again
    commit(handler.newBatch(batch, batch).commit(batch));
    assertTrue(handler.lastCommittedOffset(table).isPresent());
  }

  @Test
  void testStoredOffsetKeepsOnlyPosition() {
    EventConverter e1 =
        event(
            PARTITION,
            Map.of(
                "lsn_commit",
                100L,
                "lsn",
                1L,
                "txId",
                7L,
                "incremental_snapshot_window_opened",
                true));
    List<EventConverter> batch = List.of(e1);
    assertEquals(
        "{\"lsn\":1,\"lsn_commit\":100}",
        handler.newBatch(batch, batch).commit(batch).get(SOURCE_OFFSET_PROP));
  }

  @Test
  void testWalkBackSnapshotsWithoutOffset() {
    List<EventConverter> batch = List.of(event(1), event(2));
    commit(handler.newBatch(batch, batch).commit(batch));
    // e.g. compaction or another writer
    commit(Map.of());
    commit(Map.of("other", "value"));

    assertTrue(handler.lastCommittedOffset(table).isPresent());
    assertEquals(
        2L, ((Number) handler.lastCommittedOffset(table).get().offset().get("lsn")).longValue());
    EventConverter e3 = event(3);
    assertEquals(List.of(e3), handler.skipCommitted(table, List.of(event(1), event(2), e3)));
  }

  @Test
  void testBatchProgressMultipleCommits() {
    EventConverter e1 = event(1);
    EventConverter e2 = event(2);
    EventConverter e3 = event(3);
    EventConverter e4 = event(4);
    EventConverter e5 = event(5);
    // e1 is superseded by a later event (deduplication)
    List<EventConverter> events = List.of(e1, e2, e3, e4, e5);
    ExactlyOnceHandler.BatchProgress progress = handler.newBatch(events, List.of(e2, e3, e4, e5));

    // events committed grouped by schema: e2,e4 then e3,e5
    Map<String, String> first = progress.commit(List.of(e2, e4));
    assertTrue(first.get(SOURCE_OFFSET_PROP).contains("\"lsn\":2"), first.toString());
    assertEquals("{\"server\":\"testc\"}", first.get(SOURCE_PARTITION_PROP));

    Map<String, String> second = progress.commit(List.of(e3, e5));
    assertTrue(second.get(SOURCE_OFFSET_PROP).contains("\"lsn\":5"), second.toString());
  }

  @Test
  void testBatchProgressWithoutStreamingOffset() {
    EventConverter snapshot = event(PARTITION, Map.of("lsn", 1L, "snapshot", "INITIAL"));
    EventConverter noOffset = event(null, null);
    List<EventConverter> events = List.of(snapshot, noOffset);
    assertEquals(Map.of(), handler.newBatch(events, events).commit(events));

    // snapshot events are ignored, the last streaming event is stored
    EventConverter e7 = event(7);
    events = List.of(e7, snapshot);
    assertTrue(
        handler
            .newBatch(events, events)
            .commit(events)
            .get(SOURCE_OFFSET_PROP)
            .contains("\"lsn\":7"));
  }
}
