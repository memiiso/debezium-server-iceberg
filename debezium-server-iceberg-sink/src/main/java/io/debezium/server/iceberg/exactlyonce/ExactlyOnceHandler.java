/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.exactlyonce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.debezium.DebeziumException;
import io.debezium.server.iceberg.converter.EventConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exactly-once delivery: the source offset of the last committed event is stored in the summary of
 * the Iceberg snapshot holding the data, so data and offset are committed atomically per table. On
 * restart Debezium may replay events; events at or below the offset stored in the table are
 * skipped.
 *
 * <p>Snapshot events and events without source offset are never skipped and never stored.
 *
 * <p>A snapshot with the {@value #SOURCE_OFFSET_RESET} offset stops the history walk, to reset the
 * stored offset when the source positions go back.
 */
public class ExactlyOnceHandler {

  public static final String SOURCE_OFFSET_PROP = "debezium.source-offset";
  public static final String SOURCE_PARTITION_PROP = "debezium.source-partition";
  public static final String SOURCE_OFFSET_RESET = "reset";

  private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceHandler.class);
  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final SourceOffsetComparator comparator;

  public ExactlyOnceHandler(SourceOffsetComparator comparator) {
    this.comparator = comparator;
  }

  /**
   * Removes the events already committed to the table, based on the offset stored in the table
   * snapshot history.
   */
  public List<EventConverter> skipCommitted(Table table, List<EventConverter> events) {
    Optional<CommittedOffset> committed = lastCommittedOffset(table);
    if (committed.isEmpty()) {
      return events;
    }

    List<EventConverter> result = new ArrayList<>(events.size());
    EventConverter lastSkipped = null;
    for (EventConverter event : events) {
      if (isCommitted(committed.get(), event)) {
        lastSkipped = event;
      } else {
        result.add(event);
      }
    }

    if (lastSkipped != null) {
      LOGGER.warn(
          "Skipped {} events already committed to table {}, last skipped offset:{}, committed offset:{}",
          events.size() - result.size(),
          table.name(),
          comparator.position(lastSkipped.sourceOffset()),
          committed.get().offset());
    }
    return result;
  }

  /**
   * Walks back the snapshot history until a snapshot written by this sink is found. Snapshots
   * without the offset property (compaction, external writers) are skipped. A reset snapshot means
   * no offset is stored.
   */
  Optional<CommittedOffset> lastCommittedOffset(Table table) {
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null) {
      String offset = snapshot.summary().get(SOURCE_OFFSET_PROP);
      if (SOURCE_OFFSET_RESET.equals(offset)) {
        return Optional.empty();
      }
      if (offset != null) {
        return Optional.of(
            new CommittedOffset(
                toMap(snapshot.summary().get(SOURCE_PARTITION_PROP)), toMap(offset)));
      }
      snapshot = snapshot.parentId() == null ? null : table.snapshot(snapshot.parentId());
    }
    return Optional.empty();
  }

  private boolean isCommitted(CommittedOffset committed, EventConverter event) {
    if (!isStreamingEvent(event)) {
      return false;
    }
    // offsets of a different source are not comparable
    if (!Objects.equals(committed.partition(), normalize(event.sourcePartition()))) {
      return false;
    }
    OptionalInt result = comparator.compare(event.sourceOffset(), committed.offset());
    return result.isPresent() && result.getAsInt() <= 0;
  }

  /**
   * Tracks which events of the batch are committed, to store a safe offset in each snapshot when
   * the events of a table are committed with multiple commits.
   *
   * @param events events of the table in source order
   * @param toCommit events which will be written to the table, after deduplication
   */
  public BatchProgress newBatch(List<EventConverter> events, List<EventConverter> toCommit) {
    return new BatchProgress(events, toCommit);
  }

  private static boolean isStreamingEvent(EventConverter event) {
    return event.sourceOffset() != null
        && !event.sourceOffset().isEmpty()
        && !SourceOffsetComparator.isSnapshot(event.sourceOffset());
  }

  private static Map<String, Object> toMap(String json) {
    if (json == null) {
      return null;
    }
    try {
      return MAPPER.readValue(json, MAP_TYPE);
    } catch (JsonProcessingException e) {
      throw new DebeziumException("Failed to parse source offset '" + json + "'", e);
    }
  }

  private static String toJson(Map<String, ?> map) {
    try {
      return MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new DebeziumException("Failed to serialize source offset '" + map + "'", e);
    }
  }

  /** Converts the map like it is read back from the snapshot summary. */
  private static Map<String, Object> normalize(Map<String, ?> map) {
    return map == null ? null : MAPPER.convertValue(map, MAP_TYPE);
  }

  record CommittedOffset(Map<String, Object> partition, Map<String, Object> offset) {}

  public class BatchProgress {
    private final List<EventConverter> events;
    private final Set<EventConverter> pending = Collections.newSetFromMap(new IdentityHashMap<>());
    private int next = 0;
    private EventConverter watermark;

    private BatchProgress(List<EventConverter> events, List<EventConverter> toCommit) {
      this.events = events;
      this.pending.addAll(toCommit);
    }

    /**
     * Marks the events as committed and returns the snapshot summary properties to commit with
     * them. The stored offset is the offset of the last event such that all the events before it
     * are committed. Events removed by deduplication are superseded by a later event and don't need
     * to be committed.
     */
    public Map<String, String> commit(List<EventConverter> committed) {
      committed.forEach(pending::remove);
      while (next < events.size() && !pending.contains(events.get(next))) {
        EventConverter event = events.get(next++);
        if (isStreamingEvent(event)) {
          watermark = event;
        }
      }
      if (watermark == null) {
        return Map.of();
      }
      return Map.of(
          SOURCE_OFFSET_PROP, toJson(comparator.position(watermark.sourceOffset())),
          SOURCE_PARTITION_PROP, toJson(watermark.sourcePartition()));
    }
  }
}
