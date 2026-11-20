/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

/**
 * Base class for the partial-batch-failure integration tests.
 *
 * <p>Provides deterministic failure injection for a single destination table and helpers to count
 * rows and committed offsets.
 */
public class PartialBatchFailureTestBase extends BaseTest {

  /** Message carried by the injected failure, asserted by the tests. */
  protected static final String INJECTED_FAILURE_MESSAGE = "Injected table write failure";

  /**
   * Iceberg table name (short name, as returned by {@link #icebergTableName(String)}) whose write
   * should fail, or {@code null} to let every table succeed.
   */
  protected final AtomicReference<String> failingTable = new AtomicReference<>(null);

  private IcebergTableOperator originalTableOperator;

  @BeforeEach
  void installFailingTableOperator() {
    failingTable.set(null);
    originalTableOperator = consumer.icebergTableOperator;

    IcebergTableOperator spy = Mockito.spy(originalTableOperator);
    Mockito.doAnswer(
            invocation -> {
              final Table table = invocation.getArgument(0);
              final String tableToFail = failingTable.get();
              if (tableToFail != null && table.name().endsWith(tableToFail)) {
                throw new DebeziumException(INJECTED_FAILURE_MESSAGE + ": " + table.name());
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .addToTable(Mockito.any(), Mockito.any());

    consumer.icebergTableOperator = spy;
  }

  @AfterEach
  void restoreTableOperator() {
    failingTable.set(null);
    if (originalTableOperator != null) {
      consumer.icebergTableOperator = originalTableOperator;
      originalTableOperator = null;
    }
  }

  protected String icebergTableName(String destination) {
    return consumer.mapDestination(destination).name();
  }

  /** Cause that writes to the Iceberg table {@code destination} would fail. */
  protected void failWritesTo(String destination) {
    failingTable.set(icebergTableName(destination));
  }

  /** Restore the writes to succeed again. */
  protected void restoreWrites() {
    failingTable.set(null);
  }

  protected Table icebergTable(String destination) {
    final TableIdentifier tableId = consumer.mapDestination(destination);
    return consumer.loadIcebergTable(tableId, null);
  }

  /** Number of rows currently visible in the Iceberg table {@code destination}. */
  protected long rowCount(String destination) throws Exception {
    try (CloseableIterable<Record> rows = getTableDataV2(destination)) {
      return Lists.newArrayList(rows).size();
    }
  }

  /** Number of rows with the given {@code id} in the Iceberg table {@code destination}. */
  protected long rowCount(String destination, int id) throws Exception {
    try (CloseableIterable<Record> rows = getTableDataV2(destination)) {
      return Lists.newArrayList(rows).stream()
          .filter(r -> Objects.equals(r.getField("id"), id))
          .count();
    }
  }

  /**
   * Number of data files added across all snapshots of the table. Used to prove that a replay
   * really did write and commit a second time, instead of being skipped.
   */
  protected int dataFileCount(String destination) {
    return getDataFiles(icebergTable(destination)).size();
  }

  /**
   * Wraps events into a {@link CapturingEvents} batch, like {@code TestUtil.toCapturingEvents}, but
   * counting the per-record {@link BatchEvent#commit()} calls. {@link IcebergChangeConsumer#handle}
   * only commits records once every table of the batch succeeded, so the counter shows whether
   * source offsets would have been advanced.
   */
  protected static CapturingEvents<BatchEvent> commitCountingBatch(
      List<EmbeddedEngineChangeEvent> events, AtomicInteger commitCounter) {
    final List<BatchEvent> batch =
        events.stream().map(e -> toCommitCountingBatchEvent(e, commitCounter)).toList();

    return new CapturingEvents<>() {
      @Override
      public List<BatchEvent> records() {
        return batch;
      }

      @Override
      public String destination() {
        return null;
      }

      @Override
      public String source() {
        return "test";
      }

      @Override
      public String engine() {
        return "default";
      }
    };
  }

  private static BatchEvent toCommitCountingBatchEvent(
      EmbeddedEngineChangeEvent event, AtomicInteger commitCounter) {
    return new BatchEvent() {
      @Override
      public Object key() {
        return event.key();
      }

      @Override
      public Object value() {
        return event.value();
      }

      @Override
      public Integer partition() {
        return event.partition();
      }

      @Override
      public SourceRecord record() {
        return event.sourceRecord();
      }

      @Override
      public String destination() {
        return event.destination();
      }

      @Override
      public void commit() {
        commitCounter.incrementAndGet();
      }
    };
  }
}
