/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.TestUtil;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Delivery semantics of multi-table batches in append mode, sequential processing path. Iceberg
 * commits are atomic per table, not across tables, and Debezium commits source offsets at batch
 * granularity only. {@link IcebergChangeConsumer#handle} therefore has no way to roll back a table
 * that already committed when a later table of the same batch fails. Append mode has no
 * deduplication, so the result is duplicate rows in table A.
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerPartialBatchFailureIT.TestProfile.class)
public class IcebergChangeConsumerPartialBatchFailureIT extends PartialBatchFailureTestBase {

  /** Table A commits, table B fails, the batch is replayed: table A ends up with duplicate rows. */
  @Test
  public void testAppendModePartialBatchFailureDuplicatesCommittedTableOnReplay() throws Exception {
    final String destA = "testc.inventory.pbf_append_a";
    final String destB = "testc.inventory.pbf_append_b";

    final List<EmbeddedEngineChangeEvent> batch = new ArrayList<>();
    batch.add(eventFactory.of(destA, 1, "c"));
    batch.add(eventFactory.of(destA, 2, "c"));
    batch.add(eventFactory.of(destA, 3, "c"));
    batch.add(eventFactory.of(destB, 1, "c"));
    batch.add(eventFactory.of(destB, 2, "c"));

    // first attempt: table A commits, table B fails
    failWritesTo(destB);
    DebeziumException failure =
        assertThrows(
            DebeziumException.class, () -> consumer.handle(TestUtil.toCapturingEvents(batch)));
    assertTrue(
        failure.getMessage().contains(INJECTED_FAILURE_MESSAGE),
        "expected the injected failure to propagate, got: " + failure.getMessage());

    // table A is already committed, table B was created but holds no data
    assertEquals(3, rowCount(destA), "table A should be committed even though the batch failed");
    assertEquals(0, rowCount(destB), "table B must not hold any data after a failed write");

    // Debezium replays the whole batch, this time it succeeds
    restoreWrites();
    consumer.handle(TestUtil.toCapturingEvents(batch));

    assertEquals(6, rowCount(destA), "replaying the batch duplicates the rows of table A");
    assertEquals(2, rowCount(destA, 1));
    assertEquals(2, rowCount(destA, 2));
    assertEquals(2, rowCount(destA, 3));
    assertEquals(2, dataFileCount(destA), "the duplicates come from two independent commits");

    assertEquals(2, rowCount(destB), "table B is correct after the replay");
    assertEquals(1, rowCount(destB, 1));
    assertEquals(1, rowCount(destB, 2));
  }

  /**
   * Offsets are committed at batch granularity: a single failing table leaves the offsets of the
   * whole batch uncommitted, including those of the tables that already made it to Iceberg.
   */
  @Test
  public void testNoOffsetsCommittedWhenOneTableFails() throws Exception {
    final String destA = "testc.inventory.pbf_offsets_a";
    final String destB = "testc.inventory.pbf_offsets_b";

    final List<EmbeddedEngineChangeEvent> batch = new ArrayList<>();
    batch.add(eventFactory.of(destA, 1, "c"));
    batch.add(eventFactory.of(destA, 2, "c"));
    batch.add(eventFactory.of(destB, 1, "c"));

    final AtomicInteger committedRecords = new AtomicInteger();

    failWritesTo(destB);
    assertThrows(
        DebeziumException.class,
        () -> consumer.handle(commitCountingBatch(batch, committedRecords)));

    assertEquals(
        0,
        committedRecords.get(),
        "no record of a failed batch may be committed, not even the ones of the committed table");
    assertEquals(
        2, rowCount(destA), "table A is persisted although its offsets were never committed");

    // the replay, which this uncommitted offset causes, finally commits the batch
    committedRecords.set(0);
    restoreWrites();
    consumer.handle(commitCountingBatch(batch, committedRecords));

    assertEquals(
        batch.size(), committedRecords.get(), "all records are committed once the batch succeeds");
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      // append mode (the default)
      config.put("debezium.sink.iceberg.upsert", "false");
      // sequential processing, keeps the per table processing order deterministic
      config.put("debezium.sink.batch.concurrent-uploads", "1");
      return config;
    }
  }
}
