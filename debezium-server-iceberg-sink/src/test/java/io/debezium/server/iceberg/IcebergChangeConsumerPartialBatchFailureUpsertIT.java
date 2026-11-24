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
import org.junit.jupiter.api.Test;

/**
 * Delivery semantics of multi-table batches in upsert mode, sequential processing path. See {@link
 * IcebergChangeConsumerPartialBatchFailureIT} for more detail test scenario description. The
 * difference is what the replay does to table A: re-applying the same batch results into the same
 * final state instead of appending duplicates. That idempotency requires a primary key. Without
 * identifier fields {@code IcebergTableWriterFactory.create} falls back to the append writer even
 * when {@code upsert=true} resulting into the duplicates.
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerPartialBatchFailureUpsertIT.TestProfile.class)
public class IcebergChangeConsumerPartialBatchFailureUpsertIT extends PartialBatchFailureTestBase {

  /** Table A upserts, table B fails, the batch is replayed: table A stays without duplicates. */
  @Test
  public void testUpsertModePartialBatchFailureIsIdempotentOnReplay() throws Exception {
    final String destA = "testc.inventory.pbf_upsert_a";
    final String destB = "testc.inventory.pbf_upsert_b";

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

    assertEquals(3, rowCount(destA), "table A should be committed even though the batch failed");
    assertEquals(0, rowCount(destB), "table B must not hold any data after a failed write");

    // debezium replays the whole batch, this time it succeeds
    restoreWrites();
    consumer.handle(TestUtil.toCapturingEvents(batch));

    assertEquals(3, rowCount(destA), "upsert mode - no duplicates");
    assertEquals(1, rowCount(destA, 1));
    assertEquals(1, rowCount(destA, 2));
    assertEquals(1, rowCount(destA, 3));
    // the replay really did write and commit again, the rows were deduplicated by the equality
    // deletes rather than by skipping the write
    assertEquals(2, dataFileCount(destA));

    assertEquals(2, rowCount(destB), "table B is correct after the replay");
    assertEquals(1, rowCount(destB, 1));
    assertEquals(1, rowCount(destB, 2));
  }

  /**
   * Upsert mode only protects tables that have a primary key. Without PK field the writer factory
   * falls back to append and a replay duplicates the rows again.
   */
  @Test
  public void testUpsertModeFallsBackToAppendWithoutPrimaryKey() throws Exception {
    final String destA = "testc.inventory.pbf_upsert_no_pk_a";
    final String destB = "testc.inventory.pbf_upsert_no_pk_b";

    final List<EmbeddedEngineChangeEvent> batch = new ArrayList<>();
    batch.add(eventFactory.ofNoKey(destA, 1, "c", "user1", 1577840461000L));
    batch.add(eventFactory.ofNoKey(destA, 2, "c", "user2", 1577840461001L));
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

    assertEquals(2, rowCount(destA), "table A should be committed even though the batch failed");
    assertEquals(0, rowCount(destB), "table B must not hold any data after a failed write");

    // debezium replays the whole batch, this time it succeeds
    restoreWrites();
    consumer.handle(TestUtil.toCapturingEvents(batch));

    assertEquals(4, rowCount(destA), "upsert mode without PK - duplicates");
    assertEquals(2, rowCount(destA, 1));
    assertEquals(2, rowCount(destA, 2));
    assertEquals(2, dataFileCount(destA));

    assertEquals(2, rowCount(destB), "table B is correct after the replay");
    assertEquals(1, rowCount(destB, 1));
    assertEquals(1, rowCount(destB, 2));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.upsert", "true");
      config.put("debezium.sink.iceberg.upsert-keep-deletes", "true");
      config.put("debezium.sink.iceberg.format-version", "2");
      // sequential processing, keeps the per table processing order deterministic
      config.put("debezium.sink.batch.concurrent-uploads", "1");
      return config;
    }
  }
}
