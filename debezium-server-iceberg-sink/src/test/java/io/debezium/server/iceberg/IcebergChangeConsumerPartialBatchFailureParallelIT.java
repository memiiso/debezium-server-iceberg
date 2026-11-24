/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
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
 * Delivery semantics of multi-table batches in append mode with the parallel upload enabled. See
 * {@link IcebergChangeConsumerPartialBatchFailureIT} for more detail test scenario description.
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerPartialBatchFailureParallelIT.TestProfile.class)
public class IcebergChangeConsumerPartialBatchFailureParallelIT
    extends PartialBatchFailureTestBase {

  @Test
  public void testParallelUploadPartialBatchFailureDuplicatesSucceedingTablesOnReplay()
      throws Exception {
    final String destA = "testc.inventory.pbf_par_a";
    final String destB = "testc.inventory.pbf_par_b";
    final String destC = "testc.inventory.pbf_par_c";

    final List<EmbeddedEngineChangeEvent> batch = new ArrayList<>();
    batch.add(eventFactory.of(destA, 1, "c"));
    batch.add(eventFactory.of(destA, 2, "c"));
    batch.add(eventFactory.of(destA, 3, "c"));
    batch.add(eventFactory.of(destB, 1, "c"));
    batch.add(eventFactory.of(destB, 2, "c"));
    batch.add(eventFactory.of(destB, 3, "c"));
    batch.add(eventFactory.of(destC, 1, "c"));
    batch.add(eventFactory.of(destC, 2, "c"));
    batch.add(eventFactory.of(destC, 3, "c"));

    final AtomicInteger committedRecords = new AtomicInteger();

    // first attempt: tables A and C commit, table B fails
    failWritesTo(destB);
    DebeziumException failure =
        assertThrows(
            DebeziumException.class,
            () -> consumer.handle(commitCountingBatch(batch, committedRecords)));
    assertEquals("Parallel table processing failed", failure.getMessage());
    assertNotNull(failure.getCause());
    assertTrue(
        failure.getCause().getMessage().contains(INJECTED_FAILURE_MESSAGE),
        "expected the injected failure as cause, got: " + failure.getCause().getMessage());

    assertEquals(0, committedRecords.get(), "a failed batch must not commit any record");
    assertEquals(3, rowCount(destA), "table A committed before the batch was aborted");
    assertEquals(3, rowCount(destC), "table C committed before the batch was aborted");
    assertEquals(0, rowCount(destB), "table B must not hold any data after a failed write");

    // debezium replays the whole batch, this time it succeeds
    committedRecords.set(0);
    restoreWrites();
    consumer.handle(commitCountingBatch(batch, committedRecords));

    assertEquals(batch.size(), committedRecords.get());
    assertEquals(6, rowCount(destA), "replaying the batch duplicates the rows of table A");
    assertEquals(6, rowCount(destC), "replaying the batch duplicates the rows of table C");
    assertEquals(2, rowCount(destA, 1));
    assertEquals(2, rowCount(destC, 1));
    assertEquals(3, rowCount(destB), "table B is correct after the replay");
    assertEquals(1, rowCount(destB, 1));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.upsert", "false");
      // parallel upload
      config.put("debezium.sink.batch.concurrent-uploads", "4");
      config.put("debezium.sink.batch.concurrent-uploads.timeout-minutes", "2");
      return config;
    }
  }
}
