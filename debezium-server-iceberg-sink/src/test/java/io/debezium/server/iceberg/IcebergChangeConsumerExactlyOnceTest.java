/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import static io.debezium.server.iceberg.converter.EventFactory.withSourceOffset;
import static io.debezium.server.iceberg.exactlyonce.ExactlyOnceHandler.SOURCE_OFFSET_PROP;
import static io.debezium.server.iceberg.exactlyonce.ExactlyOnceHandler.SOURCE_PARTITION_PROP;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.debezium.server.iceberg.testresources.TestUtil;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies exactly-once delivery: the source offset is committed in the snapshot summary and
 * replayed events are skipped.
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerExactlyOnceTest.TestProfile.class)
public class IcebergChangeConsumerExactlyOnceTest extends BaseSparkTest {

  static final Map<String, ?> PARTITION = Map.of("server", "testc");

  EmbeddedEngineChangeEvent event(String dest, int id, long lsn) {
    return withSourceOffset(
        eventFactory.of(dest, id, "c"), PARTITION, Map.of("lsn_commit", 1000L, "lsn", lsn));
  }

  @Test
  public void testReplayedEventsAreSkipped() throws Exception {
    String dest = "testc.inventory.exactly_once_replay";
    List<EmbeddedEngineChangeEvent> records = new ArrayList<>();
    records.add(event(dest, 1, 1001));
    records.add(event(dest, 2, 1002));
    records.add(event(dest, 3, 1003));
    consumer.handleBatch(records, TestUtil.getCommitter());
    Assertions.assertEquals(3, getTableData(dest).count());

    Table table = consumer.loadIcebergTable(consumer.mapDestination(dest), null);
    Map<String, String> summary = table.currentSnapshot().summary();
    Assertions.assertTrue(
        summary.get(SOURCE_OFFSET_PROP).contains("\"lsn\":1003"), summary.toString());
    Assertions.assertEquals("{\"server\":\"testc\"}", summary.get(SOURCE_PARTITION_PROP));

    // crash before the offset storage commit: debezium replays events from its stored offset
    records.clear();
    records.add(event(dest, 2, 1002));
    records.add(event(dest, 3, 1003));
    records.add(event(dest, 4, 1004));
    consumer.handleBatch(records, TestUtil.getCommitter());
    Assertions.assertEquals(4, getTableData(dest).count());
    Assertions.assertEquals(1, getTableData(dest).where("id = 3").count());

    // fully replayed batch, nothing to commit
    long snapshotId =
        consumer
            .loadIcebergTable(consumer.mapDestination(dest), null)
            .currentSnapshot()
            .snapshotId();
    consumer.handleBatch(List.of(event(dest, 4, 1004)), TestUtil.getCommitter());
    Assertions.assertEquals(
        snapshotId,
        consumer
            .loadIcebergTable(consumer.mapDestination(dest), null)
            .currentSnapshot()
            .snapshotId());
    Assertions.assertEquals(4, getTableData(dest).count());
  }

  @Test
  public void testStreamingCommitsSourceOffset() throws Exception {
    SourcePostgresqlDB.runSQL(
        """
        CREATE TABLE IF NOT EXISTS inventory.exactly_once_streaming (c_id INTEGER, c_data TEXT);
        INSERT INTO inventory.exactly_once_streaming VALUES (1, 'a'), (2, 'b'), (3, 'c');
        """);

    Awaitility.await()
        .atMost(Duration.ofSeconds(180))
        .until(
            () -> {
              try {
                if (getTableData("testc.inventory.exactly_once_streaming").count() != 3) {
                  return false;
                }
                Table table =
                    consumer.loadIcebergTable(
                        consumer.mapDestination("testc.inventory.exactly_once_streaming"), null);
                Snapshot snapshot = table.currentSnapshot();
                String offset = snapshot.summary().get(SOURCE_OFFSET_PROP);
                LOGGER.info("Committed source offset {}", offset);
                return offset != null
                    && offset.contains("\"lsn\":")
                    && "{\"server\":\"testc\"}"
                        .equals(snapshot.summary().get(SOURCE_PARTITION_PROP));
              } catch (Exception e) {
                return false;
              }
            });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("debezium.sink.iceberg.exactly-once", "true");
    }
  }
}
