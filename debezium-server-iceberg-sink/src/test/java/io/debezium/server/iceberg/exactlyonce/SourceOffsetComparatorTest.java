/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.exactlyonce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import java.util.Map;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class SourceOffsetComparatorTest {

  @Test
  void testForConnector() {
    assertInstanceOf(
        PostgresOffsetComparator.class,
        SourceOffsetComparator.forConnector("io.debezium.connector.postgresql.PostgresConnector"));
    assertInstanceOf(
        BinlogOffsetComparator.class,
        SourceOffsetComparator.forConnector("io.debezium.connector.mysql.MySqlConnector"));
    // MariaDB always uses GTIDs
    assertThrows(
        DebeziumException.class,
        () ->
            SourceOffsetComparator.forConnector("io.debezium.connector.mariadb.MariaDbConnector"));
    assertThrows(
        DebeziumException.class,
        () ->
            SourceOffsetComparator.forConnector("io.debezium.connector.mongodb.MongoDbConnector"));
    assertThrows(DebeziumException.class, () -> SourceOffsetComparator.forConnector(null));
  }

  @Test
  void testIsSnapshot() {
    assertTrue(SourceOffsetComparator.isSnapshot(Map.of("snapshot", "INITIAL")));
    assertTrue(SourceOffsetComparator.isSnapshot(Map.of("snapshot", true)));
    assertFalse(SourceOffsetComparator.isSnapshot(Map.of("snapshot", false)));
    assertFalse(SourceOffsetComparator.isSnapshot(Map.of("lsn", 10L)));
  }

  @Test
  void testPostgres() {
    PostgresOffsetComparator comparator = new PostgresOffsetComparator();
    // interleaved transactions: change lsn is lower than the previous commit lsn
    Map<String, ?> txn1 = Map.of("lsn_commit", 100L, "lsn", 110L, "lsn_proc", 100L);
    Map<String, ?> txn2 = Map.of("lsn_commit", 120L, "lsn", 105L, "lsn_proc", 120L);
    assertEquals(OptionalInt.of(-1), comparator.compare(txn1, txn2));
    assertEquals(OptionalInt.of(1), comparator.compare(txn2, txn1));
    assertEquals(
        OptionalInt.of(0), comparator.compare(txn1, Map.of("lsn_commit", 100, "lsn", 110)));
    // same transaction
    assertEquals(
        OptionalInt.of(-1),
        comparator.compare(
            Map.of("lsn_commit", 120L, "lsn", 105L), Map.of("lsn_commit", 120L, "lsn", 106L)));
    // first transaction after a restart has no lsn_commit
    Map<String, ?> firstTxn = Map.of("lsn", 50L, "lsn_proc", 50L);
    // replayed later with lsn_commit restored from an older lsn_proc
    assertEquals(
        OptionalInt.of(0), comparator.compare(Map.of("lsn_commit", 40L, "lsn", 50L), firstTxn));
    assertEquals(
        OptionalInt.of(1), comparator.compare(Map.of("lsn_commit", 40L, "lsn", 51L), firstTxn));
    assertEquals(
        OptionalInt.of(-1), comparator.compare(Map.of("lsn_commit", 40L, "lsn", 49L), firstTxn));
    // later transaction, committed after the first one, with a lower change lsn
    assertEquals(
        OptionalInt.of(1), comparator.compare(Map.of("lsn_commit", 60L, "lsn", 45L), firstTxn));
    assertEquals(
        OptionalInt.of(-1), comparator.compare(firstTxn, Map.of("lsn_commit", 60L, "lsn", 45L)));
    assertEquals(OptionalInt.empty(), comparator.compare(Map.of("lsn_commit", 120L), txn1));
  }

  @Test
  void testBinlog() {
    BinlogOffsetComparator comparator = new BinlogOffsetComparator();
    Map<String, ?> base = Map.of("file", "mysql-bin.000009", "pos", 500L, "event", 2L, "row", 1);
    assertEquals(OptionalInt.of(0), comparator.compare(base, Map.copyOf(base)));
    assertEquals(
        OptionalInt.of(-1),
        comparator.compare(
            base, Map.of("file", "mysql-bin.000009", "pos", 500L, "event", 2L, "row", 2)));
    assertEquals(
        OptionalInt.of(-1),
        comparator.compare(base, Map.of("file", "mysql-bin.000009", "pos", 500L, "event", 3L)));
    assertEquals(
        OptionalInt.of(1),
        comparator.compare(
            base, Map.of("file", "mysql-bin.000009", "pos", 100L, "event", 9L, "row", 9)));
    // file sequence compared numerically
    assertEquals(
        OptionalInt.of(-1),
        comparator.compare(base, Map.of("file", "mysql-bin.000010", "pos", 4L)));
    assertEquals(
        OptionalInt.of(-1),
        comparator.compare(base, Map.of("file", "mysql-bin.1000000", "pos", 4L)));
    // different binlog base name cannot be ordered
    assertEquals(
        OptionalInt.empty(),
        comparator.compare(base, Map.of("file", "other-bin.000010", "pos", 4L)));
    assertEquals(OptionalInt.empty(), comparator.compare(base, Map.of("pos", 4L)));
  }

  @Test
  void testBinlogGtidsRejected() {
    BinlogOffsetComparator comparator = new BinlogOffsetComparator();
    Map<String, ?> base = Map.of("file", "mysql-bin.000009", "pos", 500L);
    Map<String, ?> gtid =
        Map.of("file", "mysql-bin.000009", "pos", 500L, "gtids", "3e11fa47-71ca-11e1-9e33:1-5");
    assertThrows(DebeziumException.class, () -> comparator.compare(gtid, base));
    assertThrows(DebeziumException.class, () -> comparator.compare(base, gtid));
    assertThrows(DebeziumException.class, () -> comparator.position(gtid));
  }

  @Test
  void testPositionKeepsOnlyComparedKeys() {
    assertEquals(
        Map.of("lsn", 10L, "lsn_commit", 5L),
        new PostgresOffsetComparator()
            .position(
                Map.of(
                    "lsn", 10L,
                    "lsn_commit", 5L,
                    "lsn_proc", 10L,
                    "txId", 7L,
                    "incremental_snapshot_primary_key", "aced0005")));
    assertEquals(Map.of("lsn", 10L), new PostgresOffsetComparator().position(Map.of("lsn", 10L)));
    assertEquals(
        Map.of("file", "mysql-bin.000009", "pos", 500L, "row", 1),
        new BinlogOffsetComparator()
            .position(Map.of("file", "mysql-bin.000009", "pos", 500L, "row", 1, "ts_sec", 1L)));
  }
}
