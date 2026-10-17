/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.exactlyonce;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Orders Postgres offsets by {@code (lsn_commit, lsn)}.
 *
 * <p>Transactions are streamed in commit order, but the LSN of a change can be lower than the
 * commit LSN of the previous transaction (interleaved transactions). {@code lsn_commit} is the
 * commit LSN of the last transaction streamed before the event, so it increases monotonically, and
 * {@code lsn} increases within a transaction.
 *
 * <p>{@code lsn_commit} can be missing for the events of the first transaction streamed by the
 * connector, e.g. after the initial snapshot. On restart Debezium restores it from {@code
 * lsn_proc}, so the same events can be replayed with a different {@code lsn_commit}. When only one
 * offset has {@code lsn_commit}, the other one is the earlier event if it is below that commit LSN
 * (a commit LSN is greater than the LSN of all the changes of the transactions committed before
 * it), otherwise they are ordered by {@code lsn}.
 */
public class PostgresOffsetComparator implements SourceOffsetComparator {

  static final String LSN_KEY = "lsn";
  static final String LSN_COMMIT_KEY = "lsn_commit";

  @Override
  public List<String> positionKeys() {
    return List.of(LSN_KEY, LSN_COMMIT_KEY);
  }

  @Override
  public OptionalInt compare(Map<String, ?> offset, Map<String, ?> other) {
    Long lsn = SourceOffsetComparator.longValue(offset, LSN_KEY);
    Long otherLsn = SourceOffsetComparator.longValue(other, LSN_KEY);
    if (lsn == null || otherLsn == null) {
      return OptionalInt.empty();
    }
    Long commit = SourceOffsetComparator.longValue(offset, LSN_COMMIT_KEY);
    Long otherCommit = SourceOffsetComparator.longValue(other, LSN_COMMIT_KEY);

    if (commit != null && otherCommit != null) {
      int result = Long.compare(commit, otherCommit);
      return OptionalInt.of(result != 0 ? result : Long.compare(lsn, otherLsn));
    }
    if (commit != null && commit > otherLsn) {
      return OptionalInt.of(1);
    }
    if (otherCommit != null && otherCommit > lsn) {
      return OptionalInt.of(-1);
    }
    return OptionalInt.of(Long.compare(lsn, otherLsn));
  }
}
