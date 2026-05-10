/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Exposes the in-memory state of incremental snapshot completion to external
 * orchestrators (e.g. {@code gradual_snapshot_scheduler.py}) so they can decide
 * the exact moment to emit a final report based on actual Iceberg commit
 * activity, not on a polling timeout.
 *
 * <p>The endpoint lives on the application port (default 8080). It only reads
 * an immutable snapshot of in-memory maps so the call is cheap and tolerates
 * the Vert.x event loop being busy with I/O. Returns 200 with a JSON payload
 * describing which tables have been committed to Iceberg so far, which are
 * still being written, and recent activity timestamps useful to detect stalls.
 *
 * <p>State is in-memory and resets on pod restart by design. Consumers should
 * be ready to fall back to a Lakekeeper query if the pod restarted mid-snapshot.
 */
@Path("/v1/incremental/snapshot-status")
public class SnapshotStatusResource {

    @Inject
    IcebergChangeConsumer consumer;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public StatusPayload status() {
        IcebergChangeConsumer.StatusSnapshot snapshot = consumer.getStatusSnapshot();
        StatusPayload payload = new StatusPayload();
        payload.asOf = Instant.now().toString();

        IncrementalSnapshotStatus incremental = new IncrementalSnapshotStatus();
        Instant lastCompletionAt = null;
        long totalRowsCommitted = 0;

        for (IcebergChangeConsumer.CompletionInfo info : snapshot.completed()) {
            CompletedTable c = new CompletedTable();
            c.table = info.table();
            c.completedAt = info.completedAt().toString();
            c.rowsWritten = info.rowsWritten();
            c.filesCommitted = info.filesCommitted();
            c.bytesWritten = info.bytesWritten();
            incremental.completed.add(c);
            if (lastCompletionAt == null || info.completedAt().isAfter(lastCompletionAt)) {
                lastCompletionAt = info.completedAt();
            }
            totalRowsCommitted += info.rowsWritten();
        }

        for (IcebergChangeConsumer.InProgressSnapshot ip : snapshot.inProgress()) {
            InProgressTable p = new InProgressTable();
            p.table = ip.table();
            p.startedAt = ip.startedAt().toString();
            p.lastDataAt = ip.lastDataAt().toString();
            p.rowsBufferedSoFar = ip.rowsBufferedSoFar();
            p.writerOpen = ip.writerOpen();
            incremental.inProgress.add(p);
        }

        incremental.summary.completedCount = incremental.completed.size();
        incremental.summary.inProgressCount = incremental.inProgress.size();
        incremental.summary.totalRowsCommitted = totalRowsCommitted;
        incremental.summary.lastCompletionAt = lastCompletionAt != null ? lastCompletionAt.toString() : null;
        Instant lastDataReceivedAt = snapshot.lastDataReceivedAt();
        incremental.summary.lastDataReceivedAt = lastDataReceivedAt != null ? lastDataReceivedAt.toString() : null;

        payload.incrementalSnapshot = incremental;
        return payload;
    }

    @RegisterForReflection
    public static class StatusPayload {
        public IncrementalSnapshotStatus incrementalSnapshot;
        public String asOf;
    }

    @RegisterForReflection
    public static class IncrementalSnapshotStatus {
        public final List<CompletedTable> completed = new ArrayList<>();
        public final List<InProgressTable> inProgress = new ArrayList<>();
        public final Summary summary = new Summary();
    }

    @RegisterForReflection
    public static class CompletedTable {
        public String table;
        public String completedAt;
        public long rowsWritten;
        public int filesCommitted;
        public long bytesWritten;
    }

    @RegisterForReflection
    public static class InProgressTable {
        public String table;
        public String startedAt;
        public String lastDataAt;
        public long rowsBufferedSoFar;
        public boolean writerOpen;
    }

    @RegisterForReflection
    public static class Summary {
        public int completedCount;
        public int inProgressCount;
        public long totalRowsCommitted;
        public String lastCompletionAt;
        public String lastDataReceivedAt;
    }
}
