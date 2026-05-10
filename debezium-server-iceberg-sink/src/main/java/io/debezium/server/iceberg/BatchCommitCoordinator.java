/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates batching of snapshot table completions to optimize Iceberg file sizes.
 *
 * <p>Instead of committing each table individually (which creates many small files),
 * this coordinator accumulates tables until reaching the target batch size (~512MB)
 * and then commits them together in a single transaction.
 *
 * <p>This approach:
 * - Reduces S3/GCS PUT operations (cost optimization)
 * - Creates 512MB Parquet files (optimal for Trino queries)
 * - Reduces Iceberg manifest overhead
 * - Maintains low memory usage (batches instead of full buffering)
 *
 * @author Debezium Community
 */
public class BatchCommitCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCommitCoordinator.class);

    /**
     * Represents a completed table ready for batching.
     */
    public static class CompletedTable {
        private final String tableName;
        private final List<SourceRecord> records;
        private final long estimatedBytes;

        public CompletedTable(String tableName, List<SourceRecord> records) {
            this.tableName = tableName;
            this.records = records;
            // Estimate ~200 bytes per record (conservative)
            this.estimatedBytes = records.size() * 200L;
        }

        public String getTableName() {
            return tableName;
        }

        public List<SourceRecord> getRecords() {
            return records;
        }

        public long getEstimatedBytes() {
            return estimatedBytes;
        }

        public int getRecordCount() {
            return records.size();
        }
    }

    private final IcebergChangeConsumer consumer;
    private final long targetBatchBytes;
    private final int maxTablesPerBatch;
    private final long timeoutMs;

    private final List<CompletedTable> stagingBatch = new ArrayList<>();
    private final Lock batchLock = new ReentrantLock();
    private final ScheduledExecutorService timeoutExecutor;

    private long lastFlushTime = System.currentTimeMillis();
    private long currentBatchBytes = 0;

    /**
     * Creates a new batch commit coordinator.
     *
     * @param consumer The Iceberg consumer to flush batches to
     * @param targetBatchBytes Target batch size in bytes (e.g., 512MB = 536870912)
     * @param maxTablesPerBatch Maximum tables per batch (safety limit)
     * @param timeoutMs Maximum time to wait before flushing batch
     */
    public BatchCommitCoordinator(
            IcebergChangeConsumer consumer,
            long targetBatchBytes,
            int maxTablesPerBatch,
            long timeoutMs) {

        this.consumer = consumer;
        this.targetBatchBytes = targetBatchBytes;
        this.maxTablesPerBatch = maxTablesPerBatch;
        this.timeoutMs = timeoutMs;

        // Timeout executor for periodic flush
        this.timeoutExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "batch-commit-timeout");
            t.setDaemon(true);
            return t;
        });

        // Check for timeout every 5 seconds
        this.timeoutExecutor.scheduleAtFixedRate(
                this::checkTimeoutAndFlush,
                5000,
                5000,
                TimeUnit.MILLISECONDS
        );

        LOGGER.info("BatchCommitCoordinator initialized: targetBytes={} MB, maxTables={}, timeout={}ms",
                targetBatchBytes / 1024 / 1024, maxTablesPerBatch, timeoutMs);
    }

    /**
     * Called when a snapshot worker completes a table.
     *
     * @param tableName The completed table name
     * @param records Pre-transformed SourceRecords from the snapshot worker
     */
    public void onTableCompleted(String tableName, List<SourceRecord> records) {
        if (records == null || records.isEmpty()) {
            LOGGER.debug("Skipping empty table '{}'", tableName);
            return;
        }

        CompletedTable completedTable = new CompletedTable(tableName, records);

        batchLock.lock();
        try {
            stagingBatch.add(completedTable);
            currentBatchBytes += completedTable.getEstimatedBytes();

            LOGGER.info("[{}] Added table '{}' to staging batch ({} records, ~{} MB). Batch: {}/{} tables, ~{} MB",
                    Thread.currentThread().getName(),
                    tableName,
                    completedTable.getRecordCount(),
                    completedTable.getEstimatedBytes() / 1024 / 1024,
                    stagingBatch.size(),
                    maxTablesPerBatch,
                    currentBatchBytes / 1024 / 1024);

            // Check if batch is ready to commit
            boolean shouldFlush = currentBatchBytes >= targetBatchBytes ||
                                  stagingBatch.size() >= maxTablesPerBatch;

            if (shouldFlush) {
                LOGGER.info("Batch ready: {} tables, ~{} MB (target: {} MB). Flushing now.",
                        stagingBatch.size(),
                        currentBatchBytes / 1024 / 1024,
                        targetBatchBytes / 1024 / 1024);
                flushBatch();
            }
        }
        finally {
            batchLock.unlock();
        }
    }

    /**
     * Periodic check for timeout-based flush.
     */
    private void checkTimeoutAndFlush() {
        batchLock.lock();
        try {
            if (stagingBatch.isEmpty()) {
                return;
            }

            long elapsed = System.currentTimeMillis() - lastFlushTime;
            if (elapsed >= timeoutMs) {
                LOGGER.info("Batch timeout reached ({} ms). Flushing {} tables, ~{} MB",
                        elapsed,
                        stagingBatch.size(),
                        currentBatchBytes / 1024 / 1024);
                flushBatch();
            }
        }
        finally {
            batchLock.unlock();
        }
    }

    /**
     * Flush the current batch to Iceberg (must hold lock).
     */
    private void flushBatch() {
        if (stagingBatch.isEmpty()) {
            return;
        }

        List<CompletedTable> batchToFlush = new ArrayList<>(stagingBatch);
        long batchBytes = currentBatchBytes;

        // Clear staging for next batch
        stagingBatch.clear();
        currentBatchBytes = 0;
        lastFlushTime = System.currentTimeMillis();

        // Release lock before expensive I/O
        batchLock.unlock();
        try {
            // Delegate to consumer
            consumer.flushSnapshotBatch(batchToFlush);

            LOGGER.info("✅ Batch commit completed: {} tables, {} total rows, ~{} MB",
                    batchToFlush.size(),
                    batchToFlush.stream().mapToInt(CompletedTable::getRecordCount).sum(),
                    batchBytes / 1024 / 1024);
        }
        catch (Exception e) {
            LOGGER.error("❌ Batch commit failed: {}", e.getMessage(), e);
            throw new RuntimeException("Batch commit failed", e);
        }
        finally {
            batchLock.lock();
        }
    }

    /**
     * Flush any remaining batch (called during shutdown).
     */
    public void shutdown() {
        LOGGER.info("Shutting down BatchCommitCoordinator. Flushing remaining batch...");

        batchLock.lock();
        try {
            if (!stagingBatch.isEmpty()) {
                LOGGER.info("Flushing final batch: {} tables", stagingBatch.size());
                flushBatch();
            }
        }
        finally {
            batchLock.unlock();
        }

        timeoutExecutor.shutdown();
        try {
            if (!timeoutExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                timeoutExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            timeoutExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOGGER.info("BatchCommitCoordinator shutdown complete");
    }
}
