/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.DebeziumException;
import io.debezium.embedded.Connect;
import io.debezium.embedded.ConverterBuilder;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.pipeline.notification.IncrementalSnapshotNotificationService;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.SnapshotStatus;
import io.debezium.server.iceberg.batchsizewait.BatchSizeWait;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.converter.StructEventConverter;
import io.debezium.server.iceberg.mapper.IcebergTableMapper;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@ApplicationScoped
public class IcebergChangeConsumer implements DebeziumEngine.ChangeConsumer<EmbeddedEngineChangeEvent> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);

  // Raw SourceRecord -> EmbeddedEngineChangeEvent wrapper, built once via Debezium's
  // ConverterBuilder configured with Connect format (no key/value/header conversion).
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static final java.util.function.Function<org.apache.kafka.connect.source.SourceRecord, EmbeddedEngineChangeEvent> RAW_WRAPPER = createRawWrapper();

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static java.util.function.Function<org.apache.kafka.connect.source.SourceRecord, EmbeddedEngineChangeEvent> createRawWrapper() {
    ConverterBuilder builder = new ConverterBuilder();
    builder.using(KeyValueHeaderChangeEventFormat.of(Connect.class, Connect.class, null));
    return builder.toFormat(null);
  }
  protected final Clock clock = Clock.system();
  final Configuration hadoopConf = new Configuration();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  protected static String keyValueChangeEventFormat;

  @Inject
  @Any
  Instance<BatchSizeWait> batchSizeWaitInstances;
  BatchSizeWait batchSizeWait;
  Catalog icebergCatalog;
  @Inject
  IcebergTableOperator icebergTableOperator;
  @Inject
  io.debezium.server.iceberg.tableoperator.IcebergTableWriterFactory snapshotWriterFactory;
  @Inject
  GlobalConfig config;

  @Inject
  @Any
  Instance<IcebergTableMapper> tableMappers;
  IcebergTableMapper tableMapper;
  private int numConcurrentUploads;
  private int concurrentUploadsTimeoutMinutes;
  ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private Semaphore concurrencyLimiter;

  // Topic used by Debezium core's SinkNotificationChannel to deliver notifications
  // (e.g. TABLE_SCAN_COMPLETED) inline with the data stream. Cached at startup so
  // handleBatch() can route notification records away from the data pipeline.
  private String notificationTopic;

  // Cached Cast SMT for snapshot records (lazy-initialized)
  private org.apache.kafka.connect.transforms.Cast.Value<org.apache.kafka.connect.source.SourceRecord> snapshotCastTransform;
  private boolean snapshotCastInitialized = false;

  // ============================================
  // PER-TABLE BUFFER ACCUMULATION
  // ============================================
  /**
   * In-memory buffer to accumulate events per destination table.
   * Key: table destination (e.g., "cdc.public.users")
   * Value: List of accumulated events for that table
   */
  private final Map<String, List<EventConverter>> tableEventBuffer = new ConcurrentHashMap<>();

  /**
   * Scheduled executor for periodic buffer flush.
   * Ensures low-traffic tables don't wait indefinitely.
   */
  private ScheduledExecutorService periodicFlushExecutor;

  /**
   * Last flush timestamp per table.
   * Used to enforce flush timeout for low-traffic tables.
   */
  private final Map<String, Long> lastFlushTime = new ConcurrentHashMap<>();

  /**
   * Counter for total buffered events across all tables.
   * Used to enforce max buffer size limit.
   */
  private final AtomicLong totalBufferedEvents = new AtomicLong(0);

  /**
   * Memory monitor executor for auto-flush on high memory pressure.
   */
  private ScheduledExecutorService memoryMonitorExecutor;

  /**
   * Flag to detect if snapshot is running (disables buffer accumulator).
   */
  private volatile boolean isSnapshotRunning = false;

  /**
   * Context for a streaming snapshot writer.
   * Holds an open Iceberg writer and table reference for chunk-by-chunk writes.
   */
  static class StreamingSnapshotContext {
    // Lazy-initialized on first chunk: table and writer are created from the SourceRecord schema
    // (StructSchemaConverter), not from JDBC metadata. This ensures the Iceberg table schema
    // exactly matches what Debezium CDC events produce — single source of truth for type mapping.
    Table icebergTable;
    final TableIdentifier icebergTableId;
    BaseTaskWriter<Record> writer;
    /**
     * Cached StructSchemaConverter for this table.
     * All records in a snapshot share the same schema, so we build the converter once
     * from the first record and reuse it for all subsequent records in the table.
     * This eliminates ~20K redundant StructSchemaConverter allocations per chunk.
     */
    volatile io.debezium.server.iceberg.converter.StructSchemaConverter cachedSchemaConverter;
    long totalRowsWritten = 0;
    long rowsSinceLastSplit = 0;
    /**
     * Adaptive file split threshold (rows).
     * 0 = not yet calibrated, use config default (snapshot.file-split-rows).
     * After first commit, calibrated from actual file size to produce files
     * of ~target-file-size-bytes (512MB).
     */
    long calibratedSplitRows = 0;
    /** Wall-clock instant the first chunk for this table was received. */
    final Instant startedAt = Instant.now();
    /** Wall-clock instant of the most recent chunk write for this table. */
    volatile Instant lastDataReceivedAt = startedAt;

    StreamingSnapshotContext(TableIdentifier icebergTableId) {
      this.icebergTableId = icebergTableId;
    }
  }

  /**
   * Snapshot completion record exposed by the snapshot-status endpoint. Built
   * once when {@link #completeSnapshotTable(String)} successfully commits to
   * Iceberg, and retained in-memory for the lifetime of the pod.
   */
  public record CompletionInfo(
      String table,
      Instant completedAt,
      long rowsWritten,
      int filesCommitted,
      long bytesWritten) {
  }

  /**
   * Active streaming snapshot writers, keyed by source table name.
   * Each table has at most one open writer at a time.
   */
  // Shared across CDI instances of this bean. Quarkus may instantiate the
  // consumer more than once (e.g. one for the DebeziumServer pipeline, one for
  // the management-interface route registration), and the route handler must
  // observe the same maps that the active pipeline populates. Static fields
  // are scoped to the classloader = the JVM = exactly one shared view.
  private static final Map<String, StreamingSnapshotContext> activeSnapshotWriters = new ConcurrentHashMap<>();

  /**
   * Tables whose incremental snapshot has been committed to Iceberg in the
   * current pod lifetime, keyed by destination (e.g. "cdc.public.users").
   * Insertion-ordered for stable status reporting. Resets on pod restart by design.
   */
  private static final Map<String, CompletionInfo> completedSnapshotTables = new ConcurrentSkipListMap<>();

  /**
   * Per-table accumulator for the direct-commit path (processTablesInParallel /
   * processTablesSequentially). Aggregates rows/files/bytes returned by each
   * {@link IcebergTableOperator#addToTable} call so that completeSnapshotTable
   * can report meaningful counters when no persistent writer was kept.
   * Cleared on completeSnapshotTable to avoid bleeding into the next snapshot.
   */
  private static final Map<String, DirectCommitCounter> directCommitCounters = new ConcurrentHashMap<>();

  /**
   * Tables currently being scanned by the incremental snapshot in the
   * direct-commit path, populated from {@code IN_PROGRESS} notifications and
   * cleared on {@code TABLE_SCAN_COMPLETED}. Surfaces in-flight work to the
   * status endpoint when no persistent {@link StreamingSnapshotContext} exists.
   */
  private static final Map<String, DirectCommitInProgress> inProgressDirectCommit = new ConcurrentHashMap<>();

  static final class DirectCommitCounter {
    final java.util.concurrent.atomic.AtomicLong rows = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong bytes = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicInteger files = new java.util.concurrent.atomic.AtomicInteger();
  }

  static final class DirectCommitInProgress {
    final Instant startedAt;
    volatile Instant lastNotificationAt;

    DirectCommitInProgress(Instant startedAt) {
      this.startedAt = startedAt;
      this.lastNotificationAt = startedAt;
    }
  }

  /** Updated by handleBatch to surface idle time to the status endpoint. */
  private static volatile Instant lastDataReceivedAt;

  /**
   * Batch commit coordinator for snapshot tables (v17).
   * Batches tables together to create optimal 512MB Parquet files.
   */
  private BatchCommitCoordinator batchCommitCoordinator;

  @PostConstruct
  void connect() {
    config.debezium().validateTemporalPrecisionMode();

    JsonEventConverter.initializeStaticSerdes();
    keyValueChangeEventFormat = config.debezium().keyValueChangeEventFormat();
    LOGGER.info("IcebergChangeConsumer is configured to use the '{}' format for processing events.", keyValueChangeEventFormat);
    // pass iceberg properties to iceberg and hadoop
    config.iceberg().icebergConfigs().forEach(this.hadoopConf::set);

    icebergCatalog = CatalogUtil.buildIcebergCatalog(config.iceberg().catalogName(), config.iceberg().icebergConfigs(), hadoopConf);
    batchSizeWait = IcebergUtil.selectInstance(batchSizeWaitInstances, config.batch().batchSizeWaitName());
    batchSizeWait.initizalize();
    tableMapper = IcebergUtil.selectInstance(tableMappers, config.iceberg().tableMapper());

    this.numConcurrentUploads = config.batch().concurrentUploads();
    this.concurrentUploadsTimeoutMinutes = config.batch().concurrentUploadsTimeoutMinutes();

    final String configuredNotificationTopic = config.debezium().notificationTopic();
    this.notificationTopic = (configuredNotificationTopic == null || configuredNotificationTopic.isBlank())
        ? null
        : configuredNotificationTopic;
    if (this.notificationTopic != null) {
      LOGGER.info("Listening for Debezium notifications on topic '{}' (TABLE_SCAN_COMPLETED triggers per-table snapshot flush) [instance@{}]",
          this.notificationTopic, Integer.toHexString(System.identityHashCode(this)));
    }

    // Initialize the semaphore for parallel processing
    if (numConcurrentUploads > 1) {
      this.concurrencyLimiter = new Semaphore(numConcurrentUploads);
      LOGGER.info("Parallel uploads enabled with concurrency limit: {}", numConcurrentUploads);
    }

    // Initialize periodic buffer flush if enabled
    if (config.batch().bufferPerTableEnabled()) {
      long flushIntervalMs = config.batch().bufferPerTableFlushIntervalMs();
      this.periodicFlushExecutor = Executors.newScheduledThreadPool(1);
      this.periodicFlushExecutor.scheduleAtFixedRate(
          this::flushTimedOutBuffers,
          flushIntervalMs,
          flushIntervalMs,
          TimeUnit.MILLISECONDS
      );
      LOGGER.info("Per-table buffer accumulation ENABLED:");
      LOGGER.info("  - Buffer threshold: {} events/table", config.batch().bufferPerTableThreshold());
      LOGGER.info("  - Max tables in buffer: {}", config.batch().bufferPerTableMaxTables());
      LOGGER.info("  - Periodic flush interval: {}ms", flushIntervalMs);
      LOGGER.info("  - Retry max attempts: {}", config.batch().bufferPerTableRetryMaxAttempts());
      LOGGER.info("  - Retry initial delay: {}ms", config.batch().bufferPerTableRetryInitialDelayMs());

      // Initialize memory monitor for auto-flush on high memory pressure
      this.memoryMonitorExecutor = Executors.newScheduledThreadPool(1);
      this.memoryMonitorExecutor.scheduleAtFixedRate(
          this::checkMemoryPressureAndFlush,
          30000,  // Initial delay 30s
          30000,  // Check every 30s
          TimeUnit.MILLISECONDS
      );
      LOGGER.info("Memory-aware auto-flush ENABLED (threshold: 80% heap usage)");
    } else {
      LOGGER.info("Per-table buffer accumulation DISABLED (original behavior)");
    }

    // Initialize batch commit coordinator for snapshot tables (v17)
    // Target: 512MB batches, max 20 tables per batch, 30s timeout
    long targetBatchBytes = 536_870_912L;  // 512 MB
    int maxTablesPerBatch = 20;
    long timeoutMs = 30_000L;  // 30 seconds

    this.batchCommitCoordinator = new BatchCommitCoordinator(
        this,
        targetBatchBytes,
        maxTablesPerBatch,
        timeoutMs
    );
  }

  /**
   * Flush snapshot table data immediately (batch coordinator fallback path).
   * Called by {@link BatchCommitCoordinator} via {@link #flushSnapshotBatch}.
   *
   * <p>Records are pre-transformed SourceRecords from the snapshot worker pipeline.
   *
   * @param tableName Fully qualified table name
   * @param records Pre-transformed SourceRecords
   */
  public void flushSnapshotTable(String tableName, List<org.apache.kafka.connect.source.SourceRecord> records) {
    if (records == null || records.isEmpty()) {
      LOGGER.debug("No records to flush for table '{}'", tableName);
      return;
    }

    try {
      // Prepend topic prefix to align snapshot table names with CDC table names
      // CDC events use topic name "cdc.public.table" -> "cdc_public_table"
      // Snapshot events receive "public.table" -> without prefix would be "public_table"
      // By adding prefix, snapshot writes to the SAME table as CDC: "cdc_public_table"
      String topicPrefix = config.debezium().topicPrefix();
      String unifiedDestination = topicPrefix + "." + tableName;

      // Map destination to Iceberg table identifier
      TableIdentifier icebergTableId = mapDestination(unifiedDestination);

      // Convert SourceRecords to EventConverters
      // Uses cached StructSchemaConverter: all records share the same schema.
      List<EventConverter> events = new ArrayList<>(records.size());
      io.debezium.server.iceberg.converter.StructSchemaConverter sharedConverter = null;
      for (org.apache.kafka.connect.source.SourceRecord record : records) {
        try {
          // Build StructSchemaConverter once from first record, reuse for all
          if (sharedConverter == null) {
            sharedConverter = new io.debezium.server.iceberg.converter.StructSchemaConverter(
                record.valueSchema(),
                record.keySchema(),
                config);
          }

          EmbeddedEngineChangeEvent wrappedEvent = RAW_WRAPPER.apply(record);
          events.add(new StructEventConverter(wrappedEvent, config, sharedConverter));
        } catch (Exception e) {
          LOGGER.error("Failed to convert SourceRecord to event for table '{}': {}",
              tableName, e.getMessage(), e);
          throw new RuntimeException("Failed to convert SourceRecord to event", e);
        }
      }

      // Load or create Iceberg table from SourceRecord schema
      EventConverter sampleEvent = events.get(0);
      org.apache.iceberg.Table icebergTable = loadIcebergTable(icebergTableId, sampleEvent);

      // Make non-PK required fields optional to prevent NPE on NULL values
      java.util.Set<Integer> pkFieldIds = icebergTable.schema().identifierFieldIds();
      org.apache.iceberg.UpdateSchema schemaUpdate = null;
      for (org.apache.iceberg.types.Types.NestedField field : icebergTable.schema().columns()) {
        if (field.isRequired() && !pkFieldIds.contains(field.fieldId())) {
          if (schemaUpdate == null) {
            schemaUpdate = icebergTable.updateSchema();
          }
          schemaUpdate.makeColumnOptional(field.name());
        }
      }
      if (schemaUpdate != null) {
        schemaUpdate.commit();
        LOGGER.info("Evolved schema for table '{}': made non-PK required fields optional", tableName);
        icebergTable = IcebergUtil.loadIcebergTable(icebergCatalog, icebergTableId)
            .orElseThrow(() -> new RuntimeException("Table disappeared after schema evolution: " + tableName));
      }

      LOGGER.info("Flushing {} snapshot records for table '{}'", events.size(), tableName);

      // Use tested IcebergTableOperator.addToTable() (handles null, upsert, schema evolution)
      // This is the same code path used for all CDC events and is proven to work!
      // Note: deduplicateBatch() sets newKey=true for READ operations (snapshot events),
      // so the upsert writer does a direct INSERT without equality delete.
      icebergTableOperator.addToTable(icebergTable, events);

      logConsumerProgress(events.size());
      LOGGER.info("Successfully flushed {} snapshot records for table '{}'", events.size(), tableName);
    }
    catch (Exception e) {
      LOGGER.error("Failed to flush snapshot table '{}': {}", tableName, e.getMessage(), e);
      throw new RuntimeException("Failed to flush snapshot table " + tableName, e);
    }
  }

  /**
   * Write a chunk of pre-transformed snapshot records to a persistent writer.
   * If no writer exists for the table, initializes one (schema evolution + writer creation).
   * The writer stays open until {@link #completeSnapshotTable(String, String)} is called.
   *
   * @param tableName Fully qualified table name from snapshot worker
   * @param records Pre-transformed SourceRecords from the snapshot pipeline
   */
  public void writeSnapshotChunk(String tableName, List<org.apache.kafka.connect.source.SourceRecord> records) {
    if (records == null || records.isEmpty()) {
      LOGGER.debug("Empty chunk for table '{}', skipping", tableName);
      return;
    }

    StreamingSnapshotContext ctx = activeSnapshotWriters.get(tableName);

    // First chunk for this table — initialize context
    if (ctx == null) {
      String topicPrefix = config.debezium().topicPrefix();
      String unifiedDestination = topicPrefix + "." + tableName;
      TableIdentifier icebergTableId = mapDestination(unifiedDestination);
      ctx = new StreamingSnapshotContext(icebergTableId);
      activeSnapshotWriters.put(tableName, ctx);
    }

    // Convert SourceRecords to EventConverters.
    // Snapshot records arrive in Debezium envelope format {before, after, source, op, ts_ms}
    // but the consumer expects flat/unwrapped records (table fields + __op, __source_ts_ms, etc.)
    // matching what ExtractNewRecordState SMT produces for the CDC path.
    // We unwrap them here before conversion.
    List<EventConverter> events = new ArrayList<>(records.size());
    for (org.apache.kafka.connect.source.SourceRecord record : records) {
      try {
        org.apache.kafka.connect.source.SourceRecord flatRecord = unwrapEnvelopeRecord(record);
        if (flatRecord == null) {
          continue;
        }
        flatRecord = applySnapshotCast(flatRecord);

        if (ctx.cachedSchemaConverter == null) {
          ctx.cachedSchemaConverter = new io.debezium.server.iceberg.converter.StructSchemaConverter(
              flatRecord.valueSchema(),
              flatRecord.keySchema(),
              config);
        }

        EmbeddedEngineChangeEvent wrappedEvent = RAW_WRAPPER.apply(flatRecord);
        events.add(new StructEventConverter(wrappedEvent, config, ctx.cachedSchemaConverter));
      } catch (Exception e) {
        LOGGER.error("Failed to convert SourceRecord to event for table '{}': {}",
            tableName, e.getMessage(), e);
        throw new RuntimeException("Failed to convert snapshot record", e);
      }
    }

    if (events.isEmpty()) {
      return;
    }

    // Lazy init: create Iceberg table and writer from the first SourceRecord's schema.
    // This uses StructSchemaConverter (same as CDC path) as single source of truth.
    if (ctx.icebergTable == null) {
      EventConverter sampleEvent = events.get(0);
      ctx.icebergTable = loadIcebergTable(ctx.icebergTableId, sampleEvent);
      ctx.writer = snapshotWriterFactory.create(ctx.icebergTable);
      LOGGER.info("Opened streaming snapshot writer for table '{}' (schema from CDC event)", tableName);
    }

    // Write chunk to open writer (no commit)
    icebergTableOperator.writeChunkToWriter(ctx.writer, ctx.icebergTable, events);
    ctx.totalRowsWritten += events.size();
    ctx.rowsSinceLastSplit += events.size();
    ctx.lastDataReceivedAt = Instant.now();

    // Adaptive file split: commit current writer and open a new one to bound memory.
    // First split uses config default (snapshot.file-split-rows, e.g. 500K).
    // After first commit, calibrates from actual file size to produce files
    // of ~target-file-size-bytes (512MB default).
    long splitThreshold = ctx.calibratedSplitRows > 0
        ? ctx.calibratedSplitRows
        : Long.parseLong(config.iceberg().icebergConfigs()
            .getOrDefault("snapshot.file-split-rows", "500000"));

    if (splitThreshold > 0 && ctx.rowsSinceLastSplit >= splitThreshold) {
      try {
        LOGGER.info("File split triggered for '{}': {} rows since last split (threshold: {}{}). Committing...",
            tableName, ctx.rowsSinceLastSplit, splitThreshold,
            ctx.calibratedSplitRows > 0 ? ", calibrated" : ", initial");

        IcebergTableOperator.CommitResult result =
            icebergTableOperator.commitWriter(ctx.writer, ctx.icebergTable);

        // Adaptive calibration: compute optimal rows per target-file-size from actual data,
        // then clamp based on available heap memory to prevent OOM.
        if (result.totalBytes > 0 && result.totalRecords > 0) {
          String targetSizeStr = config.iceberg().icebergConfigs()
              .getOrDefault("write.target-file-size-bytes", "536870912");
          long targetFileSize = Long.parseLong(targetSizeStr);
          double parquetBytesPerRow = (double) result.totalBytes / result.totalRecords;

          // 1) File-size based: how many rows to reach targetFileSize
          long fileSizeThreshold = (long) (targetFileSize / parquetBytesPerRow);

          // 2) Memory-aware clamp: limit rows per writer based on available heap.
          //    Parquet writer in-memory cost includes Record objects, row-group buffers,
          //    dictionary encoders, and column vectors. For wide/denormalized tables with
          //    many string columns, dictionaries grow significantly. Using 40x multiplier
          //    (previously 15x caused OOM on wide/denormalized tables).
          Runtime runtime = Runtime.getRuntime();
          long maxHeap = runtime.maxMemory();
          int numWorkers = org.eclipse.microprofile.config.ConfigProvider.getConfig()
              .getOptionalValue("debezium.source.snapshot.max.threads", Integer.class)
              .orElse(2);
          double writerMemoryFraction = 0.6;
          long memoryPerWriter = (long) (maxHeap * writerMemoryFraction / numWorkers);
          double inMemoryBytesPerRow = parquetBytesPerRow * 40;
          long memoryThreshold = (long) (memoryPerWriter / inMemoryBytesPerRow);

          // Use the smaller of file-size target and memory limit
          long newThreshold = Math.min(fileSizeThreshold, memoryThreshold);
          // Minimum 50K rows to avoid too-frequent splits
          newThreshold = Math.max(50_000, newThreshold);
          ctx.calibratedSplitRows = newThreshold;

          LOGGER.info("Calibrated split for '{}': {} bytes/{} records = {} bytes/row. "
              + "File target: {} rows (for {} MB files). "
              + "Memory limit: {} rows ({} MB heap, {} workers, {} MB/writer). "
              + "-> threshold: {} rows",
              tableName, result.totalBytes, result.totalRecords, (long) parquetBytesPerRow,
              fileSizeThreshold, targetFileSize / 1024 / 1024,
              memoryThreshold, maxHeap / 1024 / 1024, numWorkers, memoryPerWriter / 1024 / 1024,
              ctx.calibratedSplitRows);
        }

        LOGGER.info("File split committed for '{}': {} files, {} MB written",
            tableName, result.fileCount, result.totalBytes / 1024 / 1024);

        // Refresh table metadata to pick up the committed files
        ctx.icebergTable.refresh();

        // Open a new writer for the same table
        ctx.writer = snapshotWriterFactory.create(ctx.icebergTable);
        ctx.rowsSinceLastSplit = 0;

        LOGGER.info("Opened new writer for '{}' after file split (total rows: {})",
            tableName, ctx.totalRowsWritten);
      } catch (Exception e) {
        LOGGER.error("Failed to perform file split for table '{}' at {} rows: {}",
            tableName, ctx.totalRowsWritten, e.getMessage(), e);
        throw new RuntimeException("Failed to perform snapshot file split for " + tableName, e);
      }
    }

    LOGGER.info("Wrote chunk of {} rows to streaming writer for '{}' (total: {}, since split: {})",
        events.size(), tableName, ctx.totalRowsWritten, ctx.rowsSinceLastSplit);
  }

  /**
   * Unwraps a Debezium envelope record into a flat record matching ExtractNewRecordState output.
   * Envelope format: {before, after, source, op, ts_ms, ts_us, ts_ns}
   * Flat format: {table_col1, table_col2, ..., __op, __table, __source_ts_ms, __source_ts_ns, __db, __ts_ms, __ts_ns}
   */
  private org.apache.kafka.connect.source.SourceRecord unwrapEnvelopeRecord(
      org.apache.kafka.connect.source.SourceRecord record) {

    if (record.value() == null || !(record.value() instanceof org.apache.kafka.connect.data.Struct)) {
      return record;
    }

    org.apache.kafka.connect.data.Struct envelope = (org.apache.kafka.connect.data.Struct) record.value();
    org.apache.kafka.connect.data.Schema envelopeSchema = record.valueSchema();

    if (envelopeSchema.field("after") == null || envelopeSchema.field("source") == null) {
      return record;
    }

    org.apache.kafka.connect.data.Struct afterStruct = envelope.getStruct("after");
    if (afterStruct == null) {
      afterStruct = envelope.getStruct("before");
      if (afterStruct == null) {
        LOGGER.warn("Both 'after' and 'before' are null in envelope record, skipping");
        return null;
      }
    }

    String opValue = envelope.getString("op");
    org.apache.kafka.connect.data.Struct sourceStruct = envelope.getStruct("source");

    org.apache.kafka.connect.data.SchemaBuilder builder =
        org.apache.kafka.connect.data.SchemaBuilder.struct();

    for (org.apache.kafka.connect.data.Field field : afterStruct.schema().fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field("__op", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("__table", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("__source_ts_ms", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA);
    builder.field("__source_ts_ns", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA);
    builder.field("__db", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("__ts_ms", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA);
    builder.field("__ts_ns", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA);

    org.apache.kafka.connect.data.Schema flatSchema = builder.build();
    org.apache.kafka.connect.data.Struct flatValue = new org.apache.kafka.connect.data.Struct(flatSchema);

    for (org.apache.kafka.connect.data.Field field : afterStruct.schema().fields()) {
      flatValue.put(field.name(), afterStruct.get(field));
    }

    flatValue.put("__op", opValue);

    if (sourceStruct != null) {
      if (sourceStruct.schema().field("table") != null) {
        flatValue.put("__table", sourceStruct.getString("table"));
      }
      if (sourceStruct.schema().field("ts_ms") != null) {
        flatValue.put("__source_ts_ms", sourceStruct.getInt64("ts_ms"));
      }
      if (sourceStruct.schema().field("ts_ns") != null) {
        flatValue.put("__source_ts_ns", sourceStruct.getInt64("ts_ns"));
      }
      if (sourceStruct.schema().field("db") != null) {
        flatValue.put("__db", sourceStruct.getString("db"));
      }
    }

    if (envelopeSchema.field("ts_ms") != null) {
      flatValue.put("__ts_ms", envelope.getInt64("ts_ms"));
    }
    if (envelopeSchema.field("ts_ns") != null) {
      flatValue.put("__ts_ns", envelope.getInt64("ts_ns"));
    }

    return new org.apache.kafka.connect.source.SourceRecord(
        record.sourcePartition(),
        record.sourceOffset(),
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        flatSchema,
        flatValue);
  }

  /**
   * Applies the configured Cast$Value SMT to snapshot records.
   * In the CDC path, records pass through the full transform chain (unwrap → cast → ...).
   * The snapshot path bypasses these transforms, so we must apply cast explicitly
   * to match the same type conversions (e.g., UUID→string, float→int64).
   */
  private org.apache.kafka.connect.source.SourceRecord applySnapshotCast(
      org.apache.kafka.connect.source.SourceRecord record) {

    if (!snapshotCastInitialized) {
      snapshotCastInitialized = true;
      try {
        String castSpec = org.eclipse.microprofile.config.ConfigProvider.getConfig()
            .getOptionalValue("debezium.transforms.cast.spec", String.class)
            .orElse(null);
        if (castSpec != null && !castSpec.isEmpty()) {
          snapshotCastTransform = new org.apache.kafka.connect.transforms.Cast.Value<>();
          java.util.Map<String, String> castConfig = new java.util.HashMap<>();
          castConfig.put("spec", castSpec);
          snapshotCastTransform.configure(castConfig);
          LOGGER.info("Initialized Cast transform for snapshot records: {}", castSpec);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to initialize Cast transform for snapshot records: {}", e.getMessage());
      }
    }

    if (snapshotCastTransform != null) {
      return snapshotCastTransform.apply(record);
    }
    return record;
  }

  /**
   * Decode Incremental Snapshot notifications. Translates the source-side
   * collection identifier (e.g. "public.users") into the consumer-side
   * destination key (e.g. "cdc.public.users") and routes:
   * <ul>
   *   <li>{@code TABLE_SCAN_COMPLETED} → completed destinations passed to
   *       {@link #completeSnapshotTable};</li>
   *   <li>{@code IN_PROGRESS} → tracked in {@link #inProgressDirectCommit} so
   *       the status endpoint surfaces in-flight tables in the direct-commit
   *       path (where {@link #activeSnapshotWriters} is never populated).</li>
   * </ul>
   * Initial Snapshot also emits TABLE_SCAN_COMPLETED but with
   * {@code aggregate_type=Initial Snapshot} — filtered out so we don't mark a
   * table "completed" right after the initial LIMIT 10 chunk.
   */
  private static final String TABLE_SCAN_COMPLETED_NAME = SnapshotStatus.TABLE_SCAN_COMPLETED.name();
  private static final String IN_PROGRESS_NAME = SnapshotStatus.IN_PROGRESS.name();
  private static final String STARTED_NAME = SnapshotStatus.STARTED.name();

  private record ParsedNotification(String type, Map<String, String> additional) {}

  private ParsedNotification parseIncrementalNotification(EmbeddedEngineChangeEvent notification) {
    Object value = notification.value();
    String type;
    String aggregateType;
    Map<String, String> additional;
    if (value instanceof org.apache.kafka.connect.data.Struct struct) {
      try {
        type = struct.getString(Notification.TYPE);
        aggregateType = struct.getString(Notification.AGGREGATE_TYPE);
      } catch (Exception ignored) {
        return null;
      }
      Object raw;
      try {
        raw = struct.get(Notification.ADDITIONAL_DATA);
      } catch (Exception ignored) {
        return null;
      }
      if (!(raw instanceof Map)) {
        return null;
      }
      @SuppressWarnings("unchecked")
      Map<String, String> typed = (Map<String, String>) raw;
      additional = typed;
    } else if (value instanceof String json) {
      try {
        com.fasterxml.jackson.databind.JsonNode root = STATUS_MAPPER.readTree(json);
        com.fasterxml.jackson.databind.JsonNode payload = root.has("payload") ? root.get("payload") : root;
        type = payload.path(Notification.TYPE).asText(null);
        aggregateType = payload.path(Notification.AGGREGATE_TYPE).asText(null);
        com.fasterxml.jackson.databind.JsonNode addNode = payload.path(Notification.ADDITIONAL_DATA);
        if (!addNode.isObject()) {
          return null;
        }
        Map<String, String> parsed = new HashMap<>();
        addNode.fields().forEachRemaining(e -> parsed.put(e.getKey(), e.getValue().asText()));
        additional = parsed;
      } catch (Exception ignored) {
        return null;
      }
    } else {
      return null;
    }
    if (!IncrementalSnapshotNotificationService.INCREMENTAL_SNAPSHOT.equals(aggregateType)) {
      return null;
    }
    return new ParsedNotification(type, additional);
  }

  /**
   * Process the batch's notification records: mark in-progress tables (and
   * refresh their last-seen timestamp), and return the destinations whose
   * incremental scan has completed in this batch so the caller can finalize
   * them.
   */
  private Set<String> processIncrementalSnapshotNotifications(List<EmbeddedEngineChangeEvent> notifications) {
    if (notifications.isEmpty()) {
      return Collections.emptySet();
    }
    final String topicPrefix = config.debezium().topicPrefix();
    Set<String> completedDestinations = new LinkedHashSet<>();
    for (EmbeddedEngineChangeEvent notification : notifications) {
      ParsedNotification parsed = parseIncrementalNotification(notification);
      if (parsed == null) {
        continue;
      }
      if (TABLE_SCAN_COMPLETED_NAME.equals(parsed.type)) {
        String scannedCollection = parsed.additional.get(IncrementalSnapshotNotificationService.SCANNED_COLLECTION);
        if (scannedCollection != null && !scannedCollection.isBlank()) {
          completedDestinations.add(topicPrefix + "." + scannedCollection);
        }
      } else if (STARTED_NAME.equals(parsed.type)) {
        // STARTED carries data_collections (comma-joined). For signal-driven
        // single-table snapshots this is the canonical "table is now active"
        // signal — IN_PROGRESS is only emitted when a chunk completes mid-table
        // (i.e. multi-chunk tables larger than chunk.size).
        String dataCollections = parsed.additional.get(IncrementalSnapshotNotificationService.DATA_COLLECTIONS);
        if (dataCollections != null && !dataCollections.isBlank()) {
          for (String collection : dataCollections.split(IncrementalSnapshotNotificationService.LIST_DELIMITER)) {
            String trimmed = collection.trim();
            if (trimmed.isEmpty()) {
              continue;
            }
            String destination = topicPrefix + "." + trimmed;
            inProgressDirectCommit.computeIfAbsent(destination, k -> new DirectCommitInProgress(Instant.now()));
          }
        }
      } else if (IN_PROGRESS_NAME.equals(parsed.type)) {
        String currentCollection = parsed.additional.get(IncrementalSnapshotNotificationService.CURRENT_COLLECTION_IN_PROGRESS);
        if (currentCollection != null && !currentCollection.isBlank()) {
          String destination = topicPrefix + "." + currentCollection;
          DirectCommitInProgress entry = inProgressDirectCommit.computeIfAbsent(
              destination, k -> new DirectCommitInProgress(Instant.now()));
          entry.lastNotificationAt = Instant.now();
        }
      }
    }
    return completedDestinations;
  }

  /** Immutable snapshot of in-progress / completed tables, returned by {@link #getStatusSnapshot}. */
  public record InProgressSnapshot(
      String table,
      Instant startedAt,
      Instant lastDataAt,
      long rowsBufferedSoFar,
      boolean writerOpen) {
  }

  public record StatusSnapshot(
      List<CompletionInfo> completed,
      List<InProgressSnapshot> inProgress,
      Instant lastDataReceivedAt) {
  }

  /**
   * Returns an immutable, point-in-time view of the snapshot pipeline state
   * for the {@code /v1/snapshot-status/incremental} endpoint. Iterating the
   * underlying {@link ConcurrentHashMap}/{@link ConcurrentSkipListMap} is
   * weakly consistent, accepted at the 60s+ poll cadence of the resource.
   */
  public StatusSnapshot getStatusSnapshot() {
    List<CompletionInfo> completed = new ArrayList<>(completedSnapshotTables.values());
    List<InProgressSnapshot> inProgress = new ArrayList<>(activeSnapshotWriters.size() + inProgressDirectCommit.size());
    for (Map.Entry<String, StreamingSnapshotContext> entry : activeSnapshotWriters.entrySet()) {
      StreamingSnapshotContext ctx = entry.getValue();
      inProgress.add(new InProgressSnapshot(
          entry.getKey(),
          ctx.startedAt,
          ctx.lastDataReceivedAt,
          ctx.totalRowsWritten,
          ctx.writer != null));
    }
    for (Map.Entry<String, DirectCommitInProgress> entry : inProgressDirectCommit.entrySet()) {
      // Don't double-report tables that have an active streaming writer.
      if (activeSnapshotWriters.containsKey(entry.getKey())) {
        continue;
      }
      DirectCommitInProgress p = entry.getValue();
      DirectCommitCounter counter = directCommitCounters.get(entry.getKey());
      long rowsSoFar = counter == null ? 0L : counter.rows.get();
      inProgress.add(new InProgressSnapshot(
          entry.getKey(),
          p.startedAt,
          p.lastNotificationAt,
          rowsSoFar,
          false));
    }
    return new StatusSnapshot(completed, inProgress, lastDataReceivedAt);
  }

  // -------------------------------------------------------------------------
  // Snapshot status REST endpoint, registered on Quarkus's management
  // interface (default port 9000) alongside the health probes. Defined
  // inline (rather than in a separate JAX-RS resource) so the route handler
  // and the maps it reads live in the SAME @ApplicationScoped bean instance —
  // injecting the consumer into a separate resource was creating a second CDI
  // proxy, with the resource always observing an empty snapshot of the maps.
  // -------------------------------------------------------------------------

  private static final com.fasterxml.jackson.databind.ObjectMapper STATUS_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

  void registerStatusEndpoint(@jakarta.enterprise.event.Observes io.quarkus.vertx.http.ManagementInterface mi) {
    mi.router().get("/v1/snapshot-status/incremental").handler(this::handleStatusRequest);
    LOGGER.info("Registered snapshot-status route on management interface: GET /v1/snapshot-status/incremental");
  }

  private void handleStatusRequest(io.vertx.ext.web.RoutingContext ctx) {
    try {
      StatusSnapshot snapshot = getStatusSnapshot();
      java.util.Map<String, Object> root = new java.util.LinkedHashMap<>();
      java.util.Map<String, Object> incremental = new java.util.LinkedHashMap<>();
      java.util.List<java.util.Map<String, Object>> completedList = new java.util.ArrayList<>();
      Instant lastCompletionAt = null;
      long totalRowsCommitted = 0;
      for (CompletionInfo info : snapshot.completed()) {
        java.util.Map<String, Object> c = new java.util.LinkedHashMap<>();
        c.put("table", info.table());
        c.put("completedAt", info.completedAt().toString());
        c.put("rowsWritten", info.rowsWritten());
        c.put("filesCommitted", info.filesCommitted());
        c.put("bytesWritten", info.bytesWritten());
        completedList.add(c);
        if (lastCompletionAt == null || info.completedAt().isAfter(lastCompletionAt)) {
          lastCompletionAt = info.completedAt();
        }
        totalRowsCommitted += info.rowsWritten();
      }
      java.util.List<java.util.Map<String, Object>> inProgressList = new java.util.ArrayList<>();
      for (InProgressSnapshot ip : snapshot.inProgress()) {
        java.util.Map<String, Object> p = new java.util.LinkedHashMap<>();
        p.put("table", ip.table());
        p.put("startedAt", ip.startedAt().toString());
        p.put("lastDataAt", ip.lastDataAt().toString());
        p.put("rowsBufferedSoFar", ip.rowsBufferedSoFar());
        p.put("writerOpen", ip.writerOpen());
        inProgressList.add(p);
      }
      java.util.Map<String, Object> summary = new java.util.LinkedHashMap<>();
      summary.put("completedCount", completedList.size());
      summary.put("inProgressCount", inProgressList.size());
      summary.put("totalRowsCommitted", totalRowsCommitted);
      summary.put("lastCompletionAt", lastCompletionAt != null ? lastCompletionAt.toString() : null);
      summary.put("lastDataReceivedAt", snapshot.lastDataReceivedAt() != null ? snapshot.lastDataReceivedAt().toString() : null);
      incremental.put("completed", completedList);
      incremental.put("inProgress", inProgressList);
      incremental.put("summary", summary);
      root.put("incrementalSnapshot", incremental);
      root.put("asOf", Instant.now().toString());
      ctx.response()
          .putHeader("Content-Type", "application/json")
          .end(io.vertx.core.buffer.Buffer.buffer(STATUS_MAPPER.writeValueAsBytes(root)));
    }
    catch (Exception e) {
      LOGGER.warn("Failed to serialize snapshot status: {}", e.getMessage());
      ctx.response().setStatusCode(500).end(e.toString());
    }
  }

  /**
   * Complete the streaming snapshot for a table.
   * Closes the writer, commits to Iceberg, and removes the context.
   *
   * @param tableName Fully qualified table name
   */
  private static final String SCHEDULER_DONE_FLAG = "/tmp/scheduler_done.flag";
  private static final String CONSUMER_DONE_FLAG = "/tmp/consumer_done.flag";

  /**
   * Marks an incremental-snapshot table as completed in response to the
   * TABLE_SCAN_COMPLETED notification raised by Debezium core.
   *
   * <p>Two paths are reconciled here:
   * <ul>
   *   <li>If the sink kept a persistent writer for the table (streaming
   *       snapshot mode), the writer is committed and {@link CompletionInfo}
   *       carries the actual rows/files/bytes from the commit result.</li>
   *   <li>If the sink wrote chunks via {@code processTablesInParallel} /
   *       {@code processTablesSequentially} (commits issued per-batch, no
   *       persistent context in {@link #activeSnapshotWriters}), the data is
   *       already on Iceberg; the notification simply records the completion
   *       in {@link #completedSnapshotTables} so the status endpoint and the
   *       Python orchestrator can observe it. Counters are reported as 0
   *       because they are not aggregated cross-batch.</li>
   * </ul>
   */
  public void completeSnapshotTable(String tableName) {
    StreamingSnapshotContext ctx = activeSnapshotWriters.remove(tableName);

    long rowsWritten = 0;
    long bytesWritten = 0;
    int filesCommitted = 0;

    if (ctx != null) {
      try {
        if (ctx.writer == null) {
          LOGGER.info("No data written for table '{}' (all chunks were empty), skipping commit", tableName);
        } else {
          IcebergTableOperator.CommitResult result =
              icebergTableOperator.commitWriter(ctx.writer, ctx.icebergTable);
          rowsWritten = ctx.totalRowsWritten;
          bytesWritten = result.totalBytes;
          filesCommitted = result.fileCount;
          logConsumerProgress(ctx.totalRowsWritten);
          LOGGER.info("Completed streaming snapshot for table '{}': {} rows, {} files, {} MB committed",
              tableName, ctx.totalRowsWritten, result.fileCount, result.totalBytes / 1024 / 1024);
        }
      }
      catch (Exception e) {
        LOGGER.error("Failed to complete streaming snapshot for table '{}': {}",
            tableName, e.getMessage(), e);
        throw new RuntimeException("Failed to complete streaming snapshot for " + tableName, e);
      }
    } else {
      DirectCommitCounter counter = directCommitCounters.remove(tableName);
      if (counter != null) {
        rowsWritten = counter.rows.get();
        bytesWritten = counter.bytes.get();
        filesCommitted = counter.files.get();
      }
      inProgressDirectCommit.remove(tableName);
      LOGGER.info("Completed snapshot for table '{}' (direct-commit path; data already in Iceberg): {} rows, {} files, {} MB",
          tableName, rowsWritten, filesCommitted, bytesWritten / 1024 / 1024);
    }

    completedSnapshotTables.put(tableName,
        new CompletionInfo(tableName, Instant.now(), rowsWritten, filesCommitted, bytesWritten));

    checkAndWriteConsumerDoneFlag();
  }

  private static void accumulateDirectCommit(String tableName, IcebergTableOperator.CommitResult result) {
    if (result == null || tableName == null) {
      return;
    }
    DirectCommitCounter counter = directCommitCounters.computeIfAbsent(tableName, k -> new DirectCommitCounter());
    counter.rows.addAndGet(result.totalRecords);
    counter.bytes.addAndGet(result.totalBytes);
    counter.files.addAndGet(result.fileCount);
    DirectCommitInProgress inProgress = inProgressDirectCommit.get(tableName);
    if (inProgress != null) {
      inProgress.lastNotificationAt = Instant.now();
    }
  }

  private void checkAndWriteConsumerDoneFlag() {
    java.io.File consumerDone = new java.io.File(CONSUMER_DONE_FLAG);
    boolean snapshotActive = !activeSnapshotWriters.isEmpty() || hasIncrementalSnapshotInProgress();

    if (snapshotActive) {
      if (consumerDone.exists()) {
        consumerDone.delete();
        LOGGER.info("Incremental snapshot resumed — removed {}", CONSUMER_DONE_FLAG);
      }
      return;
    }

    if (consumerDone.exists()) {
      return;
    }
    java.io.File schedulerDone = new java.io.File(SCHEDULER_DONE_FLAG);
    if (!schedulerDone.exists()) {
      return;
    }
    flushAllBuffers();
    LOGGER.info("Post-snapshot buffer flush completed, writing consumer done flag");
    try {
      java.nio.file.Files.writeString(
          java.nio.file.Path.of(CONSUMER_DONE_FLAG),
          java.time.Instant.now().toString());
      LOGGER.info("All incremental snapshot data committed to Iceberg — wrote {}", CONSUMER_DONE_FLAG);
    } catch (java.io.IOException e) {
      LOGGER.warn("Failed to write consumer done flag: {}", e.getMessage());
    }
  }

  private boolean hasIncrementalSnapshotInProgress() {
    String offsetPath = org.eclipse.microprofile.config.ConfigProvider.getConfig()
        .getOptionalValue("debezium.source.offset.storage.file.filename", String.class)
        .orElse("/app/conf/data_offsets_history/offsets.dat");

    java.io.File offsetFile = new java.io.File(offsetPath);
    if (!offsetFile.exists()) {
      return false;
    }

    try {
      byte[] bytes = java.nio.file.Files.readAllBytes(offsetFile.toPath());
      String content = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);

      if (content.contains("\"incremental_snapshot_collections\":[{")) {
        LOGGER.debug("Consumer done check: offset file shows incremental snapshot still in progress");
        return true;
      }

      return false;
    } catch (java.io.IOException e) {
      LOGGER.warn("Failed to read offset file '{}': {}", offsetPath, e.getMessage());
      return true;
    }
  }

  /**
   * Flush a batch of snapshot tables together (v17 optimization).
   * Called by {@link BatchCommitCoordinator} when batch is ready.
   *
   * <p>This method writes all tables in the batch to a single Iceberg commit,
   * creating optimal 512MB Parquet files instead of many small files.
   *
   * @param batch List of completed tables to flush together
   */
  public void flushSnapshotBatch(List<BatchCommitCoordinator.CompletedTable> batch) {
    if (batch == null || batch.isEmpty()) {
      LOGGER.debug("Empty batch, nothing to flush");
      return;
    }

    int totalRecords = batch.stream().mapToInt(BatchCommitCoordinator.CompletedTable::getRecordCount).sum();
    long totalBytes = batch.stream().mapToLong(BatchCommitCoordinator.CompletedTable::getEstimatedBytes).sum();

    LOGGER.info("Flushing batch: {} tables, {} records, ~{} MB",
        batch.size(), totalRecords, totalBytes / 1024 / 1024);

    try {
      // Group tables by Iceberg destination
      Map<String, List<BatchCommitCoordinator.CompletedTable>> tablesByDestination = new java.util.HashMap<>();

      for (BatchCommitCoordinator.CompletedTable table : batch) {
        String destination = table.getTableName();
        tablesByDestination.computeIfAbsent(destination, k -> new ArrayList<>()).add(table);
      }

      // Process each destination
      for (Map.Entry<String, List<BatchCommitCoordinator.CompletedTable>> entry : tablesByDestination.entrySet()) {
        String destination = entry.getKey();
        List<BatchCommitCoordinator.CompletedTable> tablesForDestination = entry.getValue();

        // Merge all records for this destination
        List<org.apache.kafka.connect.source.SourceRecord> allRecords = new ArrayList<>();
        for (BatchCommitCoordinator.CompletedTable table : tablesForDestination) {
          allRecords.addAll(table.getRecords());
        }

        // Flush to Iceberg
        flushSnapshotTable(destination, allRecords);

        LOGGER.debug("Flushed {} tables to destination '{}' ({} records)",
            tablesForDestination.size(), destination, allRecords.size());
      }

      logConsumerProgress(totalRecords);
      LOGGER.info("Batch flush completed: {} unique destinations, {} total records",
          tablesByDestination.size(), totalRecords);
    }
    catch (Exception e) {
      LOGGER.error("❌ Batch flush failed: {}", e.getMessage(), e);
      throw new RuntimeException("Batch flush failed", e);
    }
  }

  @PreDestroy
  void close() {
    try {
      // Abort any open streaming snapshot writers (pod crash/shutdown during snapshot)
      if (!activeSnapshotWriters.isEmpty()) {
        LOGGER.warn("Aborting {} active streaming snapshot writers on shutdown",
            activeSnapshotWriters.size());
        for (Map.Entry<String, StreamingSnapshotContext> entry : activeSnapshotWriters.entrySet()) {
          try {
            entry.getValue().writer.abort();
            LOGGER.info("Aborted streaming writer for table '{}'", entry.getKey());
          } catch (Exception e) {
            LOGGER.warn("Failed to abort writer for '{}': {}", entry.getKey(), e.getMessage());
          }
        }
        activeSnapshotWriters.clear();
      }

      // Shutdown batch commit coordinator (flushes any remaining batch)
      if (batchCommitCoordinator != null) {
        batchCommitCoordinator.shutdown();
      }

      // Flush all remaining buffers before shutdown
      if (config.batch().bufferPerTableEnabled()) {
        LOGGER.info("Flushing all remaining table buffers before shutdown...");
        flushAllBuffers();

        if (periodicFlushExecutor != null) {
          LOGGER.info("Shutting down periodic flush executor.");
          periodicFlushExecutor.shutdown();
          if (!periodicFlushExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOGGER.warn("Periodic flush executor did not terminate in 10 seconds. Forcing shutdown.");
            periodicFlushExecutor.shutdownNow();
          }
        }

        if (memoryMonitorExecutor != null) {
          LOGGER.info("Shutting down memory monitor executor.");
          memoryMonitorExecutor.shutdown();
          if (!memoryMonitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.warn("Memory monitor executor did not terminate in 5 seconds. Forcing shutdown.");
            memoryMonitorExecutor.shutdownNow();
          }
        }
      }

      LOGGER.info("Shutting down executor service.");
      executor.shutdown();
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("Executor service did not terminate in 30 seconds. Forcing shutdown.");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted while shutting down executor service.");
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }

  }

  @Override
  public void handleBatch(List<EmbeddedEngineChangeEvent> records,
                          DebeziumEngine.RecordCommitter<EmbeddedEngineChangeEvent> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    // Notification records (Debezium core SinkNotificationChannel) are routed
    // out of the data pipeline. Split lazily: when the batch contains zero
    // notifications — the common case during streaming — no extra ArrayList is
    // allocated and `records` is reused as-is.
    List<EmbeddedEngineChangeEvent> dataRecords = records;
    List<EmbeddedEngineChangeEvent> notificationRecords = Collections.emptyList();
    if (notificationTopic != null) {
      for (int i = 0; i < records.size(); i++) {
        if (notificationTopic.equals(records.get(i).destination())) {
          dataRecords = new ArrayList<>(records.size() - 1);
          notificationRecords = new ArrayList<>();
          for (EmbeddedEngineChangeEvent e : records) {
            (notificationTopic.equals(e.destination()) ? notificationRecords : dataRecords).add(e);
          }
          break;
        }
      }
    }

    if (!dataRecords.isEmpty()) {
      lastDataReceivedAt = Instant.now();
    }

    // group events by destination (per iceberg table)
    Map<String, List<EventConverter>> result = dataRecords.stream()
        .map((EmbeddedEngineChangeEvent e) -> {
          return switch (keyValueChangeEventFormat) {
            case "json" -> new JsonEventConverter(e, config);
            case "connect" -> new StructEventConverter(e, config);
            default -> throw new DebeziumException("Unsupported format:" + keyValueChangeEventFormat);
          };
        })
        .collect(Collectors.groupingBy(EventConverter::destination));

    // Detect if snapshot is running by checking first event
    if (!result.isEmpty()) {
      List<EventConverter> firstTableEvents = result.values().iterator().next();
      if (!firstTableEvents.isEmpty()) {
        EventConverter firstEvent = firstTableEvents.get(0);
        // Check if this is a snapshot event (has "snapshot" field set to true/incremental)
        boolean wasSnapshotRunning = isSnapshotRunning;
        isSnapshotRunning = firstEvent.isSnapshotEvent();

        if (isSnapshotRunning && !wasSnapshotRunning) {
          LOGGER.info("🔵 Snapshot detected - switching to DIRECT processing mode (bypassing buffer)");
        } else if (!isSnapshotRunning && wasSnapshotRunning) {
          LOGGER.info("🟢 Snapshot completed - switching to BUFFER accumulation mode");
        }
      }
    }

    // Route data records to buffering or direct processing based on
    // configuration AND snapshot status.
    if (config.batch().bufferPerTableEnabled() && !isSnapshotRunning) {
      // Buffer accumulation mode (CDC streaming only)
      this.accumulateInBuffer(result);
    } else {
      // Direct processing mode (snapshot OR buffer disabled)
      if (isSnapshotRunning && config.batch().bufferPerTableEnabled()) {
        LOGGER.trace("Processing {} events directly (snapshot mode)", records.size());
      }

      if (numConcurrentUploads > 1) {
        this.processTablesInParallel(result);
      } else {
        this.processTablesSequentially(result);
      }
    }

    // Always process notifications, regardless of the data-routing branch above.
    // A batch can contain only notifications (no data records) — in that case
    // dataRecords is empty, isSnapshotRunning is not refreshed, and the buffer
    // branch would silently drop the TABLE_SCAN_COMPLETED signals if we kept
    // this loop inside the else block.
    for (String destination : processIncrementalSnapshotNotifications(notificationRecords)) {
      completeSnapshotTable(destination);
    }

    // workaround! somehow offset is not saved to file unless we call
    // committer.markProcessed per event
    // even it's should be saved to file periodically
    for (EmbeddedEngineChangeEvent record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(records.size());

    checkAndWriteConsumerDoneFlag();

    // waiting to group events as bathes
    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
  }

  /**
   * Processes events for each destination table sequentially in a single thread.
   *
   * @param eventsByDestination A map where keys are destination table names and
   *                            values are lists of events for that table.
   */
  private void processTablesSequentially(Map<String, List<EventConverter>> eventsByDestination) {
    for (Map.Entry<String, List<EventConverter>> tableEvents : eventsByDestination.entrySet()) {

      if (config.debezium().isHeartbeatTopic(tableEvents.getKey()) && config.debezium().topicHeartbeatSkipConsuming()) {
        continue;
      }

      Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0));
      IcebergTableOperator.CommitResult result =
          icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
      accumulateDirectCommit(tableEvents.getKey(), result);
    }
  }

  // In IcebergChangeConsumer.java

  /**
   * Processes events for each destination table in parallel using a virtual
   * thread pool.
   *
   * @param eventsByDestination A map where keys are destination table names and
   *                            values are lists of events for that table.
   */
  private void processTablesInParallel(Map<String, List<EventConverter>> eventsByDestination) {
    List<Callable<Void>> tasks = new ArrayList<>();
    for (Map.Entry<String, List<EventConverter>> tableEvents : eventsByDestination.entrySet()) {

      if (config.debezium().isHeartbeatTopic(tableEvents.getKey()) && config.debezium().topicHeartbeatSkipConsuming()) {
        continue;
      }

      tasks.add(() -> {
        try {
          // Acquire a permit from the Semaphore to enforce the concurrency limit.
          LOGGER.trace("Task for destination '{}' waiting for permit. Available: {}", tableEvents.getKey(),
              concurrencyLimiter.availablePermits());
          concurrencyLimiter.acquire();
          LOGGER.debug("Task for destination '{}' acquired permit. Starting processing.", tableEvents.getKey());

          Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()),
              tableEvents.getValue().get(0));
          IcebergTableOperator.CommitResult result =
              icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
          accumulateDirectCommit(tableEvents.getKey(), result);
          return null; // Callable must return a value
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Restore interrupted status
          LOGGER.warn("Task for destination '{}' was interrupted.", tableEvents.getKey(), e);
          return null;
        } catch (Exception e) {
          // Log and rethrow. This will be wrapped in an ExecutionException by the Future.
          LOGGER.error("Task for destination '{}' failed.", tableEvents.getKey(), e);
          throw e;
        } finally {
          // Always release the permit, even if an exception occurred.
          concurrencyLimiter.release();
          LOGGER.trace("Task for destination '{}' released permit. Available: {}", tableEvents.getKey(),
              concurrencyLimiter.availablePermits());
        }
      });
    }

    LOGGER.debug("Invoking {} parallel tasks and waiting for completion...", tasks.size());
    try {
      // Invoke all tasks and wait for them to complete, with a timeout.
      List<Future<Void>> futures = executor.invokeAll(tasks, concurrentUploadsTimeoutMinutes, TimeUnit.MINUTES);

      // FIX FOR CRITICAL BUG: Track failures and re-throw to prevent data loss
      // See: DEBEZIUM-SERVER-ICEBERG-SILENT-DATA-LOSS-BUG.md
      boolean hasFailures = false;
      DebeziumException firstException = null;
      int failureCount = 0;

      // Check the status of each task
      for (Future<Void> future : futures) {
        try {
          // future.get() will throw an exception if the task failed or was cancelled.
          future.get();
        } catch (CancellationException e) {
          hasFailures = true;
          failureCount++;
          LOGGER.error("A task was cancelled, likely due to timeout.", e);
          if (firstException == null) {
            firstException = new DebeziumException("Task cancelled due to timeout", e);
          }
        } catch (ExecutionException e) {
          hasFailures = true;
          failureCount++;
          // The original exception from the Callable is wrapped in ExecutionException
          LOGGER.error("A task failed with an exception: {}", e.getCause().getMessage(), e.getCause());
          if (firstException == null) {
            firstException = new DebeziumException("Parallel table processing failed", e.getCause());
          }
        }
      }

      // CRITICAL FIX: Re-throw exception if any task failed
      // This prevents Debezium from committing offsets when writes fail,
      // allowing retry and preventing permanent data loss
      if (hasFailures) {
        LOGGER.error("❌ CRITICAL: {} out of {} parallel tasks failed. Aborting batch to prevent data loss.",
            failureCount, tasks.size());
        throw firstException;
      }

      LOGGER.debug("✅ All {} parallel tasks completed successfully.", tasks.size());

    } catch (InterruptedException e) {
      LOGGER.warn("Main thread interrupted while waiting for tasks to complete.", e);
      Thread.currentThread().interrupt();
      throw new DebeziumException("Parallel processing interrupted", e);
    }
  }

  /**
   * @param tableId     iceberg table identifier
   * @param sampleEvent sample debezium event. event schema used to create iceberg
   *                    table when table not found
   * @return iceberg table, throws RuntimeException when table not found, and it's
   * not possible to create it
   */
  public Table loadIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId)
        .orElseGet(() -> this.createIcebergTable(tableId, sampleEvent));
  }

  private Table createIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {
    if (!config.debezium().eventSchemaEnabled()
        && !Objects.equals(config.debezium().keyValueChangeEventFormat(), "connect")) {
      throw new RuntimeException("Table '" + tableId + "' not found! "
          + "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
    }
    try {
      final Schema schema;
      final SortOrder sortOrder;
      if (!config.iceberg().createIdentifierFields()) {
        LOGGER.warn("Creating identifier fields is disabled, creating schema without identifier fields!");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else if (config.iceberg().nestedAsVariant()) {
        LOGGER.warn(
            "Identifier fields are not supported when data consumed to variant fields, creating schema without identifier fields!");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else if (sampleEvent.isSchemaChangeEvent()) {
        // Check if the message is a schema change event (DDL statement).
        // Schema change events are identified by the presence of "ddl", "databaseName",
        // and "tableChanges" fields.
        // "schema change topic"
        // https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-schema-change-topic
        LOGGER.warn("Creating schema change topic/table without identifier fields for append-only mode.");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else if (config.debezium().isHeartbeatTopic(sampleEvent.destination())) {
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else {
        schema = sampleEvent.icebergSchema(true);
        sortOrder = sampleEvent.sortOrder(schema);
      }

      final List<String> partitionByOptions = config.iceberg().partitionByForTable(sampleEvent.destination());
      PartitionSpec spec = IcebergUtil.createPartitionSpec(schema, partitionByOptions);

      // for backward compatibility, to be removed and set to "3" with one of the next
      // releases
      // Format 3 will be used when variant data type is used
      final String tableFormatVersion = config.iceberg().nestedAsVariant() ? "3" : "2";
      return IcebergUtil.createIcebergTable(icebergCatalog, tableId, schema, spec, sortOrder,
          config.iceberg().writeFormat(), tableFormatVersion);
    } catch (Exception e) {
      throw new DebeziumException(
          "Failed to create table from debezium event table:" + tableId + " Error:" + e.getMessage(), e);
    }
  }

  /**
   * periodically log number of events consumed
   *
   * @param numUploadedEvents number of events consumed
   */
  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents,
          Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  // ============================================
  // PER-TABLE BUFFER ACCUMULATION METHODS
  // ============================================

  /**
   * Accumulate events in per-table buffers and flush when threshold is reached.
   */
  private void accumulateInBuffer(Map<String, List<EventConverter>> eventsByDestination) {
    for (Map.Entry<String, List<EventConverter>> tableEvents : eventsByDestination.entrySet()) {
      String tableName = tableEvents.getKey();
      List<EventConverter> events = tableEvents.getValue();

      // Skip heartbeat topics if configured
      if (config.debezium().isHeartbeatTopic(tableName) && config.debezium().topicHeartbeatSkipConsuming()) {
        continue;
      }

      // Add events to buffer
      tableEventBuffer.compute(tableName, (k, existingList) -> {
        if (existingList == null) {
          existingList = new ArrayList<>();
          lastFlushTime.put(tableName, System.currentTimeMillis());
        }
        existingList.addAll(events);
        return existingList;
      });

      totalBufferedEvents.addAndGet(events.size());

      // Check if this table's buffer reached threshold
      List<EventConverter> currentBuffer = tableEventBuffer.get(tableName);
      if (currentBuffer.size() >= config.batch().bufferPerTableThreshold()) {
        LOGGER.debug("Table '{}' buffer reached threshold ({} events). Flushing...",
            tableName, currentBuffer.size());
        flushTableBuffer(tableName);
      }
    }

    // Check if total number of tables exceeded limit
    checkBufferLimits();
  }

  /**
   * Check if buffer limits are exceeded and flush largest tables if needed.
   */
  private void checkBufferLimits() {
    int maxTables = config.batch().bufferPerTableMaxTables();
    if (tableEventBuffer.size() > maxTables) {
      LOGGER.warn("Buffer table count ({}) exceeded max limit ({}). Flushing largest tables...",
          tableEventBuffer.size(), maxTables);

      // Find tables with most events and flush them
      tableEventBuffer.entrySet().stream()
          .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
          .limit(tableEventBuffer.size() - maxTables + 10) // flush 10 extra to create headroom
          .forEach(entry -> {
            LOGGER.info("Flushing table '{}' with {} buffered events (max tables limit)",
                entry.getKey(), entry.getValue().size());
            flushTableBuffer(entry.getKey());
          });
    }
  }

  /**
   * Check memory pressure and trigger aggressive flush if heap usage > 80%.
   * Called by memory monitor ScheduledExecutorService.
   */
  private void checkMemoryPressureAndFlush() {
    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();     // Max heap size
    long totalMemory = runtime.totalMemory(); // Current heap size
    long freeMemory = runtime.freeMemory();   // Free memory in current heap
    long usedMemory = totalMemory - freeMemory;

    double usagePercentage = (double) usedMemory / maxMemory * 100;

    // Log memory stats periodically
    LOGGER.debug("Memory usage: {}/{} MB ({:.1f}%), buffered events: {}",
        usedMemory / 1024 / 1024,
        maxMemory / 1024 / 1024,
        usagePercentage,
        totalBufferedEvents.get());

    // Aggressive flush if memory pressure > 80%
    if (usagePercentage > 80.0) {
      long bufferedCount = totalBufferedEvents.get();
      int tableCount = tableEventBuffer.size();

      // Only log if there are actually events to flush (avoid spam when buffer is empty)
      if (bufferedCount > 0 || tableCount > 0) {
        LOGGER.warn("⚠️ HIGH MEMORY PRESSURE: {:.1f}% heap usage ({} MB used / {} MB max)",
            usagePercentage,
            usedMemory / 1024 / 1024,
            maxMemory / 1024 / 1024);
        LOGGER.warn("⚠️ Triggering AGGRESSIVE FLUSH: {} buffered events across {} tables",
            bufferedCount, tableCount);
      }

      flushAllBuffers();

      // Suggest GC (hint only, JVM decides)
      System.gc();

      // Only log completion if we actually flushed something
      if (bufferedCount > 0 || tableCount > 0) {
        LOGGER.info("✅ Aggressive flush completed. Remaining buffered events: {}",
            totalBufferedEvents.get());
      }
    }
  }

  /**
   * Periodic flush for tables that haven't been flushed within timeout.
   * Called by ScheduledExecutorService.
   */
  private void flushTimedOutBuffers() {
    long now = System.currentTimeMillis();
    long flushIntervalMs = config.batch().bufferPerTableFlushIntervalMs();

    List<String> tablesToFlush = new ArrayList<>();
    for (Map.Entry<String, Long> entry : lastFlushTime.entrySet()) {
      String tableName = entry.getKey();
      long lastFlush = entry.getValue();
      if (now - lastFlush >= flushIntervalMs && tableEventBuffer.containsKey(tableName)) {
        tablesToFlush.add(tableName);
      }
    }

    if (!tablesToFlush.isEmpty()) {
      LOGGER.info("Periodic flush: {} tables timed out (interval: {}ms)",
          tablesToFlush.size(), flushIntervalMs);
      for (String tableName : tablesToFlush) {
        int bufferSize = tableEventBuffer.getOrDefault(tableName, List.of()).size();
        LOGGER.debug("Flushing timed-out table '{}' with {} buffered events", tableName, bufferSize);
        flushTableBuffer(tableName);
      }
    }
  }

  /**
   * Flush all remaining buffers (used during shutdown).
   */
  private void flushAllBuffers() {
    List<String> tables = new ArrayList<>(tableEventBuffer.keySet());
    LOGGER.debug("Flushing all remaining buffers for {} tables...", tables.size());
    for (String tableName : tables) {
      int bufferSize = tableEventBuffer.getOrDefault(tableName, List.of()).size();
      if (bufferSize > 0) {
        LOGGER.info("Shutdown flush: table '{}' with {} buffered events", tableName, bufferSize);
        flushTableBuffer(tableName);
      }
    }
  }

  /**
   * Flush a single table's buffer with retry logic for transient errors.
   */
  private void flushTableBuffer(String tableName) {
    List<EventConverter> events = tableEventBuffer.remove(tableName);
    if (events == null || events.isEmpty()) {
      return;
    }

    totalBufferedEvents.addAndGet(-events.size());
    lastFlushTime.put(tableName, System.currentTimeMillis());

    int maxAttempts = config.batch().bufferPerTableRetryMaxAttempts();
    long initialDelayMs = config.batch().bufferPerTableRetryInitialDelayMs();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Load Iceberg table and commit events
        Table icebergTable = this.loadIcebergTable(mapDestination(tableName), events.get(0));
        icebergTableOperator.addToTable(icebergTable, events);

        LOGGER.info("Successfully flushed {} events to table '{}' (attempt {}/{})",
            events.size(), tableName, attempt, maxAttempts);
        return; // Success!

      } catch (Exception e) {
        boolean isRetryable = isRetryableException(e);
        boolean isLastAttempt = (attempt == maxAttempts);

        if (isRetryable && !isLastAttempt) {
          long delayMs = (long) (initialDelayMs * Math.pow(2, attempt - 1));
          LOGGER.warn("Flush failed for table '{}' (attempt {}/{}). Retrying in {}ms. Error: {}",
              tableName, attempt, maxAttempts, delayMs, e.getMessage());

          try {
            Thread.sleep(delayMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.error("Retry interrupted for table '{}'. Failing flush.", tableName);
            throw new DebeziumException("Flush interrupted for table: " + tableName, ie);
          }
        } else {
          // Non-retryable error OR last attempt exhausted
          LOGGER.error("FATAL: Failed to flush {} events to table '{}' after {} attempts. Events LOST!",
              events.size(), tableName, attempt, e);
          throw new DebeziumException("Failed to flush table after " + attempt + " attempts: " + tableName, e);
        }
      }
    }
  }

  /**
   * Determine if an exception is retryable (transient network/GCS errors).
   */
  private boolean isRetryableException(Throwable e) {
    if (e == null) {
      return false;
    }

    String message = e.getMessage();
    String className = e.getClass().getName();

    // GCS StorageException with transient errors
    if (className.contains("StorageException")) {
      if (message != null && (
          message.contains("Remote host terminated the handshake") ||
          message.contains("Broken pipe") ||
          message.contains("Connection reset") ||
          message.contains("Error writing request body") ||
          message.contains("Unknown Error") ||
          message.contains("503") || // Service unavailable
          message.contains("429")    // Rate limited
      )) {
        return true;
      }
    }

    // SSL/TLS handshake errors
    if (className.contains("SSLHandshakeException") || className.contains("SSLException")) {
      return true;
    }

    // Network I/O errors
    if (className.contains("SocketException") || className.contains("IOException")) {
      if (message != null && (
          message.contains("Broken pipe") ||
          message.contains("Connection reset") ||
          message.contains("Connection refused")
      )) {
        return true;
      }
    }

    // Check nested causes recursively
    return isRetryableException(e.getCause());
  }

  public TableIdentifier mapDestination(String destination) {
    return tableMapper.mapDestination(destination);
  }
}
