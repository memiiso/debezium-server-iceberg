/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import io.debezium.server.iceberg.batchsizewait.BatchSizeWait;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.converter.StructEventConverter;
import io.debezium.server.iceberg.converter.geospatial.GeospatialTypes;
import io.debezium.server.iceberg.mapper.IcebergTableMapper;
import io.debezium.server.iceberg.offset.IcebergOffsetBackingStore;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.server.iceberg.tableoperator.MultiTableCommitter;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  protected final Clock clock = Clock.system();
  final Configuration hadoopConf = new Configuration();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  protected static String keyValueChangeEventFormat;

  @Inject @Any Instance<BatchSizeWait> batchSizeWaitInstances;
  BatchSizeWait batchSizeWait;
  Catalog icebergCatalog;
  @Inject IcebergTableOperator icebergTableOperator;
  @Inject GlobalConfig config;

  @Inject @Any Instance<IcebergTableMapper> tableMappers;
  IcebergTableMapper tableMapper;
  private int numConcurrentUploads;
  private int concurrentUploadsTimeoutMinutes;
  ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private Semaphore concurrencyLimiter;
  // set when tables and offset are committed in one transaction
  private MultiTableCommitter multiTableCommitter;

  @PostConstruct
  void connect() {
    config.debezium().validateTemporalPrecisionMode();
    GeospatialTypes.validateConfig(config.iceberg());

    JsonEventConverter.initializeStaticSerdes();
    keyValueChangeEventFormat = config.debezium().keyValueChangeEventFormat();
    LOGGER.info(
        "IcebergChangeConsumer is configured to use the '{}' format for processing events.",
        keyValueChangeEventFormat);
    // pass iceberg properties to iceberg and hadoop
    config.iceberg().icebergConfigs().forEach(this.hadoopConf::set);

    icebergCatalog =
        CatalogUtil.buildIcebergCatalog(
            config.iceberg().catalogName(), config.iceberg().icebergConfigs(), hadoopConf);
    batchSizeWait =
        IcebergUtil.selectInstance(batchSizeWaitInstances, config.batch().batchSizeWaitName());
    batchSizeWait.initizalize();
    tableMapper = IcebergUtil.selectInstance(tableMappers, config.iceberg().tableMapper());

    this.numConcurrentUploads = config.batch().concurrentUploads();
    this.concurrentUploadsTimeoutMinutes = config.batch().concurrentUploadsTimeoutMinutes();

    // Initialize the semaphore for parallel processing
    if (numConcurrentUploads > 1) {
      this.concurrencyLimiter = new Semaphore(numConcurrentUploads);
      LOGGER.info("Parallel uploads enabled with concurrency limit: {}", numConcurrentUploads);
    }

    if (config.iceberg().atomicOffsetCommit()) {
      if (!(icebergCatalog instanceof RESTCatalog restCatalog)) {
        throw new DebeziumException(
            "`debezium.sink.iceberg.atomic-offset-commit` requires a REST catalog supporting multi-table transactions, got: "
                + icebergCatalog.getClass().getName());
      }
      if (!IcebergOffsetBackingStore.class.getName().equals(config.debezium().offsetStorage())) {
        throw new DebeziumException(
            "`debezium.sink.iceberg.atomic-offset-commit` requires `debezium.source.offset.storage` to be "
                + IcebergOffsetBackingStore.class.getName());
      }
      multiTableCommitter =
          new MultiTableCommitter(restCatalog, config.iceberg().atomicOffsetCommitRetries());
      LOGGER.info(
          "Atomic offset commit enabled, tables and offset are committed in one transaction");
    }
  }

  @PreDestroy
  void close() {
    try {
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
  public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {
    Instant start = Instant.now();

    List<EventConverter> converted =
        events.records().stream()
            .<EventConverter>map(
                (BatchEvent e) -> {
                  return switch (keyValueChangeEventFormat) {
                    case "json" -> new JsonEventConverter(e, config);
                    case "connect" -> new StructEventConverter(e, config);
                    default ->
                        throw new DebeziumException(
                            "Unsupported format:" + keyValueChangeEventFormat);
                  };
                })
            .filter(
                e ->
                    !(config.debezium().isHeartbeatTopic(e.destination())
                        && config.debezium().topicHeartbeatSkipConsuming()))
            .toList();

    if (multiTableCommitter != null) {
      this.commitAtomically(converted, events.records());
    } else {
      // group events by destination (per iceberg table)
      Map<String, List<EventConverter>> result =
          converted.stream()
              .collect(
                  Collectors.groupingBy(
                      EventConverter::destination, LinkedHashMap::new, Collectors.toList()));

      // consume list of events for each destination table
      this.processTables(
          result,
          this::mapDestination,
          (tableId, icebergTable, tableEvents) ->
              icebergTableOperator.addToTable(icebergTable, tableEvents));
    }

    for (BatchEvent record : events.records()) {
      LOGGER.trace("Processed event '{}'", record);
      record.commit();
    }
    this.logConsumerProgress(events.records().size());

    // waiting to group events as batches
    batchSizeWait.waitMs(
        events.records().size(), (int) Duration.between(start, Instant.now()).toMillis());
  }

  /**
   * Writes the events of all the destination tables, then commits the tables and the Debezium
   * offset of the batch in one transaction.
   *
   * <p>Events are grouped by destination table, topics mapped to the same table are committed
   * together. When the commit fails nothing is committed, and the batch is replayed from the
   * previous offset on restart.
   *
   * @param events events to write, heartbeat events already filtered out when not consumed
   * @param records all the records of the batch, their offsets are committed
   */
  private void commitAtomically(List<EventConverter> events, List<BatchEvent> records)
      throws InterruptedException {
    IcebergOffsetBackingStore offsetStore =
        IcebergOffsetBackingStore.instance()
            .orElseThrow(() -> new DebeziumException("Iceberg offset storage is not started!"));
    String sinkCatalogUri = config.iceberg().icebergConfigs().get("uri");
    if (!Objects.equals(offsetStore.catalogUri(), sinkCatalogUri)) {
      throw new DebeziumException(
          "Offset storage catalog `"
              + offsetStore.catalogUri()
              + "` must be the sink catalog `"
              + sinkCatalogUri
              + "` to commit the offset atomically with the data");
    }

    Map<TableIdentifier, List<EventConverter>> eventsByTable =
        events.stream()
            .collect(
                Collectors.groupingBy(
                    e -> mapDestination(e.destination()), LinkedHashMap::new, Collectors.toList()));

    Map<TableIdentifier, MultiTableCommitter.PendingTable> pendingTables =
        new ConcurrentHashMap<>();
    IcebergOffsetBackingStore.PendingOffset pendingOffset = null;
    try {
      this.processTables(
          eventsByTable,
          Function.identity(),
          (tableId, icebergTable, tableEvents) ->
              pendingTables.put(
                  tableId,
                  new MultiTableCommitter.PendingTable(
                      tableId,
                      icebergTable,
                      icebergTableOperator.writeToTable(icebergTable, tableEvents),
                      tableEvents.size())));
      pendingOffset =
          offsetStore.prepare(
              records.stream().map(BatchEvent::record).filter(Objects::nonNull).toList());
    } catch (InterruptedException | RuntimeException e) {
      multiTableCommitter.abort(
          pendingTables.values(),
          offsetStore.tableIdentifier(),
          pendingOffset == null ? null : pendingOffset.files());
      throw e;
    }

    multiTableCommitter.commit(
        pendingTables.values(), offsetStore.tableIdentifier(), pendingOffset.files());
    offsetStore.committed(pendingOffset);
    pendingTables.values().forEach(p -> icebergTableOperator.afterCommit(p.table(), p.numEvents()));
  }

  /** Writes the events of one destination table. */
  @FunctionalInterface
  private interface TableWriter {
    void write(TableIdentifier tableId, Table icebergTable, List<EventConverter> events);
  }

  /**
   * Processes events for each destination table, in parallel when concurrent uploads are enabled.
   *
   * @param eventsByDestination events grouped by destination
   * @param toTableId maps the destination to the iceberg table identifier
   * @param tableWriter writes the events of one table
   */
  private <K> void processTables(
      Map<K, List<EventConverter>> eventsByDestination,
      Function<K, TableIdentifier> toTableId,
      TableWriter tableWriter)
      throws InterruptedException {
    if (numConcurrentUploads > 1) {
      this.processTablesInParallel(eventsByDestination, toTableId, tableWriter);
    } else {
      this.processTablesSequentially(eventsByDestination, toTableId, tableWriter);
    }
  }

  /**
   * Processes events for each destination table sequentially in a single thread.
   *
   * @param eventsByDestination A map where keys are destinations and values are lists of events for
   *     that destination.
   * @param toTableId maps the destination to the iceberg table identifier
   * @param tableWriter writes the events of one table
   */
  private <K> void processTablesSequentially(
      Map<K, List<EventConverter>> eventsByDestination,
      Function<K, TableIdentifier> toTableId,
      TableWriter tableWriter) {
    for (Map.Entry<K, List<EventConverter>> tableEvents : eventsByDestination.entrySet()) {
      TableIdentifier tableId = toTableId.apply(tableEvents.getKey());
      Table icebergTable = this.loadIcebergTable(tableId, tableEvents.getValue().get(0));
      tableWriter.write(tableId, icebergTable, tableEvents.getValue());
    }
  }

  /**
   * Processes events for each destination table in parallel using a virtual thread pool.
   *
   * @param eventsByDestination A map where keys are destinations and values are lists of events for
   *     that destination.
   * @param toTableId maps the destination to the iceberg table identifier
   * @param tableWriter writes the events of one table
   */
  private <K> void processTablesInParallel(
      Map<K, List<EventConverter>> eventsByDestination,
      Function<K, TableIdentifier> toTableId,
      TableWriter tableWriter)
      throws InterruptedException {
    List<Callable<Void>> tasks = new ArrayList<>();
    for (Map.Entry<K, List<EventConverter>> tableEvents : eventsByDestination.entrySet()) {

      tasks.add(
          () -> {
            boolean acquired = false;
            try {
              // Acquire a permit from the Semaphore to enforce the concurrency limit.
              LOGGER.trace(
                  "Task for destination '{}' waiting for permit. Available: {}",
                  tableEvents.getKey(),
                  concurrencyLimiter.availablePermits());
              concurrencyLimiter.acquire();
              acquired = true;
              LOGGER.debug(
                  "Task for destination '{}' acquired permit. Starting processing.",
                  tableEvents.getKey());

              TableIdentifier tableId = toTableId.apply(tableEvents.getKey());
              Table icebergTable = this.loadIcebergTable(tableId, tableEvents.getValue().get(0));
              tableWriter.write(tableId, icebergTable, tableEvents.getValue());
              return null; // Callable must return a value
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt(); // Restore interrupted status
              LOGGER.warn("Task for destination '{}' was interrupted.", tableEvents.getKey(), e);
              throw e;
            } catch (Exception e) {
              // Log and rethrow. This will be wrapped in an ExecutionException by the Future.
              LOGGER.error("Task for destination '{}' failed.", tableEvents.getKey(), e);
              throw e;
            } finally {
              // Always release the permit if acquired, even if an exception occurred.
              if (acquired) {
                concurrencyLimiter.release();
                LOGGER.trace(
                    "Task for destination '{}' released permit. Available: {}",
                    tableEvents.getKey(),
                    concurrencyLimiter.availablePermits());
              }
            }
          });
    }

    LOGGER.debug("Invoking {} parallel tasks and waiting for completion...", tasks.size());
    try {
      // Invoke all tasks and wait for them to complete, with a timeout.
      List<Future<Void>> futures =
          executor.invokeAll(tasks, concurrentUploadsTimeoutMinutes, TimeUnit.MINUTES);

      // Track failures and re-throw to prevent silent data loss.
      // Without this, a failed parallel task is only logged: the loop exits normally,
      // Debezium considers the batch successful, and commits the source offset.
      // The data for the failed table is permanently lost.
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
          LOGGER.error(
              "A task failed with an exception: {}", e.getCause().getMessage(), e.getCause());
          if (firstException == null) {
            firstException =
                new DebeziumException("Parallel table processing failed", e.getCause());
          }
        }
      }

      // Re-throw if any task failed: prevents Debezium from committing the source offset
      // when at least one table write did not succeed. The whole batch is retried, which
      // is the correct behavior since offsets are committed at batch granularity.
      if (hasFailures) {
        LOGGER.error(
            "CRITICAL: {} out of {} parallel tasks failed. Aborting batch to prevent data loss.",
            failureCount,
            tasks.size());
        throw firstException;
      }
      LOGGER.debug("All parallel tasks have been processed.");

    } catch (InterruptedException e) {
      LOGGER.warn("Main thread interrupted while waiting for tasks to complete.", e);
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /**
   * @param tableId iceberg table identifier
   * @param sampleEvent sample debezium event. event schema used to create iceberg table when table
   *     not found
   * @return iceberg table, throws RuntimeException when table not found, and it's not possible to
   *     create it
   */
  public Table loadIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId)
        .orElseGet(() -> this.createIcebergTable(tableId, sampleEvent));
  }

  private Table createIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {
    if (!config.debezium().eventSchemaEnabled()
        && !Objects.equals(config.debezium().keyValueChangeEventFormat(), "connect")) {
      throw new RuntimeException(
          "Table '"
              + tableId
              + "' not found! "
              + "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
    }
    try {
      final Schema schema;
      final SortOrder sortOrder;
      if (!config.iceberg().createIdentifierFields()) {
        LOGGER.warn(
            "Creating identifier fields is disabled, creating schema without identifier fields!");
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
        LOGGER.warn(
            "Creating schema change topic/table without identifier fields for append-only mode.");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else if (config.debezium().isHeartbeatTopic(sampleEvent.destination())) {
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else {
        schema = sampleEvent.icebergSchema(true);
        sortOrder = sampleEvent.sortOrder(schema);
      }

      final List<String> partitionByOptions =
          config.iceberg().partitionByForTable(sampleEvent.destination());
      final boolean isTableSpecific =
          config.iceberg().isPartitionByTableSpecific(sampleEvent.destination());
      PartitionSpec spec =
          IcebergUtil.createPartitionSpec(schema, partitionByOptions, isTableSpecific);

      // for backward compatibility, to be removed and set to "3" with one of the next
      // releases
      // Format 3 will be used when variant data type is used
      final String tableFormatVersion =
          config.iceberg().nestedAsVariant() ? "3" : config.iceberg().formatVersion();
      return IcebergUtil.createIcebergTable(
          icebergCatalog,
          tableId,
          schema,
          spec,
          sortOrder,
          config.iceberg().writeFormat(),
          tableFormatVersion);
    } catch (Exception e) {
      throw new DebeziumException(
          "Failed to create table from debezium event table:"
              + tableId
              + " Error:"
              + e.getMessage(),
          e);
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
      LOGGER.info(
          "Consumed {} records after {}",
          numConsumedEvents,
          Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  public TableIdentifier mapDestination(String destination) {
    return tableMapper.mapDestination(destination);
  }
}
