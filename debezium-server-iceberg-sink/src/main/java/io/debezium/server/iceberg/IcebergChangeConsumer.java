/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.DebeziumEngine;
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
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer implements DebeziumEngine.ChangeConsumer<EmbeddedEngineChangeEvent> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
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
  GlobalConfig config;

  @Inject
  @Any
  Instance<IcebergTableMapper> tableMappers;
  IcebergTableMapper tableMapper;
  private int numConcurrentUploads;
  private int concurrentUploadsTimeoutMinutes;
  ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private Semaphore concurrencyLimiter;

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

    // Initialize the semaphore for parallel processing
    if (numConcurrentUploads > 1) {
      this.concurrencyLimiter = new Semaphore(numConcurrentUploads);
      LOGGER.info("Parallel uploads enabled with concurrency limit: {}", numConcurrentUploads);
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
  public void handleBatch(List<EmbeddedEngineChangeEvent> records, DebeziumEngine.RecordCommitter<EmbeddedEngineChangeEvent> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    //group events by destination (per iceberg table)
    Map<String, List<EventConverter>> result =
        records.stream()
            .map((EmbeddedEngineChangeEvent e)
                    -> {
                  return switch (keyValueChangeEventFormat) {
                    case "json" -> new JsonEventConverter(e, config);
                    case "connect" -> new StructEventConverter(e, config);
                    default -> throw new DebeziumException("Unsupported format:" + keyValueChangeEventFormat);
                  };
                }
            )
            .collect(Collectors.groupingBy(EventConverter::destination));

    // consume list of events for each destination table
    if (numConcurrentUploads > 1) {
      this.processTablesInParallel(result);
    } else {
      this.processTablesSequentially(result);
    }

    // workaround! somehow offset is not saved to file unless we call committer.markProcessed per event
    // even it's should be saved to file periodically
    for (EmbeddedEngineChangeEvent record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(records.size());

    // waiting to group events as bathes
    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
  }

  /**
   * Processes events for each destination table sequentially in a single thread.
   *
   * @param eventsByDestination A map where keys are destination table names and values are lists of events for that table.
   */
  private void processTablesSequentially(Map<String, List<EventConverter>> eventsByDestination) {
    for (Map.Entry<String, List<EventConverter>> tableEvents : eventsByDestination.entrySet()) {

      if (config.debezium().isHeartbeatTopic(tableEvents.getKey()) && config.debezium().topicHeartbeatSkipConsuming()) {
        continue;
      }

      Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0));
      icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
    }
  }


  // In IcebergChangeConsumer.java

  /**
   * Processes events for each destination table in parallel using a virtual thread pool.
   *
   * @param eventsByDestination A map where keys are destination table names and values are lists of events for that table.
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
          LOGGER.trace("Task for destination '{}' waiting for permit. Available: {}", tableEvents.getKey(), concurrencyLimiter.availablePermits());
          concurrencyLimiter.acquire();
          LOGGER.debug("Task for destination '{}' acquired permit. Starting processing.", tableEvents.getKey());

          Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0));
          icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
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
          LOGGER.trace("Task for destination '{}' released permit. Available: {}", tableEvents.getKey(), concurrencyLimiter.availablePermits());
        }
      });
    }

    LOGGER.debug("Invoking {} parallel tasks and waiting for completion...", tasks.size());
    try {
      // Invoke all tasks and wait for them to complete, with a timeout.
      List<Future<Void>> futures = executor.invokeAll(tasks, concurrentUploadsTimeoutMinutes, TimeUnit.MINUTES);

      // Check the status of each task to log any exceptions
      for (Future<Void> future : futures) {
        try {
          // future.get() will throw an exception if the task failed or was cancelled.
          future.get();
        } catch (CancellationException e) {
          LOGGER.error("A task was cancelled, likely due to timeout.", e);
        } catch (ExecutionException e) {
          // The original exception from the Callable is wrapped in ExecutionException
          LOGGER.error("A task failed with an exception: {}", e.getCause().getMessage(), e.getCause());
        }
      }
      LOGGER.debug("All parallel tasks have been processed.");

    } catch (InterruptedException e) {
      LOGGER.warn("Main thread interrupted while waiting for tasks to complete.", e);
      Thread.currentThread().interrupt();
    }
  }


  /**
   * @param tableId     iceberg table identifier
   * @param sampleEvent sample debezium event. event schema used to create iceberg table when table not found
   * @return iceberg table, throws RuntimeException when table not found, and it's not possible to create it
   */
  public Table loadIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> this.createIcebergTable(tableId, sampleEvent));
  }

  private Table createIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {

    if (!config.debezium().eventSchemaEnabled() && !Objects.equals(config.debezium().keyValueChangeEventFormat(), "connect")) {
      throw new RuntimeException("Table '" + tableId + "' not found! " + "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
    }
    try {
      final Schema schema;
      final SortOrder sortOrder;
      if (!config.iceberg().createIdentifierFields()) {
        LOGGER.warn("Creating identifier fields is disabled, creating schema without identifier fields!");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else if (config.iceberg().nestedAsVariant()) {
        LOGGER.warn("Identifier fields are not supported when data consumed to variant fields, creating schema without identifier fields!");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else if (sampleEvent.isSchemaChangeEvent()) {
        // Check if the message is a schema change event (DDL statement).
        // Schema change events are identified by the presence of "ddl", "databaseName", and "tableChanges" fields.
        // "schema change topic" https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-schema-change-topic
        LOGGER.warn("Schema change topic detected. Creating Iceberg schema without identifier fields for append-only mode.");
        schema = sampleEvent.icebergSchema(false);
        sortOrder = SortOrder.unsorted();
      } else {
        schema = sampleEvent.icebergSchema(true);
        sortOrder = sampleEvent.sortOrder(schema);
      }


      // for backward compatibility, to be removed and set to "3" with one of the next releases
      // Format 3 will be used when variant data type is used
      final String tableFormatVersion = config.iceberg().nestedAsVariant() ? "3" : "2";
      return IcebergUtil.createIcebergTable(icebergCatalog, tableId, schema, sortOrder, config.iceberg().writeFormat(), tableFormatVersion);
    } catch (Exception e) {
      throw new DebeziumException("Failed to create table from debezium event table:" + tableId + " Error:" + e.getMessage(), e);
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
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  public TableIdentifier mapDestination(String destination) {
    return tableMapper.mapDestination(destination);
  }
}
