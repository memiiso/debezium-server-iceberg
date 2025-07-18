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
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    for (Map.Entry<String, List<EventConverter>> tableEvents : result.entrySet()) {
      Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0));
      icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
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
   * @param tableId     iceberg table identifier
   * @param sampleEvent sample debezium event. event schema used to create iceberg table when table not found
   * @return iceberg table, throws RuntimeException when table not found, and it's not possible to create it
   */
  public Table loadIcebergTable(TableIdentifier tableId, EventConverter sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
      if (!config.debezium().eventSchemaEnabled() && !Objects.equals(config.debezium().keyValueChangeEventFormat(), "connect")) {
        throw new RuntimeException("Table '" + tableId + "' not found! " + "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
      }
      try {
        final Schema schema = sampleEvent.icebergSchema();
        // for backward compatibility, to be removed and set to "3" with one of the next releases
        // Format 3 will be used when variant data type is used
        final String tableFormatVersion = config.iceberg().nestedAsVariant() ? "3" : "2";
        // Check if the message is a schema change event (DDL statement).
        // Schema change events are identified by the presence of "ddl", "databaseName", and "tableChanges" fields.
        // "schema change topic" https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-schema-change-topic
        if (sampleEvent.isSchemaChangeEvent()) {
          LOGGER.warn("Schema change topic detected. Creating Iceberg schema without identifier fields for append-only mode.");
          return IcebergUtil.createIcebergTable(icebergCatalog, tableId, new Schema(schema.columns()), config.iceberg().writeFormat(), tableFormatVersion);
        }

        return IcebergUtil.createIcebergTable(icebergCatalog, tableId, schema, config.iceberg().writeFormat(), tableFormatVersion);
      } catch (Exception e) {
        throw new DebeziumException("Failed to create table from debezium event table:" + tableId + " Error:" + e.getMessage(), e);
      }
    });
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
