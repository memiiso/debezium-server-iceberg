/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.iceberg.batchsizewait.InterfaceBatchSizeWait;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  static Deserializer<JsonNode> valDeserializer;
  static Deserializer<JsonNode> keyDeserializer;
  protected final Clock clock = Clock.system();
  final Configuration hadoopConf = new Configuration();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);

  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;
  Catalog icebergCatalog;
  @Inject
  IcebergTableOperator icebergTableOperator;
  @Inject
  IcebergConsumerConfig config;


  @PostConstruct
  void connect() {
    if (!config.valueFormat().equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.value={" + config.valueFormat() + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!config.keyFormat().equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.key={" + config.valueFormat() + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    // pass iceberg properties to iceberg and hadoop
    config.icebergConfigs().forEach(this.hadoopConf::set);

    icebergCatalog = CatalogUtil.buildIcebergCatalog(config.catalogName(), config.icebergConfigs(), hadoopConf);
    batchSizeWait = IcebergUtil.selectInstance(batchSizeWaitInstances, config.batchSizeWaitName());
    batchSizeWait.initizalize();

    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    //group events by destination (per iceberg table)
    Map<String, List<RecordConverter>> result =
        records.stream()
            .map((ChangeEvent<Object, Object> e)
                -> new RecordConverter(e.destination(), getBytes(e.value()), e.key() == null ? null : getBytes(e.key())))
            .collect(Collectors.groupingBy(RecordConverter::destination));

    // consume list of events for each destination table
    for (Map.Entry<String, List<RecordConverter>> tableEvents : result.entrySet()) {
      Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0));
      icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
    }

    // workaround! somehow offset is not saved to file unless we call committer.markProcessed per event
    // even it's should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
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
  public Table loadIcebergTable(TableIdentifier tableId, RecordConverter sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
      if (!config.eventSchemaEnabled()) {
        throw new RuntimeException("Table '" + tableId + "' not found! " + "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
      }
      try {
        return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(config.createIdentifierFields()), config.writeFormat());
      } catch (Exception e) {
        throw new DebeziumException("Failed to create table from debezium event schema:" + tableId + " Error:" + e.getMessage(), e);
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
    final String tableName = destination
        .replaceAll(config.destinationRegexp().orElse(""), config.destinationRegexpReplace().orElse(""))
        .replace(".", "_");

    if (config.destinationUppercaseTableNames()) {
      return TableIdentifier.of(Namespace.of(config.namespace()), (config.tablePrefix().orElse("") + tableName).toUpperCase());
    } else if (config.destinationLowercaseTableNames()) {
      return TableIdentifier.of(Namespace.of(config.namespace()), (config.tablePrefix().orElse("") + tableName).toLowerCase());
    } else {
      return TableIdentifier.of(Namespace.of(config.namespace()), config.tablePrefix().orElse("") + tableName);
    }
  }
}
