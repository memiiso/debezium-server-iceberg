/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  protected static final ObjectMapper mapper = new ObjectMapper();
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  public static final String PROP_PREFIX = "debezium.sink.iceberg.";
  static Deserializer<JsonNode> valDeserializer;
  static Deserializer<JsonNode> keyDeserializer;
  protected final Clock clock = Clock.system();
  final Configuration hadoopConf = new Configuration();
  final Map<String, String> icebergProperties = new ConcurrentHashMap<>();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  @ConfigProperty(name = PROP_PREFIX + CatalogProperties.WAREHOUSE_LOCATION)
  String warehouseLocation;
  @ConfigProperty(name = "debezium.sink.iceberg.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.iceberg.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.iceberg.catalog-name", defaultValue = "default")
  String catalogName;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
  @ConfigProperty(name = "debezium.sink.iceberg.partition-field", defaultValue = "__ts_ms")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;
  @ConfigProperty(name = "debezium.format.value.schemas.enable", defaultValue = "false")
  boolean eventSchemaEnabled;
  @ConfigProperty(name = "debezium.sink.iceberg." + DEFAULT_FILE_FORMAT, defaultValue = DEFAULT_FILE_FORMAT_DEFAULT)
  String writeFormat;
  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;
  Catalog icebergCatalog;
  @Inject
  IcebergTableOperator icebergTableOperator;

  @PostConstruct
  void connect() {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    // pass iceberg properties to iceberg and hadoop
    Map<String, String> conf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX);
    conf.forEach(this.hadoopConf::set);
    this.icebergProperties.putAll(conf);

    icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

    batchSizeWait = IcebergUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
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

    //group events by destination
    Map<String, List<IcebergChangeEvent>> result =
        records.stream()
            .map((ChangeEvent<Object, Object> e)
                -> {
              try {
                return new IcebergChangeEvent(e.destination(),
                    valDeserializer.deserialize(e.destination(), getBytes(e.value())),
                    e.key() == null ? null : keyDeserializer.deserialize(e.destination(), getBytes(e.key())),
                    mapper.readTree(getBytes(e.value())).get("schema"),
                    e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema")
                );
              } catch (IOException ex) {
                throw new DebeziumException(ex);
              }
            })
            .collect(Collectors.groupingBy(IcebergChangeEvent::destination));

    // consume list of events for each destination table
    for (Map.Entry<String, List<IcebergChangeEvent>> tableEvents : result.entrySet()) {
      Table icebergTable = this.loadIcebergTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0));
      icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
    }

    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even it's should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(records.size());

    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
  }

  /**
   * @param tableId     iceberg table identifier
   * @param sampleEvent sample debezium event. event schema used to create iceberg table when table not found
   * @return iceberg table, throws RuntimeException when table not found and it's not possible to create it
   */
  public Table loadIcebergTable(TableIdentifier tableId, IcebergChangeEvent sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
      if (!eventSchemaEnabled) {
        throw new RuntimeException("Table '" + tableId + "' not found! " + "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
      }
      return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(), writeFormat,
          !upsert, // partition if its append mode
          partitionField);
    });
  }

  /**
   * @param numUploadedEvents periodically log number of events consumed
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
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");

    return TableIdentifier.of(Namespace.of(namespace), tablePrefix + tableName);
  }
}
