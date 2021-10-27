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
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.iceberg.batchsizewait.InterfaceBatchSizeWait;
import io.debezium.server.iceberg.tableoperator.InterfaceIcebergTableOperator;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.TableProperties.*;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
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
  @ConfigProperty(name = "debezium.sink.iceberg.fs.defaultFS")
  String defaultFs;
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.iceberg.catalog-name", defaultValue = "default")
  String catalogName;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
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
  @Any
  Instance<InterfaceIcebergTableOperator> icebergTableOperatorInstances;
  InterfaceIcebergTableOperator icebergTableOperator;

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

    Instance<InterfaceBatchSizeWait> instance = batchSizeWaitInstances.select(NamedLiteral.of(batchSizeWaitName));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + batchSizeWaitName + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + batchSizeWaitName + "' is available");
    }
    batchSizeWait = instance.get();
    batchSizeWait.initizalize();
    LOGGER.info("Using {}", batchSizeWait.getClass().getName());

    String icebergTableOperatorName = upsert ? "IcebergTableOperatorUpsert" : "IcebergTableOperatorAppend";
    Instance<InterfaceIcebergTableOperator> toInstance = icebergTableOperatorInstances.select(NamedLiteral.of(icebergTableOperatorName));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple class named `" + icebergTableOperatorName + "` were found");
    }
    if (instance.isUnsatisfied()) {
      throw new DebeziumException("No class named `" + icebergTableOperatorName + "` found");
    }
    icebergTableOperator = toInstance.get();
    icebergTableOperator.initialize();
    LOGGER.info("Using {}", icebergTableOperator.getClass().getName());

  }

  public String map(String destination) {
    return destination.replace(".", "_");
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            objectObjectChangeEvent -> map(objectObjectChangeEvent.destination()),
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> event : result.entrySet()) {
      final TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), tablePrefix + event.getKey());
      Table icebergTable = loadIcebergTable(tableIdentifier)
          .orElseGet(() -> createIcebergTable(tableIdentifier, event.getValue().get(0)));
      //addToTable(icebergTable, event.getValue());
      icebergTableOperator.addToTable(icebergTable, event.getValue());
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(records.size());

    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());

  }

  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }


  private Table createIcebergTable(TableIdentifier tableIdentifier,
                                   ChangeEvent<Object, Object> event) {

    if (!eventSchemaEnabled) {
      throw new RuntimeException("Table '" + tableIdentifier + "' not found! " +
          "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
    }

    if (event.value() == null) {
      throw new RuntimeException("Failed to get event schema for table '" + tableIdentifier + "' event value is null");
    }

    List<Types.NestedField> tableColumns = IcebergUtil.getIcebergFieldsFromEventSchema(getBytes(event.value()));
    List<Types.NestedField> keyColumns =
        IcebergUtil.getIcebergFieldsFromEventSchema(event.key() == null ? null : getBytes(event.key()));

    if (tableColumns.isEmpty()) {
      throw new RuntimeException("Failed to create table " + tableIdentifier);
    }

    Schema schema = IcebergUtil.getSchema(tableColumns, keyColumns);

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperty(FORMAT_VERSION, "2")
        .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
        .withSortOrder(IcebergUtil.getIdentifierFieldAsSortOrder(schema))
        .create();
  }


  private Optional<Table> loadIcebergTable(TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.warn("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

}
