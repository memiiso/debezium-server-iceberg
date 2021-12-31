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
import io.debezium.server.iceberg.tableoperator.PartitionedAppendWriter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Implementation of the consumer that delivers the messages to iceberg table.
 *
 * @author Ismail Simsek
 */
@Named("icebergevents")
@Dependent
public class IcebergEventsChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  static final Schema TABLE_SCHEMA = new Schema(
      required(1, "event_destination", Types.StringType.get()),
      optional(2, "event_key", Types.StringType.get()),
      optional(3, "event_value", Types.StringType.get()),
      optional(4, "event_sink_epoch_ms", Types.LongType.get()),
      optional(5, "event_sink_timestamptz", Types.TimestampType.withZone())
  );
  static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA)
      .identity("event_destination")
      .hour("event_sink_timestamptz")
      .build();
  static final SortOrder TABLE_SORT_ORDER = SortOrder.builderFor(TABLE_SCHEMA)
      .asc("event_sink_epoch_ms", NullOrder.NULLS_LAST)
      .build();
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergEventsChangeConsumer.class);
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
  final Configuration hadoopConf = new Configuration();
  final Map<String, String> icebergProperties = new ConcurrentHashMap<>();
  @ConfigProperty(name = "debezium.sink.iceberg." + CatalogProperties.WAREHOUSE_LOCATION)
  String warehouseLocation;
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  @ConfigProperty(name = "debezium.sink.iceberg.fs.defaultFS")
  String defaultFs;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.iceberg.catalog-name", defaultValue = "default")
  String catalogName;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;
  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;
  Catalog icebergCatalog;
  Table eventTable;


  @PostConstruct
  void connect() {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.value={" + valueFormat + "} not supported, " +
                                  "Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.key={" + valueFormat + "} not supported, " +
                                  "Supported (debezium.format.key=*) formats are {json,}!");
    }

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), "debezium_events");

    Map<String, String> conf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX);
    conf.forEach(this.hadoopConf::set);
    this.icebergProperties.putAll(conf);

    if (warehouseLocation == null || warehouseLocation.trim().isEmpty()) {
      warehouseLocation = defaultFs + "/iceberg_warehouse";
    }

    icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

    // create table if not exists
    if (!icebergCatalog.tableExists(tableIdentifier)) {
      icebergCatalog
          .buildTable(tableIdentifier, TABLE_SCHEMA)
          .withPartitionSpec(TABLE_PARTITION)
          .withSortOrder(TABLE_SORT_ORDER)
          .create();
    }
    // load table
    eventTable = icebergCatalog.loadTable(tableIdentifier);

    Instance<InterfaceBatchSizeWait> instance = batchSizeWaitInstances.select(NamedLiteral.of(batchSizeWaitName));
    batchSizeWait = IcebergUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
    batchSizeWait.initizalize();
    LOGGER.info("Using {}", batchSizeWait.getClass().getName());
  }

  public GenericRecord getIcebergRecord(String destination, ChangeEvent<Object, Object> record, OffsetDateTime batchTime) {
    GenericRecord rec = GenericRecord.create(TABLE_SCHEMA.asStruct());
    rec.setField("event_destination", destination);
    rec.setField("event_key", getString(record.key()));
    rec.setField("event_value", getString(record.value()));
    rec.setField("event_sink_epoch_ms", batchTime.toEpochSecond());
    rec.setField("event_sink_timestamptz", batchTime);
    return rec;
  }

  public String map(String destination) {
    return destination.replace(".", "_");
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    OffsetDateTime batchTime = OffsetDateTime.now(ZoneOffset.UTC);
    ArrayList<Record> icebergRecords = records.stream()
        .map(e -> getIcebergRecord(e.destination(), e, batchTime))
        .collect(Collectors.toCollection(ArrayList::new));
    commitBatch(icebergRecords);

    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even it's should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
  }

  private void commitBatch(ArrayList<Record> icebergRecords) {

    FileFormat format = IcebergUtil.getTableFileFormat(eventTable);
    GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(eventTable);
    int partitionId = Integer.parseInt(dtFormater.format(Instant.now()));
    OutputFileFactory fileFactory = OutputFileFactory.builderFor(eventTable, partitionId, 1L)
        .defaultSpec(eventTable.spec()).format(format).build();

    BaseTaskWriter<Record> writer = new PartitionedAppendWriter(
        eventTable.spec(), format, appenderFactory, fileFactory, eventTable.io(), Long.MAX_VALUE, eventTable.schema());

    try {
      for (Record icebergRecord : icebergRecords) {
        writer.write(icebergRecord);
      }

      writer.close();
      WriteResult files = writer.complete();
      AppendFiles appendFiles = eventTable.newAppend();
      Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
      appendFiles.commit();

    } catch (IOException e) {
      LOGGER.error("Failed committing events to iceberg table!", e);
      throw new DebeziumException("Failed committing events to iceberg table!", e);
    }

    LOGGER.info("Committed {} events to table! {}", icebergRecords.size(), eventTable.location());
  }

}
