/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
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

  @ConfigProperty(name = "debezium.sink.iceberg." + CatalogProperties.WAREHOUSE_LOCATION)
  String warehouseLocation;

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergEventsChangeConsumer.class);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  Configuration hadoopConf = new Configuration();
  @ConfigProperty(name = "debezium.sink.iceberg.fs.defaultFS")
  String defaultFs;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.iceberg.catalog-name", defaultValue = "default")
  String catalogName;
  @ConfigProperty(name = "debezium.sink.iceberg.dynamic-wait", defaultValue = "true")
  boolean dynamicWaitEnabled;
  @Inject
  BatchDynamicWait dynamicWait;

  private TableIdentifier tableIdentifier;
  Map<String, String> icebergProperties = new ConcurrentHashMap<>();
  Catalog icebergCatalog;
  Table eventTable;


  @PostConstruct
  void connect() throws InterruptedException {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    tableIdentifier = TableIdentifier.of(Namespace.of(namespace), "debezium_events");

    Map<String, String> conf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX);
    conf.forEach(this.hadoopConf::set);
    conf.forEach(this.icebergProperties::put);

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

    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            objectObjectChangeEvent -> map(objectObjectChangeEvent.destination()),
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destEvents : result.entrySet()) {
      // each destEvents is set of events for a single table
      ArrayList<Record> destIcebergRecords = destEvents.getValue().stream()
          .map(e -> getIcebergRecord(destEvents.getKey(), e, batchTime))
          .collect(Collectors.toCollection(ArrayList::new));

      commitBatch(destEvents.getKey(), batchTime, destIcebergRecords);
    }
    // committer.markProcessed(record);
    committer.markBatchFinished();

    if (dynamicWaitEnabled) {
      dynamicWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
    }
  }

  private void commitBatch(String destination, OffsetDateTime batchTime, ArrayList<Record> icebergRecords) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;

    PartitionKey pk = new PartitionKey(TABLE_PARTITION, TABLE_SCHEMA);
    Record pr = GenericRecord.create(TABLE_SCHEMA)
        .copy("event_destination",
            destination, "event_sink_timestamptz",
            DateTimeUtil.microsFromTimestamptz(batchTime));
    pk.partition(pr);

    OutputFile out =
        eventTable.io().newOutputFile(eventTable.locationProvider().newDataLocation(pk.toPath() + "/" + fileName));

    FileAppender<Record> writer;
    try {
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(eventTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        writer.addAll(icebergRecords);
      }

    } catch (IOException e) {
      LOGGER.error("Failed committing events to iceberg table!", e);
      throw new InterruptedException(e.getMessage());
    }

    DataFile dataFile = DataFiles.builder(eventTable.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .withPartition(pk)
        .build();

    LOGGER.debug("Appending new file '{}' !", dataFile.path());
    eventTable.newAppend()
        .appendFile(dataFile)
        .commit();
    LOGGER.info("Committed events to table! {}", eventTable.location());
  }

}
