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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
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

  // @TODO add schema enabled flags! for key and value!
  // @TODO add flattened flag SMT unwrap! as bolean?
  // @TODO add event_destination and event_sink_timestamp to partition
  static final Schema TABLE_SCHEMA = new Schema(
      required(1, "event_destination", Types.StringType.get(), "event destination"),
      optional(2, "event_key", Types.StringType.get()),
      // @TODO split to before after payload and source and schema
      // @TODO change strategy based on unwrap flag
      optional(3, "event_value", Types.StringType.get()),
      optional(4, "event_sink_timestamp", Types.TimestampType.withZone()));
  static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA).identity("event_destination").build();

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
      icebergCatalog.createTable(tableIdentifier, TABLE_SCHEMA, TABLE_PARTITION);
    }
    // load table
    eventTable = icebergCatalog.loadTable(tableIdentifier);
  }

  public GenericRecord getIcebergRecord(String destination, ChangeEvent<Object, Object> record, LocalDateTime batchTime) {
    Map<String, Object> var1 = Maps.newHashMapWithExpectedSize(TABLE_SCHEMA.columns().size());
    var1.put("event_destination", destination);
    var1.put("event_key", getString(record.key()));
    var1.put("event_value", getString(record.value()));
    var1.put("event_sink_timestamp", batchTime.atOffset(ZoneOffset.UTC));
    return GenericRecord.create(TABLE_SCHEMA).copy(var1);
  }

  public String map(String destination) {
    return destination.replace(".", "_");
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LocalDateTime batchTime = LocalDateTime.now(ZoneOffset.UTC);

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

      commitBatch(destEvents.getKey(), destIcebergRecords, batchTime);
    }
    // committer.markProcessed(record);
    committer.markBatchFinished();
  }

  private void commitBatch(String destination, ArrayList<Record> icebergRecords, LocalDateTime batchTime) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + batchTime + "." + FileFormat.PARQUET.toString().toLowerCase();
    // NOTE! manually setting partition directory here to destination
    OutputFile out = eventTable.io().newOutputFile(eventTable.locationProvider().newDataLocation("event_destination=" + destination + "/" + fileName));

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

    PartitionKey pk = new PartitionKey(TABLE_PARTITION, TABLE_SCHEMA);
    Record pr = GenericRecord.create(TABLE_SCHEMA).copy("event_destination", destination, "event_sink_timestamp", batchTime);
    pk.partition(pr);

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
