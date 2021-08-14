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

import java.io.Closeable;
import java.io.IOException;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages to iceberg tables.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
  static ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  @ConfigProperty(name = "debezium.format.value.schemas.enable", defaultValue = "false")
  boolean eventSchemaEnabled;
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
  boolean upsertData;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-keep-deletes", defaultValue = "true")
  boolean upsertDataKeepDeletes;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-op-column", defaultValue = "__op")
  String opColumn;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-source-ts-ms-column", defaultValue = "__source_ts_ms")
  String sourceTsMsColumn;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;
  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;
  Configuration hadoopConf = new Configuration();
  Catalog icebergCatalog;
  Map<String, String> icebergProperties = new ConcurrentHashMap<>();
  Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  Deserializer<JsonNode> valDeserializer;

  @PostConstruct
  void connect() throws InterruptedException {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    // pass iceberg properties to iceberg and hadoop
    Map<String, String> conf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX);
    conf.forEach(this.hadoopConf::set);
    conf.forEach(this.icebergProperties::put);

    icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();

    Instance<InterfaceBatchSizeWait> instance = batchSizeWaitInstances.select(NamedLiteral.of(batchSizeWaitName));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + batchSizeWaitName + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + batchSizeWaitName + "' is available");
    }
    batchSizeWait = instance.get();
    batchSizeWait.initizalize();
    LOGGER.info("Using {}", batchSizeWait.getClass().getName());
  }

  public String map(String destination) {
    return destination.replace(".", "_");
  }

  private Table createIcebergTable(TableIdentifier tableIdentifier, ChangeEvent<Object, Object> event) throws Exception {
    if (eventSchemaEnabled && event.value() != null) {
      DebeziumToIcebergTable eventSchema = event.key() == null
          ? new DebeziumToIcebergTable(getBytes(event.value()))
          : new DebeziumToIcebergTable(getBytes(event.value()), getBytes(event.key()));

      return eventSchema.create(icebergCatalog, tableIdentifier);
    }
    return null;
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
      Table icebergTable;
      final TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), tablePrefix + event.getKey());
      try {
        // load iceberg table
        icebergTable = icebergCatalog.loadTable(tableIdentifier);
      } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
        // get schema fom an event and create iceberg table
        try {
          icebergTable = createIcebergTable(tableIdentifier, event.getValue().get(0));
        } catch (Exception e2) {
          e.printStackTrace();
          throw new InterruptedException("Failed to create iceberg table, " + e2.getMessage());
        }
      }
      addToTable(icebergTable, event.getValue());
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();

    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());

  }

  public int compareByTsThenOp(GenericRecord lhs, GenericRecord rhs) {
    if (lhs.getField(sourceTsMsColumn).equals(rhs.getField(sourceTsMsColumn))) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      return
          cdcOperations.getOrDefault(lhs.getField(opColumn), -1)
              .compareTo(
                  cdcOperations.getOrDefault(rhs.getField(opColumn), -1)
              )
          ;
    } else {
      return Long.compare((Long) lhs.getField(sourceTsMsColumn), (Long) rhs.getField(sourceTsMsColumn));
    }
  }


  private ArrayList<Record> toIcebergRecords(Schema schema, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException {

    ArrayList<Record> icebergRecords = new ArrayList<>();
    for (ChangeEvent<Object, Object> e : events) {
      GenericRecord icebergRecord = IcebergUtil.getIcebergRecord(schema, valDeserializer.deserialize(e.destination(),
          getBytes(e.value())));
      icebergRecords.add(icebergRecord);
    }

    return icebergRecords;
  }

  private ArrayList<Record> toDeduppedIcebergRecords(Schema schema, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException {
    ConcurrentHashMap<Object, GenericRecord> icebergRecordsmap = new ConcurrentHashMap<>();

    for (ChangeEvent<Object, Object> e : events) {
      GenericRecord icebergRecord = IcebergUtil.getIcebergRecord(schema, valDeserializer.deserialize(e.destination(),
          getBytes(e.value())));

      // only replace it if its newer
      if (icebergRecordsmap.containsKey(e.key())) {

        if (this.compareByTsThenOp(icebergRecordsmap.get(e.key()), icebergRecord) <= 0) {
          icebergRecordsmap.put(e.key(), icebergRecord);
        }

      } else {
        icebergRecordsmap.put(e.key(), icebergRecord);
      }

    }
    return new ArrayList<>(icebergRecordsmap.values());
  }

  private void addToTable(Table icebergTable, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException {

    if (!upsertData || icebergTable.sortOrder().isUnsorted()) {

      if (upsertData && icebergTable.sortOrder().isUnsorted()) {
        LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      }

      ArrayList<Record> icebergRecords = toIcebergRecords(icebergTable.schema(), events);
      DataFile dataFile = getDataFile(icebergTable, icebergRecords);
      LOGGER.debug("Committing new file as Append '{}' !", dataFile.path());
      icebergTable.newAppend()
          .appendFile(dataFile)
          .commit();

    } else {
      // DO UPSERT >>> DELETE + INSERT
      ArrayList<Record> icebergRecords = toDeduppedIcebergRecords(icebergTable.schema(), events);
      DataFile dataFile = getDataFile(icebergTable, icebergRecords);
      DeleteFile deleteDataFile = getDeleteDataFile(icebergTable, icebergRecords);
      LOGGER.debug("Committing new file as Upsert (has deletes:{}) '{}' !", deleteDataFile != null, dataFile.path());
      RowDelta c = icebergTable
          .newRowDelta()
          .addRows(dataFile);

      if (deleteDataFile != null) {
        c.addDeletes(deleteDataFile)
            .validateDeletedFiles();
      }

      c.commit();
    }
    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());

  }

  private DeleteFile getDeleteDataFile(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {

    final String fileName = "del-" + UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));
    Set<Integer> equalityDeleteFieldIds = icebergTable.schema().identifierFieldIds();

    EqualityDeleteWriter<Record> deleteWriter;

    // anything is not an insert.
    // upsertDataKeepDeletes = false, which means delete deletes
    List<Record> deleteRows = icebergRecords.stream()
        .filter(r ->
                // anything is not an insert.
                !r.getField(opColumn).equals("c")
            // upsertDataKeepDeletes = false and its deleted record, which means delete deletes
            // || !(upsertDataKeepDeletes && r.getField(opColumn).equals("d"))
        ).collect(Collectors.toList());

    if (deleteRows.size() == 0) {
      return null;
    }

    try {
      LOGGER.debug("Writing data to equality delete file: {}!", out);

      deleteWriter = Parquet.writeDeletes(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .overwrite()
          .rowSchema(icebergTable.sortOrder().schema())
          .withSpec(icebergTable.spec())
          .equalityFieldIds(Lists.newArrayList(icebergTable.schema().identifierFieldIds()))
          .metricsConfig(MetricsConfig.fromProperties(icebergTable.properties()))
          .withSortOrder(icebergTable.sortOrder())
          .setAll(icebergTable.properties())
          .buildEqualityWriter()
      ;

      try (Closeable toClose = deleteWriter) {
        deleteWriter.deleteAll(deleteRows);
      }

    } catch (IOException e) {
      throw new InterruptedException(e.getMessage());
    }

    LOGGER.debug("Creating iceberg equality delete file!");
    // Equality delete files identify deleted rows in a collection of data files by one or more column values,
    // and may optionally contain additional columns of the deleted row.
    return FileMetadata.deleteFileBuilder(icebergTable.spec())
        .ofEqualityDeletes(Ints.toArray(icebergTable.schema().identifierFieldIds()))
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(deleteWriter.length())
        .withFileSizeInBytes(deleteWriter.length())
        .withRecordCount(deleteRows.size())
        .withSortOrder(icebergTable.sortOrder())
        .build();
  }

  private DataFile getDataFile(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

    FileAppender<Record> writer;
    // if its append OR upsert with keep deletes then add all the records to data file
    // if table has no PK - which means fall back to append
    // if its upsert and upsertKeepDeletes = true
    // if nothing above then exclude deletes
    List<Record> newRecords = icebergRecords.stream()
        .filter(r ->
            // if its append OR upsert with keep deletes then add all the records to data file
            !upsertData
                // if table has no PK - which means fall back to append
                || icebergTable.sortOrder().isUnsorted()
                // if its upsert and upsertKeepDeletes = true
                || upsertDataKeepDeletes
                // if nothing above then exclude deletes
                || !(r.getField(opColumn).equals("d")))
        .collect(Collectors.toList());
    try {
      LOGGER.debug("Writing data to file: {}!", out);
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(icebergTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        writer.addAll(newRecords);
      }

    } catch (IOException e) {
      throw new InterruptedException(e.getMessage());
    }

    LOGGER.debug("Creating iceberg DataFile!");
    return DataFiles.builder(icebergTable.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

}
