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
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.apache.iceberg.util.ArrayUtil;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
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
  @ConfigProperty(name = "debezium.sink.iceberg.dynamic-wait", defaultValue = "true")
  boolean dynamicWaitEnabled;

  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsertData;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-keep-deletes", defaultValue = "true")
  boolean upsertDataKeepDeletes;

  @Inject
  BatchDynamicWait dynamicWait;

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
  }

  public String map(String destination) {
    return destination.replace(".", "_");
  }

  private Table createIcebergTable(TableIdentifier tableIdentifier, ChangeEvent<Object, Object> event) {
    if (eventSchemaEnabled && event.value() != null) {
      try {
        EventToIcebergTable eventSchema = event.key() == null
            ? new EventToIcebergTable(getBytes(event.value()))
            : new EventToIcebergTable(getBytes(event.key()), getBytes(event.value()));

        return eventSchema.create(icebergCatalog, tableIdentifier);
      } catch (Exception e) {
        LOGGER.warn("Failed creating iceberg table! {}", e.getMessage());
      }
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
        icebergTable = createIcebergTable(tableIdentifier, event.getValue().get(0));
        if (icebergTable == null) {
          LOGGER.warn("Iceberg table '{}' not found! Ignoring received data for the table!", tableIdentifier);
          continue;
        }
      }
      appendTable(icebergTable, toIcebergRecords(icebergTable.schema(), event.getValue()));
    }
    committer.markBatchFinished();

    if (dynamicWaitEnabled) {
      dynamicWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
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

  // dedup
  // @TODO add test table without PK {insert update delete}  ins del up on same record
  // @TODO add test table with PK {insert update delete}
  // @TODO add test table with PK {insert update delete} keep deletes = true
  // @TODO make column and the value configurable
  private void appendTable(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {

    if (!upsertData || icebergTable.sortOrder().isUnsorted()) {

      if (upsertData && icebergTable.sortOrder().isUnsorted()) {
        LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      }

      DataFile dataFile = getDataFile(icebergTable, icebergRecords);
      LOGGER.debug("Committing new file as newAppend '{}' !", dataFile.path());
      icebergTable.newAppend()
          .appendFile(dataFile)
          .commit();

    } else {
      // DO UPSERT >>> DELETE + INSERT
      DataFile dataFile = getDataFile(icebergTable, icebergRecords);
      DeleteFile deleteDataFile = getDeleteDataFile(icebergTable, icebergRecords);
      // @TODO do add deletes
      LOGGER.debug("Committing new file as newAppend '{}' !", dataFile.path());
      icebergTable
          .newRowDelta()
          .addDeletes(deleteDataFile)
          .addRows(dataFile)
          .commit();
    }
    LOGGER.info("Committed events to table! {}", icebergTable.location());

  }

  private List<Integer> getEqualityFieldIds(Table icebergTable) {
    List<Integer> fieldIds = new ArrayList<>();

    for (SortField f : icebergTable.sortOrder().fields()) {
      fieldIds.add(f.sourceId());
    }
    return fieldIds;
  }

  private DeleteFile getDeleteDataFile(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {

    final String fileName = "del-" + UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));
    List<Integer> equalityDeleteFieldIds = getEqualityFieldIds(icebergTable);

    EqualityDeleteWriter<Record> deleteWriter;
    try {
      LOGGER.debug("Writing data to equality delete file: {}!", out);

//      GenericAppenderFactory gaf = new GenericAppenderFactory(
//          icebergTable.sortOrder().schema(), // FileAppender, data file appender schema! not used/relevant
//          icebergTable.spec(), // PartitionSpec
//          ArrayUtil.toIntArray(equalityDeleteFieldIds), // int[] equalityFieldIds,
//          icebergTable.schema(), // Schema eqDeleteRowSchema
//          null // Schema posDeleteRowSchema
//      );
//      gaf.newEqDeleteWriter(
//          out,// EncryptedOutputFile file,
//          FileFormat.PARQUET,// FileFormat format,
//          icebergTable.sortOrder() // StructLike partition ???
//      );

      deleteWriter = Parquet.writeDeletes(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .overwrite()
          .rowSchema(icebergTable.sortOrder().schema())
          .withSpec(icebergTable.spec())
          .equalityFieldIds(equalityDeleteFieldIds)
          //.withKeyMetadata() // ??
          .metricsConfig(MetricsConfig.fromProperties(icebergTable.properties()))
          // .withPartition() // ??
          // @TODO add sort order v12 ??
          .setAll(icebergTable.properties())
          .buildEqualityWriter()
      ;

      try (Closeable toClose = deleteWriter) {
        icebergRecords.stream()
            .filter(r ->
                // anything is not an insert.
                !r.getField("__op").equals("c")
                    // upsertDataKeepDeletes = false, which means delete deletes
                    && (!upsertDataKeepDeletes && r.getField("__op").equals("d"))
            )
            .forEach(deleteWriter::delete);
      }

    } catch (IOException e) {
      throw new InterruptedException(e.getMessage());
    }

    LOGGER.debug("Creating iceberg equality delete file!");
    // Equality delete files identify deleted rows in a collection of data files by one or more column values,
    // and may optionally contain additional columns of the deleted row.
    return FileMetadata.deleteFileBuilder(icebergTable.spec())
        .ofEqualityDeletes(ArrayUtil.toIntArray(equalityDeleteFieldIds))
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(deleteWriter.length())
        //.withMetrics(deleteWriter.metrics()) //
        //.withRecordCount(3) // ?? throws Exception! and its mandatory field!
        //.withSortOrder(icebergTable.sortOrder())
        .build();
  }

  private DataFile getDataFile(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

    FileAppender<Record> writer;
    try {
      LOGGER.debug("Writing data to file: {}!", out);
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(icebergTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        icebergRecords.stream()
            .filter(r ->
                // if its append OR upsert with keep deletes then add all the records to data file
                !upsertData
                    // if table has no PK - which means fall back to append
                    || icebergTable.sortOrder().isUnsorted()
                    // if its upsert and upsertKeepDeletes = true
                    || upsertDataKeepDeletes
                    // if nothing above then exclude deletes
                    || !(r.getField("__op").equals("d")))
            .forEach(writer::add);
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
