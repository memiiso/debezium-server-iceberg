/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.iceberg.batchsizewait.BatchSizeWait;
import io.debezium.server.iceberg.tableoperator.PartitionedAppendWriter;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  protected static final ObjectMapper mapper = new ObjectMapper();
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  static final String TABLE_NAME = "debezium_events";
  static final Schema TABLE_SCHEMA = new Schema(
      required(1, "event_destination", Types.StringType.get()),
      optional(2, "event_key_schema", Types.StringType.get()),
      optional(3, "event_key_payload", Types.StringType.get()),
      optional(4, "event_value_schema", Types.StringType.get()),
      optional(5, "event_value_payload", Types.StringType.get()),
      optional(6, "event_sink_epoch_ms", Types.LongType.get()),
      optional(7, "event_sink_timestamptz", Types.TimestampType.withZone())
  );

  static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA)
      .identity("event_destination")
      .hour("event_sink_timestamptz")
      .build();
  static final SortOrder TABLE_SORT_ORDER = SortOrder.builderFor(TABLE_SCHEMA)
      .asc("event_sink_epoch_ms", NullOrder.NULLS_LAST)
      .asc("event_sink_timestamptz", NullOrder.NULLS_LAST)
      .build();
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergEventsChangeConsumer.class);
  static Deserializer<JsonNode> valDeserializer;
  static Deserializer<JsonNode> keyDeserializer;
  final Configuration hadoopConf = new Configuration();

  @Inject
  IcebergConsumerConfig config;

  @Inject
  @Any
  Instance<BatchSizeWait> batchSizeWaitInstances;
  BatchSizeWait batchSizeWait;
  Catalog icebergCatalog;
  Table eventTable;

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
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(config.namespace()), TABLE_NAME);

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

    batchSizeWait = IcebergUtil.selectInstance(batchSizeWaitInstances, config.batchSizeWaitName());
    batchSizeWait.initizalize();

    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();

    LOGGER.info("Using {}", batchSizeWait.getClass().getName());
  }

  public GenericRecord getIcebergRecord(ChangeEvent<Object, Object> record, OffsetDateTime batchTime) {

    try {
      // deserialize
      JsonNode valueSchema = record.value() == null ? null : mapper.readTree(getBytes(record.value())).get("schema");
      JsonNode valuePayload = valDeserializer.deserialize(record.destination(), getBytes(record.value()));
      JsonNode keyPayload = record.key() == null ? null : keyDeserializer.deserialize(record.destination(), getBytes(record.key()));
      JsonNode keySchema = record.key() == null ? null : mapper.readTree(getBytes(record.key())).get("schema");
      // convert to GenericRecord
      GenericRecord rec = GenericRecord.create(TABLE_SCHEMA.asStruct());
      rec.setField("event_destination", record.destination());
      rec.setField("event_key_schema", mapper.writeValueAsString(keySchema));
      rec.setField("event_key_payload", mapper.writeValueAsString(keyPayload));
      rec.setField("event_value_schema", mapper.writeValueAsString(valueSchema));
      rec.setField("event_value_payload", mapper.writeValueAsString(valuePayload));
      rec.setField("event_sink_epoch_ms", batchTime.toEpochSecond());
      rec.setField("event_sink_timestamptz", batchTime);

      return rec;
    } catch (IOException e) {
      throw new DebeziumException(e);
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    OffsetDateTime batchTime = OffsetDateTime.now(ZoneOffset.UTC);
    ArrayList<Record> icebergRecords = records.stream()
        .map(e -> getIcebergRecord(e, batchTime))
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
