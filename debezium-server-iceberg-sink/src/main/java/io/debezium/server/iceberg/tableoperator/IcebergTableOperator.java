/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.SchemaConverter;
import io.debezium.server.iceberg.deduplication.BatchDeduplicator;
import io.debezium.server.iceberg.deduplication.RetainLastBatchDeduplicator;
import io.debezium.server.iceberg.deduplication.TimestampAndOpBatchDeduplicator;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @Inject IcebergTableWriterFactory writerFactory;
  @Inject GlobalConfig config;

  private volatile ConnectorContext openLineageContext;
  private BatchDeduplicator deduplicator;

  @PostConstruct
  void init() {
    this.deduplicator = resolveDeduplicator();
    LOGGER.info("Using batch deduplicator: {}", this.deduplicator.getClass().getSimpleName());
  }

  BatchDeduplicator resolveDeduplicator() {
    String dedup =
        (config != null && config.iceberg() != null && config.iceberg().deduplicator().isPresent())
            ? config.iceberg().deduplicator().get().trim().toLowerCase(Locale.ROOT)
            : "";

    if (dedup.equals("retainlast") || dedup.equals("retain-last")) {
      return new RetainLastBatchDeduplicator();
    } else if (dedup.equals("default")
        || dedup.equals("timestampandop")
        || dedup.equals("timestamp-and-op")) {
      return new TimestampAndOpBatchDeduplicator(config.iceberg());
    } else if (config != null
        && config.iceberg() != null
        && config.iceberg().cdcSourceTsField().filter(s -> !s.isBlank()).isPresent()) {
      return new TimestampAndOpBatchDeduplicator(config.iceberg());
    } else {
      return new RetainLastBatchDeduplicator();
    }
  }

  protected List<EventConverter> deduplicateBatch(List<EventConverter> events) {
    return this.deduplicator.deduplicate(events);
  }

  /**
   * If given schema contains new fields compared to target table schema then it adds new fields to
   * target iceberg table.
   *
   * <p>Its used when allow field addition feature is enabled.
   *
   * @param icebergTable
   * @param newSchema
   */
  private void applyFieldAddition(Table icebergTable, Schema newSchema) {

    UpdateSchema us =
        icebergTable
            .updateSchema()
            .unionByNameWith(newSchema)
            .setIdentifierFields(newSchema.identifierFieldNames());
    Schema newSchemaCombined = us.apply();

    // @NOTE avoid committing when there is no schema change. commit creates new commit even when
    // there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }
  }

  /**
   * Adds list of events to iceberg table.
   *
   * <p>If field addition enabled then it groups list of change events by their schema first. Then
   * adds new fields to iceberg table if there is any. And then follows with adding data to the
   * table.
   *
   * <p>New fields are detected using CDC event schema, since events are grouped by their schemas it
   * uses single event to find-out schema for the whole list of events.
   *
   * @param icebergTable
   * @param events
   */
  public void addToTable(Table icebergTable, List<EventConverter> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (config.iceberg().upsert() && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!config.iceberg().allowFieldAddition()) {
      // if field additions not enabled add set of events to table
      addToTablePerSchema(icebergTable, events);
    } else {
      Map<SchemaConverter, List<EventConverter>> eventsGroupedBySchema =
          events.stream()
              .collect(
                  Collectors.groupingBy(
                      EventConverter::schemaConverter, LinkedHashMap::new, Collectors.toList()));
      LOGGER.debug(
          "Batch got {} records with {} different schema!!",
          events.size(),
          eventsGroupedBySchema.keySet().size());

      for (Map.Entry<SchemaConverter, List<EventConverter>> schemaEvents :
          eventsGroupedBySchema.entrySet()) {
        // extend table schema if new fields found
        applyFieldAddition(
            icebergTable,
            schemaEvents
                .getValue()
                .get(0)
                .icebergSchema(config.iceberg().createIdentifierFields()));
        // add set of events to table
        addToTablePerSchema(icebergTable, schemaEvents.getValue());
      }
    }
  }

  /**
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private void addToTablePerSchema(Table icebergTable, List<EventConverter> events) {
    // Initialize a task writer to write both INSERT and equality DELETE.
    final Schema tableSchema = icebergTable.schema();
    BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);
    boolean commitAttempted = false;
    try (writer) {
      for (EventConverter e : events) {
        final RecordWrapper record =
            (config.iceberg().upsert() && !tableSchema.identifierFieldIds().isEmpty())
                ? e.convert(tableSchema)
                : e.convertAsAppend(tableSchema);
        writer.write(record);
      }

      WriteResult files = writer.complete();
      commitAttempted = true;
      if (files.deleteFiles().length > 0) {
        RowDelta newRowDelta = icebergTable.newRowDelta();
        Arrays.stream(files.dataFiles()).forEach(newRowDelta::addRows);
        Arrays.stream(files.deleteFiles()).forEach(newRowDelta::addDeletes);
        newRowDelta.commit();
      } else {
        AppendFiles appendFiles = icebergTable.newAppend();
        Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
        appendFiles.commit();
      }
    } catch (Exception ex) {
      // once a commit was attempted the files may already be referenced by the table (e.g.
      // CommitStateUnknownException), only delete them when iceberg reports the failure cleanable
      if (!commitAttempted || ex instanceof CleanableFailure) {
        try {
          writer.abort();
        } catch (Exception e) {
          LOGGER.warn("Failed to abort writer for table:`{}`", icebergTable.name(), e);
        }
      }
      throw new DebeziumException(
          "Failed to write data to table:`" + icebergTable.name() + "`", ex);
    }

    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());

    // Emit OpenLineage output dataset metadata after successful commit
    if (config.iceberg().openlineageEnabled()) {
      try {
        emitOpenLineageEvent(icebergTable);
      } catch (Exception e) {
        LOGGER.debug("OpenLineage emission failed (non-critical)", e);
      }
    }
  }

  private ConnectorContext getOpenLineageContext() {
    if (openLineageContext == null) {
      synchronized (this) {
        if (openLineageContext == null) {
          openLineageContext =
              new ConnectorContext(
                  "debezium-server-iceberg",
                  "iceberg",
                  "0",
                  Module.version(),
                  java.util.UUID.randomUUID(),
                  java.util.Map.of());
        }
      }
    }
    return openLineageContext;
  }

  private void emitOpenLineageEvent(Table icebergTable) {
    List<DatasetMetadata.FieldDefinition> fields =
        icebergTable.schema().columns().stream()
            .map(f -> new DatasetMetadata.FieldDefinition(f.name(), f.type().toString(), ""))
            .toList();

    DatasetMetadata metadata =
        new DatasetMetadata(
            icebergTable.name(),
            DatasetMetadata.DatasetKind.OUTPUT,
            DatasetMetadata.TABLE_DATASET_TYPE,
            DatasetMetadata.DataStore.DATABASE,
            fields);

    DebeziumOpenLineageEmitter.emit(
        getOpenLineageContext(), DebeziumTaskState.RUNNING, List.of(metadata));
  }
}
