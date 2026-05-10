/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.ImmutableMap;
import io.debezium.DebeziumException;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.SchemaConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.data.Record;
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

  /**
   * Result of committing a writer. Contains file count, total bytes, and record count
   * for adaptive file split calibration.
   */
  public static class CommitResult {
    public final int fileCount;
    public final long totalBytes;
    public final long totalRecords;

    public CommitResult(int fileCount, long totalBytes, long totalRecords) {
      this.fileCount = fileCount;
      this.totalBytes = totalBytes;
      this.totalRecords = totalRecords;
    }
  }

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY =
      ImmutableMap.of(
          Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @Inject IcebergTableWriterFactory writerFactory;
  @Inject GlobalConfig config;

  private volatile ConnectorContext openLineageContext;

  protected List<EventConverter> deduplicateBatch(List<EventConverter> events) {

    ConcurrentHashMap<Object, EventConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(
        e -> {
          if (!e.hasKeyData()) {
            throw new DebeziumException(
                "Cannot deduplicate data with null key! destination:'"
                    + e.destination()
                    + "' event: '"
                    + e.value().toString()
                    + "'");
          }

          try {
            // READ (snapshot) events are new data like INSERT — no prior version to delete
            Operation op = e.cdcOpValue();
            e.setNewKey(op == Operation.INSERT || op == Operation.READ);
            // deduplicate using key(PK)
            deduplicatedEvents.merge(
                e.key(),
                e,
                (oldValue, newValue) -> {
                  if (this.compareByTsThenOp(oldValue, newValue) <= 0) {
                    return newValue;
                  } else {
                    return oldValue;
                  }
                });
          } catch (Exception ex) {
            throw new DebeziumException("Failed to deduplicate events", ex);
          }
        });

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   *
   * <p>Forex ample a record can be updated multiple times in the source. for example insert
   * followed by update and delete. for this case we need to only pick last change event for the
   * row.
   *
   * <p>Its used when `upsert` feature enabled (when the consumer operating non append mode) which
   * means it should not add duplicate records to target table.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(EventConverter lhs, EventConverter rhs) {
    if (config.iceberg().cdcSourceTsField().orElse("").isBlank()) {
      rhs.setNewKey(lhs.isNewKey());
      return -1;
    }

    int result = Long.compare(lhs.cdcSourceTsValue(), rhs.cdcSourceTsValue());

    if (result == 0) {
      result =
          CDC_OPERATION_PRIORITY
              .getOrDefault(lhs.cdcOpValue(), -1)
              .compareTo(CDC_OPERATION_PRIORITY.getOrDefault(rhs.cdcOpValue(), -1));
    }

    return result;
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

    // Pre-scan: detect conditions that require allowIncompatibleChanges()
    boolean needsIncompatibleChanges = false;

    // 1. Detect timestamp type mismatches between existing table and CDC event schema.
    //    Snapshot path (jdbcTypeToIcebergType) may have created tables with timestamptz,
    //    while CDC path (StructSchemaConverter) produces timestamp without zone.
    //    Only timestamp<->timestamptz changes are safe; other type changes should fail.
    for (Types.NestedField newField : newSchema.columns()) {
      Types.NestedField existingField = icebergTable.schema().findField(newField.name());
      if (existingField != null && !existingField.type().equals(newField.type())) {
        if (isSafeTypeChange(existingField.type(), newField.type())) {
          LOGGER.info("Detected safe type change for '{}' in table {}: {} -> {} (allowing)",
              newField.name(), icebergTable.name(), existingField.type(), newField.type());
          needsIncompatibleChanges = true;
        }
      }
    }

    // 2. Detect PK fields that need requireColumn after unionByNameWith.
    // unionByNameWith adds ALL new columns as optional (SchemaUpdate.addColumn uses isOptional=true).
    // For existing columns, it may also widen required→optional.
    // We must re-require identifier fields in all these cases.
    Set<String> newIdentifierFieldNames = new java.util.HashSet<>(newSchema.identifierFieldNames());
    for (String idFieldName : newIdentifierFieldNames) {
      Types.NestedField existingField = icebergTable.schema().findField(idFieldName);
      Types.NestedField newField = newSchema.findField(idFieldName);
      if (existingField == null ||
          existingField.isOptional() ||
          (newField != null && newField.isOptional())) {
        needsIncompatibleChanges = true;
      }
    }

    // Build UpdateSchema — only allow incompatible changes when we detected safe cases above
    UpdateSchema us = icebergTable.updateSchema();
    if (needsIncompatibleChanges) {
      us.allowIncompatibleChanges();
    }
    us.unionByNameWith(newSchema);

    // Fix PK fields: ensure ALL identifier fields are required after unionByNameWith.
    // unionByNameWith adds new columns as optional and may widen existing required→optional.
    for (String idFieldName : newIdentifierFieldNames) {
      Types.NestedField existingField = icebergTable.schema().findField(idFieldName);
      Types.NestedField newField = newSchema.findField(idFieldName);
      if (existingField == null ||
          existingField.isOptional() ||
          (newField != null && newField.isOptional())) {
        LOGGER.info("Making PK column '{}' required in table {} (ensuring required for identifier field)",
            idFieldName, icebergTable.name());
        us.requireColumn(idFieldName);
      }
    }

    // Set identifier fields if present
    if (!newIdentifierFieldNames.isEmpty()) {
      us.setIdentifierFields(newSchema.identifierFieldNames());
    }

    // Make ALL non-identifier (non-PK) required fields optional.
    // Prevents NPE in Parquet writer when source data has NULL values for required fields.
    Set<Integer> identifierFieldIds = icebergTable.schema().identifierFieldIds();
    for (Types.NestedField existingField : icebergTable.schema().columns()) {
      if (existingField.isRequired()
          && !identifierFieldIds.contains(existingField.fieldId())
          && !newIdentifierFieldNames.contains(existingField.name())) {
        LOGGER.info("Making non-PK column '{}' optional in table {} (was required, fieldId={})",
            existingField.name(), icebergTable.name(), existingField.fieldId());
        us.makeColumnOptional(existingField.name());
      }
    }

    Schema newSchemaCombined = us.apply();

    // Avoid committing when there is no schema change
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
      icebergTable.refresh();
    }
  }

  /**
   * Checks if a type change between two Iceberg types is safe.
   * Safe changes are type mismatches caused by different code paths
   * (snapshot vs CDC) mapping the same database column to different Iceberg types.
   *
   * Safe cases:
   * - timestamp <-> timestamptz (snapshot uses withZone, CDC uses withoutZone)
   * - decimal <-> double/float (snapshot uses decimal from JDBC metadata, CDC may use double)
   * - int <-> long (JDBC reports int, CDC may widen to long)
   */
  private boolean isSafeTypeChange(org.apache.iceberg.types.Type existingType,
                                    org.apache.iceberg.types.Type newType) {
    // timestamp <-> timestamptz
    if (existingType instanceof Types.TimestampType && newType instanceof Types.TimestampType) {
      return true;
    }
    // decimal <-> double or float (numeric type mapping mismatch)
    if (isNumericType(existingType) && isNumericType(newType)) {
      return true;
    }
    // int <-> long
    if ((existingType instanceof Types.IntegerType && newType instanceof Types.LongType)
        || (existingType instanceof Types.LongType && newType instanceof Types.IntegerType)) {
      return true;
    }
    return false;
  }

  private boolean isNumericType(org.apache.iceberg.types.Type type) {
    return type instanceof Types.DecimalType
        || type instanceof Types.DoubleType
        || type instanceof Types.FloatType;
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
  public CommitResult addToTable(Table icebergTable, List<EventConverter> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (config.iceberg().upsert() && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!config.iceberg().allowFieldAddition()) {
      // if field additions not enabled add set of events to table
      return addToTablePerSchema(icebergTable, events);
    } else {
      Map<SchemaConverter, List<EventConverter>> eventsGroupedBySchema =
          events.stream().collect(Collectors.groupingBy(EventConverter::schemaConverter));
      LOGGER.debug(
          "Batch got {} records with {} different schema!!",
          events.size(),
          eventsGroupedBySchema.keySet().size());

      int files = 0;
      long bytes = 0;
      long records = 0;
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
        CommitResult r = addToTablePerSchema(icebergTable, schemaEvents.getValue());
        files += r.fileCount;
        bytes += r.totalBytes;
        records += r.totalRecords;
      }
      return new CommitResult(files, bytes, records);
    }
  }

  /**
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private CommitResult addToTablePerSchema(Table icebergTable, List<EventConverter> events) {
    // Initialize a task writer to write both INSERT and equality DELETE.
    final Schema tableSchema = icebergTable.schema();
    BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);
    int fileCount = 0;
    long totalBytes = 0;
    try (writer) {
      for (EventConverter e : events) {
        final RecordWrapper record =
            (config.iceberg().upsert() && !tableSchema.identifierFieldIds().isEmpty())
                ? e.convert(tableSchema)
                : e.convertAsAppend(tableSchema);
        writer.write(record);
      }

      WriteResult files = writer.complete();
      fileCount = files.dataFiles().length + files.deleteFiles().length;
      for (org.apache.iceberg.DataFile f : files.dataFiles()) {
        totalBytes += f.fileSizeInBytes();
      }
      for (org.apache.iceberg.DeleteFile f : files.deleteFiles()) {
        totalBytes += f.fileSizeInBytes();
      }
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
    } catch (IOException ex) {
      try {
        writer.abort();
      } catch (IOException e) {
        // pass
      }
      throw new DebeziumException(
          "Failed to write data to table:`" + icebergTable.name() + "`", ex);
    }

    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());

    // Emit OpenLineage output dataset metadata after successful commit
    try {
      emitOpenLineageEvent(icebergTable);
    } catch (Exception e) {
      LOGGER.debug("OpenLineage emission failed (non-critical): {}", e.getMessage());
    }

    return new CommitResult(fileCount, totalBytes, events.size());
  }

  /**
   * Writes a chunk of events to an already-open writer.
   * Does NOT close the writer or commit. Used by streaming snapshot flush.
   *
   * @param writer Open writer to write to
   * @param icebergTable The Iceberg table (for schema reference)
   * @param events Events to write
   */
  public void writeChunkToWriter(BaseTaskWriter<Record> writer, Table icebergTable,
                                  List<EventConverter> events) {

    if (config.iceberg().upsert() && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (config.iceberg().allowFieldAddition()) {
      Map<SchemaConverter, List<EventConverter>> eventsGroupedBySchema =
          events.stream().collect(Collectors.groupingBy(EventConverter::schemaConverter));

      for (Map.Entry<SchemaConverter, List<EventConverter>> schemaEvents :
          eventsGroupedBySchema.entrySet()) {
        applyFieldAddition(
            icebergTable,
            schemaEvents.getValue().get(0)
                .icebergSchema(config.iceberg().createIdentifierFields()));
      }
    }

    final Schema tableSchema = icebergTable.schema();
    try {
      for (EventConverter e : events) {
        final RecordWrapper record =
            (config.iceberg().upsert() && !tableSchema.identifierFieldIds().isEmpty())
                ? e.convert(tableSchema)
                : e.convertAsAppend(tableSchema);
        writer.write(record);
      }
    } catch (IOException ex) {
      throw new DebeziumException(
          "Failed to write chunk to table: " + icebergTable.name(), ex);
    }

    LOGGER.debug("Wrote {} events to open writer for table {}", events.size(), icebergTable.name());
  }

  /**
   * Closes the writer and commits the result to the Iceberg table.
   * Used by streaming snapshot flush when all chunks for a table have been written.
   *
   * @param writer The writer to close and commit
   * @param icebergTable The Iceberg table to commit to
   * @return CommitResult with file count, total bytes, and total records
   */
  public CommitResult commitWriter(BaseTaskWriter<Record> writer, Table icebergTable) {
    try {
      WriteResult files = writer.complete();

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

      int totalFiles = files.dataFiles().length + files.deleteFiles().length;
      long totalBytes = Arrays.stream(files.dataFiles())
          .mapToLong(f -> f.fileSizeInBytes()).sum();
      long totalRecords = Arrays.stream(files.dataFiles())
          .mapToLong(f -> f.recordCount()).sum();

      LOGGER.info("Committed writer for table {}: {} data files, {} delete files, {} bytes, {} records",
          icebergTable.name(), files.dataFiles().length, files.deleteFiles().length,
          totalBytes, totalRecords);

      try {
        emitOpenLineageEvent(icebergTable);
      } catch (Exception e) {
        LOGGER.debug("OpenLineage emission failed (non-critical): {}", e.getMessage());
      }

      return new CommitResult(totalFiles, totalBytes, totalRecords);
    } catch (IOException ex) {
      try {
        writer.abort();
      } catch (IOException e) {
        // pass
      }
      throw new DebeziumException(
          "Failed to commit writer for table: " + icebergTable.name(), ex);
    }
  }

  private ConnectorContext getOpenLineageContext() {
    if (openLineageContext == null) {
      openLineageContext = new ConnectorContext(
          "debezium-server-iceberg",
          "iceberg",
          "0",
          "1.0.0",
          UUID.randomUUID(),
          null);
    }
    return openLineageContext;
  }

  private void emitOpenLineageEvent(Table icebergTable) {
    List<DatasetMetadata.FieldDefinition> fields = icebergTable.schema().columns().stream()
        .map(f -> new DatasetMetadata.FieldDefinition(f.name(), f.type().toString(), ""))
        .toList();

    DatasetMetadata metadata = new DatasetMetadata(
        icebergTable.name(),
        DatasetMetadata.DatasetKind.OUTPUT,
        DatasetMetadata.TABLE_DATASET_TYPE,
        DatasetMetadata.DataStore.DATABASE,
        fields);

    DebeziumOpenLineageEmitter.emit(
        getOpenLineageContext(),
        DebeziumTaskState.RUNNING,
        List.of(metadata));
  }
}
