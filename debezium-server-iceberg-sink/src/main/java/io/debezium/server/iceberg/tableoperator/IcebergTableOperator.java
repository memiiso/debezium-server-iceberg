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
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.SchemaConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY = ImmutableMap.of(Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @Inject
  IcebergTableWriterFactory writerFactory;
  @Inject
  GlobalConfig config;

  protected List<EventConverter> deduplicateBatch(List<EventConverter> events) {

    ConcurrentHashMap<Object, EventConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e -> {
          if (!e.hasKeyData()) {
            throw new DebeziumException("Cannot deduplicate data with null key! destination:'" + e.destination() + "' event: '" + e.value().toString() + "'");
          }

          try {
            // deduplicate using key(PK)
            deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
              if (this.compareByTsThenOp(oldValue, newValue) <= 0) {
                return newValue;
              } else {
                return oldValue;
              }
            });
          } catch (Exception ex) {
            throw new DebeziumException("Failed to deduplicate events", ex);
          }
        }
    );

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   * <p>
   * Forex ample a record can be updated multiple times in the source. for example insert followed by update and
   * delete. for this case we need to only pick last change event for the row.
   * <p>
   * Its used when `upsert` feature enabled (when the consumer operating non append mode) which means it should not add
   * duplicate records to target table.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(EventConverter lhs, EventConverter rhs) {

    int result = Long.compare(lhs.cdcSourceTsValue(), rhs.cdcSourceTsValue());

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = CDC_OPERATION_PRIORITY.getOrDefault(lhs.cdcOpValue(), -1)
          .compareTo(
              CDC_OPERATION_PRIORITY.getOrDefault(rhs.cdcOpValue(), -1)
          );
    }

    return result;
  }

  /**
   * If given schema contains new fields compared to target table schema then it adds new fields to target iceberg
   * table.
   * <p>
   * Its used when allow field addition feature is enabled.
   *
   * @param icebergTable
   * @param newSchema
   */
  private void applyFieldAddition(Table icebergTable, Schema newSchema) {

    UpdateSchema us = icebergTable.updateSchema().
        unionByNameWith(newSchema).
        setIdentifierFields(newSchema.identifierFieldNames());
    Schema newSchemaCombined = us.apply();

    // @NOTE avoid committing when there is no schema change. commit creates new commit even when there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }
  }

  /**
   * Adds list of events to iceberg table.
   * <p>
   * If field addition enabled then it groups list of change events by their schema first. Then adds new fields to
   * iceberg table if there is any. And then follows with adding data to the table.
   * <p>
   * New fields are detected using CDC event schema, since events are grouped by their schemas it uses single
   * event to find-out schema for the whole list of events.
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
              .collect(Collectors.groupingBy(EventConverter::schemaConverter));
      LOGGER.debug("Batch got {} records with {} different schema!!", events.size(), eventsGroupedBySchema.keySet().size());

      for (Map.Entry<SchemaConverter, List<EventConverter>> schemaEvents : eventsGroupedBySchema.entrySet()) {
        // extend table schema if new fields found
        applyFieldAddition(icebergTable, schemaEvents.getValue().get(0).icebergSchema());
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
    try (writer) {
      for (EventConverter e : events) {
        final RecordWrapper record = (config.iceberg().upsert() && !tableSchema.identifierFieldIds().isEmpty()) ? e.convert(tableSchema) : e.convertAsAppend(tableSchema);
        if (config.iceberg().reselectUnavailableValuesOnDelete() && !tableSchema.identifierFieldIds().isEmpty() && record.op() == Operation.DELETE) {
            reselectUnavailableValues(icebergTable, record);
        }
        writer.write(record);
      }

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
    } catch (IOException ex) {
      try {
        writer.abort();
      } catch (IOException e) {
        // pass
      }
      throw new DebeziumException("Failed to write data to table:`" + icebergTable.name() + "`", ex);
    }

    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());
  }

  /**
   * Reselect unavailable values.
   *
   * @param icebergTable
   * @param record
   */
  private void reselectUnavailableValues(Table icebergTable, RecordWrapper record) throws IOException {
    final Schema tableSchema = icebergTable.schema();
    final String placeholderValue = config.debezium().unavailableValuePlaceholder();
    List<String> placeholders = new ArrayList<>();
    for (NestedField field : tableSchema.columns()) {
      if (field.type().typeId() == TypeID.STRING && placeholderValue.equals(record.getField(field.name()))) {
        placeholders.add(field.name());
      }
    }

    if (placeholders.size() > 0) {
      Expression primaryId = null;
      for (Integer fieldId : tableSchema.identifierFieldIds()) {
        String fieldName = tableSchema.findColumnName(fieldId);
        Expression fieldExpr = Expressions.equal(fieldName, record.getField(fieldName));
        primaryId = primaryId == null ? fieldExpr : Expressions.and(primaryId, fieldExpr);
      }
      if (primaryId == null) return;

      LOGGER.debug("Reselecting {} where {}", placeholders, primaryId);
      try (CloseableIterable<Record> results = IcebergGenerics.read(icebergTable).where(primaryId).select(placeholders).build()) {
        for (Record r : results) {
          for (String fieldName : placeholders) {
            Object fieldValue = r.getField(fieldName);
            LOGGER.debug("Reselected {}: {}", fieldName, fieldValue);
            record.setField(fieldName, fieldValue);
          }
          break;
        }
      }
    }
  }
}
