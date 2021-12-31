/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergChangeEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper to perform operations in iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-dedup-column", defaultValue = "__source_ts_ms")
  String sourceTsMsColumn;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-op-column", defaultValue = "__op")
  String opColumn;
  @ConfigProperty(name = "debezium.sink.iceberg.allow-field-addition", defaultValue = "true")
  boolean allowFieldAddition;
  @Inject
  IcebergTableWriterFactory writerFactory;

  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;

  private List<IcebergChangeEvent> deduplicateBatch(List<IcebergChangeEvent> events) {

    ConcurrentHashMap<JsonNode, IcebergChangeEvent> icebergRecordsmap = new ConcurrentHashMap<>();

    for (IcebergChangeEvent e : events) {

      // deduplicate using key(PK) @TODO improve using map.merge
      if (icebergRecordsmap.containsKey(e.key())) {

        // replace it if it's new
        if (this.compareByTsThenOp(icebergRecordsmap.get(e.key()).value(), e.value()) <= 0) {
          icebergRecordsmap.put(e.key(), e);
        }

      } else {
        icebergRecordsmap.put(e.key(), e);
      }

    }
    return new ArrayList<>(icebergRecordsmap.values());
  }


  private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {

    int result = Long.compare(lhs.get(sourceTsMsColumn).asLong(0), rhs.get(sourceTsMsColumn).asLong(0));

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = cdcOperations.getOrDefault(lhs.get(opColumn).asText("c"), -1)
          .compareTo(
              cdcOperations.getOrDefault(rhs.get(opColumn).asText("c"), -1)
          );
    }

    return result;
  }

  private void applyFieldAddition(Table icebergTable, Schema newSchema) {

    UpdateSchema us = icebergTable.updateSchema().
        unionByNameWith(newSchema).
        setIdentifierFields(newSchema.identifierFieldNames());
    // @TODO add UNIT TEST PK change!!
    Schema newSchemaCombined = us.apply();

    // @NOTE avoid committing when there is no schema change. commit creates new commit even when there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }
    // @TODO update sort order?? us.withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
  }

  public void addToTable(Table icebergTable, List<IcebergChangeEvent> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!allowFieldAddition) {
      // if field additions not enabled add set of events to table
      addToTablePerSchema(icebergTable, events);
    } else {
      Map<IcebergChangeEvent.JsonSchema, List<IcebergChangeEvent>> eventsGroupedBySchema =
          events.stream()
              .collect(Collectors.groupingBy(IcebergChangeEvent::jsonSchema));
      LOGGER.debug("Batch got {} records with {} different schema!!", events.size(), eventsGroupedBySchema.keySet().size());

      for (Map.Entry<IcebergChangeEvent.JsonSchema, List<IcebergChangeEvent>> schemaEvents : eventsGroupedBySchema.entrySet()) {
        // extend table schema if new fields found
        applyFieldAddition(icebergTable, schemaEvents.getKey().icebergSchema());
        // add set of events to table
        addToTablePerSchema(icebergTable, schemaEvents.getValue());
      }
    }

  }

  private void addToTablePerSchema(Table icebergTable, List<IcebergChangeEvent> events) {
    // Initialize a task writer to write both INSERT and equality DELETE.
    BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);
    try {
      for (IcebergChangeEvent e : events) {
        writer.write(e.asIcebergRecord(icebergTable.schema()));
      }

      writer.close();
      WriteResult files = writer.complete();
      RowDelta newRowDelta = icebergTable.newRowDelta();
      Arrays.stream(files.dataFiles()).forEach(newRowDelta::addRows);
      Arrays.stream(files.deleteFiles()).forEach(newRowDelta::addDeletes);
      newRowDelta.commit();

    } catch (IOException ex) {
      throw new DebeziumException(ex);
    }

    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());
  }
}
