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
import java.util.concurrent.ConcurrentHashMap;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
public class IcebergTableOperator {

  static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-dedup-column", defaultValue = "__source_ts_ms")
  String sourceTsMsColumn;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-op-column", defaultValue = "__op")
  String opColumn;
  @Inject
  IcebergTableWriterFactory writerFactory;

  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;

  private ArrayList<Record> deduplicatedBatchRecords(Schema schema, List<IcebergChangeEvent> events) {
    ConcurrentHashMap<Object, GenericRecord> icebergRecordsmap = new ConcurrentHashMap<>();

    for (IcebergChangeEvent e : events) {
      GenericRecord icebergRecord = e.asIcebergRecord(schema);

      // deduplicate over key(PK)
      if (icebergRecordsmap.containsKey(e.key())) {

        // replace it if it's new
        if (this.compareByTsThenOp(icebergRecordsmap.get(e.key()), icebergRecord) <= 0) {
          icebergRecordsmap.put(e.key(), icebergRecord);
        }

      } else {
        icebergRecordsmap.put(e.key(), icebergRecord);
      }

    }
    return new ArrayList<>(icebergRecordsmap.values());
  }

  private int compareByTsThenOp(GenericRecord lhs, GenericRecord rhs) {

    int result = Long.compare((Long) lhs.getField(sourceTsMsColumn), (Long) rhs.getField(sourceTsMsColumn));

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = cdcOperations.getOrDefault(lhs.getField(opColumn), -1)
          .compareTo(
              cdcOperations.getOrDefault(rhs.getField(opColumn), -1)
          );
    }

    return result;
  }

  public void addToTable(Table icebergTable, List<IcebergChangeEvent> events) {
    // Initialize a task writer to write both INSERT and equality DELETE.
    BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);
    try {
      if (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) {
        ArrayList<Record> icebergRecords = deduplicatedBatchRecords(icebergTable.schema(), events);
        for (Record icebergRecord : icebergRecords) {
          writer.write(icebergRecord);
        }
      } else {
        for (IcebergChangeEvent e : events) {
          writer.write(e.asIcebergRecord(icebergTable.schema()));
        }
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
