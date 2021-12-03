/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergChangeEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
@Named("IcebergTableOperatorUpsert")
public class IcebergTableOperatorUpsert extends AbstractIcebergTableOperator {

  static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperatorUpsert.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-dedup-column", defaultValue = "__source_ts_ms")
  String sourceTsMsColumn;

  @ConfigProperty(name = "debezium.sink.iceberg.upsert-keep-deletes", defaultValue = "true")
  boolean upsertKeepDeletes;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-op-column", defaultValue = "__op")
  String opColumn;

  @Inject
  IcebergTableOperatorAppend icebergTableAppend;


  @Override
  public void initialize() {
    super.initialize();
    icebergTableAppend.initialize();
  }

  private Optional<DeleteFile> getDeleteFile(Table icebergTable, ArrayList<Record> icebergRecords) {
    
    FileFormat fileFormat = getFileFormat(icebergTable);
    GenericAppenderFactory appender = getAppender(icebergTable);
    final String fileName = "del-" + UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + fileFormat.name();
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));
    EncryptedOutputFile eout = icebergTable.encryption().encrypt(out);

    EqualityDeleteWriter<Record> edw = appender.newEqDeleteWriter(eout, fileFormat, null);

    // anything is not an insert.
    // upsertKeepDeletes = false, which means delete deletes
    List<Record> deleteRows = icebergRecords.stream()
        .filter(r ->
                // anything is not an insert.
                !r.getField(opColumn).equals("c")
            // upsertKeepDeletes = false and its deleted record, which means delete deletes
            // || !(upsertKeepDeletes && r.getField(opColumn).equals("d"))
        ).collect(Collectors.toList());

    if (deleteRows.size() == 0) {
      return Optional.empty();
    }

    edw.deleteAll(deleteRows);

    try {
      edw.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOGGER.debug("Creating iceberg equality delete file!");
    return Optional.of(edw.toDeleteFile());
  }

  private ArrayList<Record> toDeduppedIcebergRecords(Schema schema, List<IcebergChangeEvent> events) {
    ConcurrentHashMap<Object, GenericRecord> icebergRecordsmap = new ConcurrentHashMap<>();

    for (IcebergChangeEvent e : events) {
      GenericRecord icebergRecord = e.getIcebergRecord(schema);

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

  private int compareByTsThenOp(GenericRecord lhs, GenericRecord rhs) {
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

  @Override
  public void addToTable(Table icebergTable, List<IcebergChangeEvent> events) {

    if (icebergTable.sortOrder().isUnsorted()) {
      LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      // call append here!
      icebergTableAppend.addToTable(icebergTable, events);
      return;
    }

    // DO UPSERT >>> DELETE + INSERT
    ArrayList<Record> icebergRecords = toDeduppedIcebergRecords(icebergTable.schema(), events);
    DataFile dataFile = getDataFile(icebergTable, icebergRecords);
    Optional<DeleteFile> deleteDataFile = getDeleteFile(icebergTable, icebergRecords);
    LOGGER.debug("Committing new file as Upsert (has deletes:{}) '{}' !", deleteDataFile.isPresent(), dataFile.path());
    RowDelta c = icebergTable
        .newRowDelta()
        .addRows(dataFile);
    deleteDataFile.ifPresent(deleteFile -> c.addDeletes(deleteFile).validateDeletedFiles());

    c.commit();
    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());
  }

  @Override
  public Predicate<Record> filterEvents() {
    return p ->
        // if its upsert and upsertKeepDeletes = true
        upsertKeepDeletes
            // if not then exclude deletes
            || !(p.getField(opColumn).equals("d"));
  }

}
