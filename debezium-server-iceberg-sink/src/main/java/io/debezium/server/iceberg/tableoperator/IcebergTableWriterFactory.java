package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergUtil;

import java.util.Set;

import jakarta.enterprise.context.Dependent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Table Writer Factory to get TaskWriter for the table. upsert modes used to return correct writer.
 *
 */
@Dependent
public class IcebergTableWriterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-keep-deletes", defaultValue = "true")
  boolean upsertKeepDeletes;

  public BaseTaskWriter<Record> create(Table icebergTable) {

    // file format of the table parquet, orc ...
    FileFormat format = IcebergUtil.getTableFileFormat(icebergTable);
    // appender factory
    GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(icebergTable);
    OutputFileFactory fileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, format);
    // equality Field Ids
    Set<Integer> equalityFieldIds = icebergTable.schema().identifierFieldIds();

    BaseTaskWriter<Record> writer;

    // 1. TABLE DONT HAVE identifierFieldIds
    // 2. OR RUNNING APPEND MODE
    // THEN USE APPEND WRITERS
    if (icebergTable.schema().identifierFieldIds().isEmpty() || !upsert) {

      if (upsert) {
        // running in upsert mode but table dont have identifierFieldIds to do delete!
        LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      }

      if (icebergTable.spec().isUnpartitioned()) {
        // table is un partitioned use un partitioned append writer
        writer = new UnpartitionedWriter<>(
            icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), Long.MAX_VALUE);

      } else {
        // table is partitioned use partitioned append writer
        writer = new PartitionedAppendWriter(
            icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), Long.MAX_VALUE, icebergTable.schema());
      }

    }

    // ITS UPSERT MODE
    // AND TABLE HAS identifierFieldIds
    // USE DELTA WRITERS
    else if (icebergTable.spec().isUnpartitioned()) {
      // running with upsert mode + un partitioned table
      writer = new UnpartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          Long.MAX_VALUE, icebergTable.schema(), equalityFieldIds, true, upsertKeepDeletes);
    } else {
      // running with upsert mode + partitioned table
      writer = new PartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          Long.MAX_VALUE, icebergTable.schema(), equalityFieldIds, true, upsertKeepDeletes);
    }

    return writer;
  }
}
