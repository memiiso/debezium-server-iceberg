package io.debezium.server.iceberg.tableoperator;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergUtil;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Table Writer Factory to get TaskWriter for the table. upsert modes used to return correct
 * writer.
 */
@Dependent
public class IcebergTableWriterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableWriterFactory.class);
  @Inject GlobalConfig config;

  public BaseTaskWriter<Record> create(Table icebergTable) {

    // file format of the table parquet, orc ...
    FileFormat format = IcebergUtil.getTableFileFormat(icebergTable);
    // writer factory
    FileWriterFactory<Record> fileWriterFactory = IcebergUtil.getTableWriterFactory(icebergTable);
    OutputFileFactory fileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, format);
    // equality Field Ids
    long targetFileSize =
        PropertyUtil.propertyAsLong(
            icebergTable.properties(),
            WRITE_TARGET_FILE_SIZE_BYTES,
            WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    if (!config.iceberg().upsert()) {
      // RUNNING APPEND MODE
      return appendWriter(icebergTable, format, fileWriterFactory, fileFactory, targetFileSize);
    } else if (icebergTable.schema().identifierFieldIds().isEmpty()) {
      // ITS UPSERT MODE BUT!!!!! TABLE DON'T HAVE identifierFieldIds(Primary Key)
      if (config.iceberg().upsert()) {
        LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      }
      return appendWriter(icebergTable, format, fileWriterFactory, fileFactory, targetFileSize);
    } else {
      // ITS UPSERT MODE AND TABLE HAS identifierFieldIds(Primary Key)
      // USE DELTA WRITERS
      return deltaWriter(icebergTable, format, fileFactory, targetFileSize);
    }
  }

  private BaseTaskWriter<Record> appendWriter(
      Table icebergTable,
      FileFormat format,
      FileWriterFactory<Record> fileWriterFactory,
      OutputFileFactory fileFactory,
      long targetFileSize) {

    if (icebergTable.spec().isUnpartitioned()) {
      // table is un partitioned use un partitioned append writer
      return new UnpartitionedAppendWriter(
          icebergTable.spec(),
          format,
          fileWriterFactory,
          fileFactory,
          icebergTable.io(),
          targetFileSize);

    } else {
      // table is partitioned use partitioned append writer
      return new PartitionedAppendWriter(
          icebergTable.spec(),
          format,
          fileWriterFactory,
          fileFactory,
          icebergTable.io(),
          targetFileSize,
          icebergTable.schema());
    }
  }

  private BaseTaskWriter<Record> deltaWriter(
      Table icebergTable, FileFormat format, OutputFileFactory fileFactory, long targetFileSize) {

    Set<Integer> identifierFieldIds = icebergTable.schema().identifierFieldIds();
    int formatVersion = TableUtil.formatVersion(icebergTable);
    boolean useDv = config.iceberg().useDv().orElse(formatVersion > 2);

    GenericFileWriterFactory fileWriterFactory =
        new GenericFileWriterFactory.Builder(icebergTable)
            .dataFileFormat(format)
            .dataSchema(icebergTable.schema())
            .deleteFileFormat(format)
            .equalityFieldIds(ArrayUtil.toIntArray(new java.util.ArrayList<>(identifierFieldIds)))
            .equalityDeleteRowSchema(TypeUtil.select(icebergTable.schema(), identifierFieldIds))
            .build();

    if (icebergTable.spec().isUnpartitioned()) {
      // running with upsert mode + un partitioned table
      return new UnpartitionedDeltaWriter(
          icebergTable.spec(),
          format,
          fileWriterFactory,
          fileFactory,
          icebergTable.io(),
          targetFileSize,
          icebergTable.schema(),
          identifierFieldIds,
          config.iceberg().keepDeletes(),
          useDv);
    } else {
      // running with upsert mode + partitioned table
      return new PartitionedDeltaWriter(
          icebergTable.spec(),
          format,
          fileWriterFactory,
          fileFactory,
          icebergTable.io(),
          targetFileSize,
          icebergTable.schema(),
          identifierFieldIds,
          config.iceberg().keepDeletes(),
          useDv);
    }
  }
}
