package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergUtil;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.enterprise.context.Dependent;

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

@Dependent
public class IcebergTableWriterFactory {
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-keep-deletes", defaultValue = "true")
  boolean upsertKeepDeletes;

  public BaseTaskWriter<Record> create(Table icebergTable) {

    FileFormat format = IcebergUtil.getTableFileFormat(icebergTable);
    GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(icebergTable);
    int partitionId = Integer.parseInt(dtFormater.format(Instant.now()));
    OutputFileFactory fileFactory = OutputFileFactory.builderFor(icebergTable, partitionId, 1L)
        .defaultSpec(icebergTable.spec()).format(format).build();
    List<Integer> equalityFieldIds = new ArrayList<>(icebergTable.schema().identifierFieldIds());

    BaseTaskWriter<Record> writer;
    if (icebergTable.schema().identifierFieldIds().isEmpty() || !upsert) {
      if (upsert) {
        LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      }
      if (icebergTable.spec().isUnpartitioned()) {
        writer = new UnpartitionedWriter<>(
            icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), Long.MAX_VALUE);
      } else {
        writer = new IcebergPartitionedWriter(
            icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), Long.MAX_VALUE, icebergTable.schema());
      }
    } else if (icebergTable.spec().isUnpartitioned()) {
      writer = new UnpartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          Long.MAX_VALUE, icebergTable.schema(), equalityFieldIds, true, upsertKeepDeletes);
    } else {
      writer = new PartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          Long.MAX_VALUE, icebergTable.schema(), equalityFieldIds, true, upsertKeepDeletes);
    }

    return writer;
  }
}
