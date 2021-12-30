package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import java.util.List;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

class UnpartitionedDeltaWriter extends BaseDeltaTaskWriter {
  private final RowDataDeltaWriter writer;

  UnpartitionedDeltaWriter(PartitionSpec spec,
                           FileFormat format,
                           FileAppenderFactory<Record> appenderFactory,
                           OutputFileFactory fileFactory,
                           FileIO io,
                           long targetFileSize,
                           Schema schema,
                           List<Integer> equalityFieldIds,
                           boolean upsert,
                           boolean upsertKeepDeletes) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, equalityFieldIds, upsert, upsertKeepDeletes);
    this.writer = new RowDataDeltaWriter(null);
  }

  @Override
  RowDataDeltaWriter route(Record row) {
    return writer;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
