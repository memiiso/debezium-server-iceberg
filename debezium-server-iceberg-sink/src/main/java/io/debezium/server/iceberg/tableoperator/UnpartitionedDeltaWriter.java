package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;

class UnpartitionedDeltaWriter extends BaseDeltaTaskWriter {
  private final RowDataDeltaWriter writer;

  UnpartitionedDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean keepDeletes,
      boolean useDv) {
    super(
        spec,
        format,
        fileWriterFactory,
        fileFactory,
        io,
        targetFileSize,
        schema,
        identifierFieldIds,
        keepDeletes,
        useDv);
    this.writer = new RowDataDeltaWriter(null, dvFileWriter());
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
