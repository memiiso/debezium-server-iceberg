package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;

public class UnpartitionedAppendWriter extends BaseTaskWriter<Record> {
  private final RollingFileWriter writer;

  public UnpartitionedAppendWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize) {
    super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize);
    this.writer = new RollingFileWriter(null);
  }

  @Override
  public void write(Record row) throws IOException {
    writer.write(row);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
