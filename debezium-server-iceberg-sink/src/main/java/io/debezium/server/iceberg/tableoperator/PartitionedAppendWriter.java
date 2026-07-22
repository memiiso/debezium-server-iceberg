package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.Tasks;

public class PartitionedAppendWriter extends BaseTaskWriter<Record> {
  private final PartitionKey partitionKey;
  private final InternalRecordWrapper wrapper;
  private final Map<PartitionKey, RollingFileWriter> writers = Maps.newHashMap();

  public PartitionedAppendWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema) {
    super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize);
    this.partitionKey = new PartitionKey(spec, schema);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
  }

  @Override
  public void write(Record row) throws IOException {
    partitionKey.partition(wrapper.wrap(row));

    RollingFileWriter writer = writers.get(partitionKey);
    if (writer == null) {
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RollingFileWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    writer.write(row);
  }

  @Override
  public void close() throws IOException {
    Tasks.foreach(writers.values())
        .throwFailureWhenFinished()
        .noRetry()
        .run(RollingFileWriter::close, IOException.class);
  }
}
