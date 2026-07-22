package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.Tasks;

class PartitionedDeltaWriter extends BaseDeltaTaskWriter {

  private final PartitionKey partitionKey;

  private final Map<PartitionKey, RowDataDeltaWriter> writers = Maps.newHashMap();

  PartitionedDeltaWriter(
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
    this.partitionKey = new PartitionKey(spec, schema);
  }

  @Override
  RowDataDeltaWriter route(Record row) {
    partitionKey.partition(wrapper().wrap(row));

    RowDataDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in
      // writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RowDataDeltaWriter(copiedKey, dvFileWriter());
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  public void close() {
    try {
      Tasks.foreach(writers.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(RowDataDeltaWriter::close, IOException.class);

      writers.clear();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delta writer", e);
    }
  }
}
