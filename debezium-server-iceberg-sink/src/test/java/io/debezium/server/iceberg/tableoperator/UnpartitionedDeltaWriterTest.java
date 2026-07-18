package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UnpartitionedDeltaWriterTest extends BaseWriterTest {

  @Test
  public void testUnpartitionedDeltaWriter() throws IOException {
    GenericFileWriterFactory fileWriterFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(format)
            .dataSchema(table.schema())
            .deleteFileFormat(format)
            .equalityFieldIds(ArrayUtil.toIntArray(new java.util.ArrayList<>(identifierFieldIds)))
            .equalityDeleteRowSchema(TypeUtil.select(table.schema(), identifierFieldIds))
            .build();

    UnpartitionedDeltaWriter writer =
        new UnpartitionedDeltaWriter(
            table.spec(),
            format,
            fileWriterFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            identifierFieldIds,
            true,
            false);

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", "123");
    row.setField("data", "hello world!");
    row.setField("id2", "123");
    row.setField("__op", "u");

    writer.write(new RecordWrapper(row, Operation.UPDATE, false));
    WriteResult result = writer.complete();

    // in upsert mode, each write is a delete + append, so we'll have 1 data file
    // and 1 delete file
    Assertions.assertEquals(result.dataFiles().length, 1);
    Assertions.assertEquals(result.dataFiles()[0].format(), format);
    Assertions.assertEquals(result.deleteFiles().length, 1);
    Assertions.assertEquals(result.deleteFiles()[0].format(), format);
  }
}
