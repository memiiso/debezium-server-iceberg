package io.debezium.server.iceberg.tableoperator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.debezium.server.iceberg.IcebergUtil;
import java.util.Set;
import org.apache.iceberg.*;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;

public class BaseWriterTest {

  protected InMemoryFileIO fileIO;
  protected Table table;
  FileFormat format;
  FileWriterFactory<Record> fileWriterFactory;
  OutputFileFactory fileFactory;
  Set<Integer> identifierFieldIds;

  protected static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.StringType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.StringType.get()),
              Types.NestedField.required(4, "__op", Types.StringType.get())),
          ImmutableSet.of(1, 3));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();

  @BeforeEach
  public void before() {
    fileIO = new InMemoryFileIO();

    table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.io()).thenReturn(fileIO);
    when(table.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(table.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(table.properties()).thenReturn(ImmutableMap.of());

    format = IcebergUtil.getTableFileFormat(table);
    fileWriterFactory = IcebergUtil.getTableWriterFactory(table);
    fileFactory = IcebergUtil.getTableOutputFileFactory(table, format);
    identifierFieldIds = table.schema().identifierFieldIds();
  }
}
