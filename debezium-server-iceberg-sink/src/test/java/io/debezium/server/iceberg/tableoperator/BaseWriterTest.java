package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergUtil;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseWriterTest {

  protected InMemoryFileIO fileIO;
  protected Table table;
  FileFormat format;
  GenericAppenderFactory appenderFactory;
  OutputFileFactory fileFactory;
  Set<Integer> equalityFieldIds;

  protected static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.StringType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.StringType.get()),
              Types.NestedField.required(4, "__op", Types.StringType.get())
          ),
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
    appenderFactory = IcebergUtil.getTableAppender(table);
    fileFactory = IcebergUtil.getTableOutputFileFactory(table, format);
    equalityFieldIds = table.schema().identifierFieldIds();
  }

}
