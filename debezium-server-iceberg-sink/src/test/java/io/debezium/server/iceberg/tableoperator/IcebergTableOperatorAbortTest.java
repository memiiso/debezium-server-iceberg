/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for writer cleanup when {@link IcebergTableOperator#addToTable} fails. */
class IcebergTableOperatorAbortTest {

  private IcebergTableOperator operator;
  private BaseTaskWriter<Record> writer;
  private Table table;
  private AppendFiles appendFiles;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws IOException {
    GlobalConfig globalConfig = mock(GlobalConfig.class);
    when(globalConfig.iceberg()).thenReturn(mock(IcebergConfig.class));

    writer = mock(BaseTaskWriter.class);
    when(writer.complete()).thenReturn(WriteResult.builder().build());

    appendFiles = mock(AppendFiles.class);
    table = mock(Table.class);
    when(table.name()).thenReturn("test_table");
    when(table.schema())
        .thenReturn(new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
    when(table.newAppend()).thenReturn(appendFiles);

    IcebergTableWriterFactory writerFactory = mock(IcebergTableWriterFactory.class);
    when(writerFactory.create(table)).thenReturn(writer);

    operator = new IcebergTableOperator();
    operator.config = globalConfig;
    operator.writerFactory = writerFactory;
  }

  @Test
  void testCommitStateUnknownKeepsFiles() throws IOException {
    CommitStateUnknownException failure =
        new CommitStateUnknownException(new RuntimeException("timeout"));
    doThrow(failure).when(appendFiles).commit();

    DebeziumException thrown =
        assertThrows(DebeziumException.class, () -> operator.addToTable(table, List.of()));

    assertSame(failure, thrown.getCause());
    // the commit may have succeeded, deleting the files would corrupt the table
    verify(writer, never()).abort();
  }

  @Test
  void testCleanableCommitFailureAbortsWriter() throws IOException {
    doThrow(new CommitFailedException("conflict")).when(appendFiles).commit();

    assertThrows(DebeziumException.class, () -> operator.addToTable(table, List.of()));

    verify(writer).abort();
  }

  @Test
  void testFailureBeforeCommitAbortsWriter() throws IOException {
    doThrow(new UncheckedIOException(new IOException("disk full"))).when(writer).write(any());

    assertThrows(
        DebeziumException.class,
        () -> operator.addToTable(table, List.of(mock(EventConverter.class))));

    verify(writer).abort();
    verify(table, never()).newAppend();
  }
}
