/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.iceberg.BatchConfig;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergChangeConsumer;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.batchsizewait.NoBatchSizeWait;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.deduplication.RetainLastBatchDeduplicator;
import io.debezium.server.iceberg.mapper.DefaultIcebergTableMapper;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.server.iceberg.tableoperator.IcebergTableWriterFactory;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MultiTableTransactionTest {

  private static final String NAMESPACE = "default";

  @TempDir Path tempDir;

  private InMemoryRestCatalog restCatalog;
  private GlobalConfig globalConfig;
  private IcebergConfig icebergConfig;
  private DebeziumConfig debeziumConfig;
  private BatchConfig batchConfig;
  private IcebergTableOperator tableOperator;
  private IcebergChangeConsumer consumer;

  @BeforeEach
  void setUp() throws Exception {
    restCatalog = new InMemoryRestCatalog(tempDir.resolve("warehouse").toString());
    restCatalog.createNamespace(Namespace.of(NAMESPACE));

    icebergConfig = mock(IcebergConfig.class);
    debeziumConfig = mock(DebeziumConfig.class);
    batchConfig = mock(BatchConfig.class);
    globalConfig = mock(GlobalConfig.class);

    lenient().when(globalConfig.iceberg()).thenReturn(icebergConfig);
    lenient().when(globalConfig.debezium()).thenReturn(debeziumConfig);
    lenient().when(globalConfig.batch()).thenReturn(batchConfig);

    lenient().when(icebergConfig.namespace()).thenReturn(NAMESPACE);
    lenient().when(icebergConfig.tablePrefix()).thenReturn(Optional.of("debeziumcdc_"));
    lenient().when(icebergConfig.destinationRegexp()).thenReturn(Optional.empty());
    lenient().when(icebergConfig.destinationRegexpReplace()).thenReturn(Optional.empty());
    lenient().when(icebergConfig.destinationUppercaseTableNames()).thenReturn(false);
    lenient().when(icebergConfig.destinationLowercaseTableNames()).thenReturn(false);
    lenient().when(icebergConfig.createIdentifierFields()).thenReturn(true);
    lenient().when(icebergConfig.nestedAsVariant()).thenReturn(false);
    lenient().when(icebergConfig.writeFormat()).thenReturn("parquet");
    lenient().when(icebergConfig.formatVersion()).thenReturn("2");
    lenient().when(icebergConfig.allowFieldAddition()).thenReturn(true);
    lenient().when(icebergConfig.upsert()).thenReturn(false);
    lenient().when(icebergConfig.keepDeletes()).thenReturn(true);
    lenient().when(icebergConfig.cdcOpField()).thenReturn("__op");
    lenient().when(icebergConfig.cdcSourceTsField()).thenReturn(Optional.of("__source_ts_ns"));
    lenient().when(icebergConfig.openlineageEnabled()).thenReturn(false);
    lenient().when(icebergConfig.multiTableTransaction()).thenReturn(true);
    lenient().when(icebergConfig.partitionBy()).thenReturn(Optional.empty());
    lenient()
        .when(icebergConfig.partitionByForTable(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(List.of());
    lenient()
        .when(icebergConfig.isPartitionByTableSpecific(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(false);

    lenient().when(debeziumConfig.keyValueChangeEventFormat()).thenReturn("json");
    lenient().when(debeziumConfig.eventSchemaEnabled()).thenReturn(true);
    lenient().when(debeziumConfig.isEventFlatteningEnabled()).thenReturn(true);
    lenient().when(debeziumConfig.topicHeartbeatSkipConsuming()).thenReturn(true);
    lenient()
        .when(debeziumConfig.isHeartbeatTopic(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(false);

    JsonEventConverter.initializeStaticSerdes();

    IcebergTableWriterFactory writerFactory = new IcebergTableWriterFactory();
    setField(writerFactory, "config", globalConfig);

    tableOperator = spy(new IcebergTableOperator());
    setField(tableOperator, "config", globalConfig);
    setField(tableOperator, "writerFactory", writerFactory);
    setField(tableOperator, "deduplicator", new RetainLastBatchDeduplicator());

    DefaultIcebergTableMapper mapper = new DefaultIcebergTableMapper();
    setField(mapper, "config", globalConfig);

    consumer = new IcebergChangeConsumer();
    setField(consumer, "config", globalConfig);
    setField(consumer, "icebergCatalog", restCatalog);
    setField(consumer, "icebergTableOperator", tableOperator);
    setField(consumer, "tableMapper", mapper);
    setField(consumer, "batchSizeWait", new NoBatchSizeWait());
    setField(consumer, "numConcurrentUploads", 1);
    setField(consumer, "concurrentUploadsTimeoutMinutes", 5);
    setStaticField(IcebergChangeConsumer.class, "keyValueChangeEventFormat", "json");
  }

  @Test
  void testFailFastWhenCatalogIsNotRestCatalog() throws Exception {
    InMemoryCatalog nonRestCatalog = new InMemoryCatalog();
    nonRestCatalog.initialize(
        "in-memory",
        Map.of(CatalogProperties.WAREHOUSE_LOCATION, tempDir.resolve("wh2").toString()));
    setField(consumer, "icebergCatalog", nonRestCatalog);

    DebeziumException ex =
        assertThrows(
            DebeziumException.class, () -> consumer.validateMultiTableTransactionSupport());
    assertTrue(ex.getMessage().contains("RESTCatalog"));
  }

  @Test
  void testMultiTableTransactionSequential() throws Exception {
    BatchEvent event1 = createBatchEvent("inventory.customers", 1, "Alice", 101L);
    BatchEvent event2 = createBatchEvent("inventory.orders", 10, "Order-10", 102L);
    CapturingEvents<BatchEvent> batch = createCapturingEvents(List.of(event1, event2));

    consumer.handle(batch);

    // 1. Verify single atomic commitTransaction call containing both data tables
    assertEquals(1, restCatalog.committedBatches.size());
    List<TableCommit> commits = restCatalog.committedBatches.get(0);
    Set<TableIdentifier> committedTables =
        commits.stream().map(TableCommit::identifier).collect(Collectors.toSet());
    assertEquals(
        Set.of(
            TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_customers"),
            TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_orders")),
        committedTables);

    // 2. Verify data in both tables
    assertEquals(
        1,
        readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_customers")).size());
    assertEquals(
        1, readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_orders")).size());

    // 3. Verify Debezium BatchEvents were marked committed after transaction commit
    verify(event1, times(1)).commit();
    verify(event2, times(1)).commit();
  }

  @Test
  void testMultiTableTransactionParallel() throws Exception {
    setField(consumer, "numConcurrentUploads", 4);
    setField(consumer, "concurrencyLimiter", new Semaphore(4));

    BatchEvent event1 = createBatchEvent("inventory.customers", 1, "Alice", 201L);
    BatchEvent event2 = createBatchEvent("inventory.orders", 10, "Order-10", 202L);
    BatchEvent event3 = createBatchEvent("inventory.products", 100, "Widget", 203L);
    CapturingEvents<BatchEvent> batch = createCapturingEvents(List.of(event1, event2, event3));

    consumer.handle(batch);

    assertEquals(1, restCatalog.committedBatches.size());
    List<TableCommit> commits = restCatalog.committedBatches.get(0);
    Set<TableIdentifier> committedTables =
        commits.stream().map(TableCommit::identifier).collect(Collectors.toSet());
    assertEquals(
        Set.of(
            TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_customers"),
            TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_orders"),
            TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_products")),
        committedTables);

    assertEquals(
        1,
        readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_customers")).size());
    assertEquals(
        1, readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_orders")).size());
    assertEquals(
        1,
        readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_products")).size());

    verify(event1, times(1)).commit();
    verify(event2, times(1)).commit();
    verify(event3, times(1)).commit();
  }

  @Test
  void testMultiTableTransactionRollbackOnCommitFailure() throws Exception {
    // Batch 1 succeeds
    BatchEvent b1Event = createBatchEvent("inventory.customers", 1, "Alice", 100L);
    consumer.handle(createCapturingEvents(List.of(b1Event)));
    assertEquals(1, restCatalog.committedBatches.size());

    // Batch 2 fails during commitTransaction
    restCatalog.failNextCommitTransaction = true;
    BatchEvent b2Event1 = createBatchEvent("inventory.customers", 2, "Bob", 200L);
    BatchEvent b2Event2 = createBatchEvent("inventory.orders", 20, "Order-20", 201L);

    assertThrows(
        CommitFailedException.class,
        () -> consumer.handle(createCapturingEvents(List.of(b2Event1, b2Event2))));

    // Data tables must still reflect only Batch 1
    assertEquals(
        1,
        readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_customers")).size());
    assertEquals(
        0, readTableRecords(TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_orders")).size());

    // Batch 2 events must NOT be committed to Debezium
    verify(b2Event1, never()).commit();
    verify(b2Event2, never()).commit();
  }

  @Test
  void testMultipleTopicsMappedToSameTableCommittedOncePerBatch() throws Exception {
    when(icebergConfig.destinationRegexp()).thenReturn(Optional.of("_shard\\d+"));
    when(icebergConfig.destinationRegexpReplace()).thenReturn(Optional.of(""));

    BatchEvent shard1Event = createBatchEvent("inventory.orders_shard1", 1, "Order-1", 301L);
    BatchEvent shard2Event = createBatchEvent("inventory.orders_shard2", 2, "Order-2", 302L);

    consumer.handle(createCapturingEvents(List.of(shard1Event, shard2Event)));

    TableIdentifier mappedTableId = TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_orders");
    List<Record> rows = readTableRecords(mappedTableId);
    assertEquals(2, rows.size());

    // addToTable must be invoked only ONCE for the mapped table in this batch
    verify(tableOperator, times(1))
        .addToTable(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
  }

  @Test
  void testSchemaEvolutionInsideMultiTableTransaction() throws Exception {
    // Batch 1 creates table with (id, first_name)
    BatchEvent event1 = createBatchEvent("inventory.customers", 1, "Alice", 100L);
    consumer.handle(createCapturingEvents(List.of(event1)));

    // Batch 2 adds a new column 'email' and writes to a second table in the same transaction
    BatchEvent event2WithNewCol =
        createBatchEventWithExtraField("inventory.customers", 2, "Bob", "bob@example.com", 200L);
    BatchEvent orderEvent = createBatchEvent("inventory.orders", 10, "Order-10", 201L);
    consumer.handle(createCapturingEvents(List.of(event2WithNewCol, orderEvent)));

    assertEquals(2, restCatalog.committedBatches.size());
    TableIdentifier customersId = TableIdentifier.of(NAMESPACE, "debeziumcdc_inventory_customers");
    Table customersTable = restCatalog.loadTable(customersId);
    assertNotNull(customersTable.schema().findField("email"));

    List<Record> rows = readTableRecords(customersId);
    assertEquals(2, rows.size());
  }

  @SuppressWarnings("unchecked")
  private CapturingEvents<BatchEvent> createCapturingEvents(List<BatchEvent> records) {
    CapturingEvents<BatchEvent> batch = mock(CapturingEvents.class);
    when(batch.records()).thenReturn(records);
    return batch;
  }

  private BatchEvent createBatchEvent(String destination, int id, String name, long lsn) {
    String key =
        """
        {
          "schema": {
            "type": "struct",
            "fields": [
              {"type": "int32", "optional": false, "field": "id"}
            ],
            "optional": false
          },
          "payload": {"id": %d}
        }
        """
            .formatted(id);
    String val =
        """
        {
          "schema": {
            "type": "struct",
            "fields": [
              {"type": "int32", "optional": false, "field": "id"},
              {"type": "string", "optional": true, "field": "first_name"},
              {"type": "string", "optional": true, "field": "__op"},
              {"type": "int64", "optional": true, "field": "__source_ts_ns"},
              {"type": "boolean", "optional": true, "field": "__deleted"}
            ],
            "optional": false
          },
          "payload": {
            "id": %d,
            "first_name": "%s",
            "__op": "c",
            "__source_ts_ns": %d,
            "__deleted": false
          }
        }
        """
            .formatted(id, name, lsn);

    BatchEvent batchEvent = mock(BatchEvent.class);
    lenient().when(batchEvent.destination()).thenReturn(destination);
    lenient().when(batchEvent.key()).thenReturn(key);
    lenient().when(batchEvent.value()).thenReturn(val);
    return batchEvent;
  }

  private BatchEvent createBatchEventWithExtraField(
      String destination, int id, String name, String email, long lsn) {
    String key =
        """
        {
          "schema": {
            "type": "struct",
            "fields": [
              {"type": "int32", "optional": false, "field": "id"}
            ],
            "optional": false
          },
          "payload": {"id": %d}
        }
        """
            .formatted(id);
    String val =
        """
        {
          "schema": {
            "type": "struct",
            "fields": [
              {"type": "int32", "optional": false, "field": "id"},
              {"type": "string", "optional": true, "field": "first_name"},
              {"type": "string", "optional": true, "field": "email"},
              {"type": "string", "optional": true, "field": "__op"},
              {"type": "int64", "optional": true, "field": "__source_ts_ns"},
              {"type": "boolean", "optional": true, "field": "__deleted"}
            ],
            "optional": false
          },
          "payload": {
            "id": %d,
            "first_name": "%s",
            "email": "%s",
            "__op": "c",
            "__source_ts_ns": %d,
            "__deleted": false
          }
        }
        """
            .formatted(id, name, email, lsn);

    BatchEvent batchEvent = mock(BatchEvent.class);
    lenient().when(batchEvent.destination()).thenReturn(destination);
    lenient().when(batchEvent.key()).thenReturn(key);
    lenient().when(batchEvent.value()).thenReturn(val);
    return batchEvent;
  }

  private List<Record> readTableRecords(TableIdentifier tableId) throws Exception {
    Table table = restCatalog.loadTable(tableId);
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      return Lists.newArrayList(iterable);
    }
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static void setStaticField(Class<?> clazz, String fieldName, Object value)
      throws Exception {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(null, value);
  }

  /**
   * Lightweight in-memory {@link RESTCatalog} for testing multi-table transactions without Docker.
   */
  static class InMemoryRestCatalog extends RESTCatalog {
    private final InMemoryCatalog delegate = new InMemoryCatalog();
    final List<List<TableCommit>> committedBatches = new ArrayList<>();
    boolean failNextCommitTransaction = false;

    InMemoryRestCatalog(String warehouseLocation) {
      delegate.initialize(
          "test-rest", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
      delegate.createNamespace(namespace, meta);
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
      return delegate.namespaceExists(namespace);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return delegate.listTables(namespace);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
      return delegate.tableExists(identifier);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      return delegate.loadTable(identifier);
    }

    @Override
    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        String location,
        Map<String, String> properties) {
      return delegate.createTable(identifier, schema, spec, location, properties);
    }

    @Override
    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties) {
      return delegate.createTable(identifier, schema, spec, properties);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema) {
      return delegate.createTable(identifier, schema);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
      return delegate.buildTable(identifier, schema);
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {
      delegate.invalidateTable(identifier);
    }

    @Override
    public void commitTransaction(List<TableCommit> commits) {
      if (failNextCommitTransaction) {
        failNextCommitTransaction = false;
        throw new CommitFailedException("Simulated atomic multi-table commit failure");
      }
      for (TableCommit tc : commits) {
        BaseTable baseTable = (BaseTable) delegate.loadTable(tc.identifier());
        TableMetadata current = baseTable.operations().current();
        TableMetadata.Builder builder = TableMetadata.buildFrom(current);
        tc.updates().forEach(u -> u.applyTo(builder));
        baseTable.operations().commit(current, builder.build());
      }
      committedBatches.add(List.copyOf(commits));
    }
  }
}
