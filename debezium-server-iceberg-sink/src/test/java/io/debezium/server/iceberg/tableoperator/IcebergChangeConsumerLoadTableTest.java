package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.BaseTest;
import io.debezium.server.iceberg.IcebergChangeConsumerTest;
import io.debezium.server.iceberg.IcebergTableConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerTest.TestProfile.class)
class IcebergChangeConsumerLoadTableTest extends BaseTest {
    final String schemaName = "inventory";

    @Test
    void testLoadIcebergTable_partitionSpecValidation() {
        String tableName = "test_table_partitioned";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert disabled and config with global partitionBy = ["id"]
        Mockito.when(icebergConfig.upsert()).thenReturn(false);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.of(List.of("id")));
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("data", "record1")
                .build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec contains 'id'
        Assertions.assertTrue(
                table.spec().fields().stream().anyMatch(f -> f.name().equals("id")),
                "Partition spec should contain 'id' field"
        );
    }

    @Test
    void testLoadIcebergTable_unpartitionedSpecValidation() {
        String tableName = "test_table_unpartitioned";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert disabled and config with no global partitionBy
        Mockito.when(icebergConfig.upsert()).thenReturn(false);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.empty());
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("data", "record1")
                .build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec is unpartitioned
        Assertions.assertTrue(table.spec().isUnpartitioned(), "Partition spec should be unpartitioned");
    }

    @Test
    void testLoadIcebergTable_upsertWithGlobalPartitionBy() {
        String tableName = "test_table_upsert_global";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert enabled and global partitionBy
        Mockito.when(icebergConfig.upsert()).thenReturn(true);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.of(List.of("id", "data")));
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("data", "record1").build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec contains both 'id' and 'data'
        Assertions.assertTrue(
                table.spec().fields().stream().anyMatch(f -> f.name().equals("id")),
                "Partition spec should contain 'id' field"
        );
        Assertions.assertTrue(
                table.spec().fields().stream().anyMatch(f -> f.name().equals("data")),
                "Partition spec should contain 'data' field"
        );
    }

    @Test
    void testLoadIcebergTable_upsertWithEmptyGlobalPartitionBy() {
        String tableName = "test_table_upsert_global";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert enabled and global partitionBy
        Mockito.when(icebergConfig.upsert()).thenReturn(true);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.of(List.of()));
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("data", "record1").build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec is unpartitioned
        Assertions.assertTrue(table.spec().isUnpartitioned(), "Partition spec should be unpartitioned");
    }

    @Test
    void testLoadIcebergTable_upsertWithTableLevelPartitionBy() {
        String tableName = "test_table_upsert_tablelevel";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert enabled and table-level partitionBy
        Mockito.when(icebergConfig.upsert()).thenReturn(true);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.empty());
        Map<String, IcebergTableConfig> tableConfigMap = Map.of(fullTableName, () -> Optional.of(List.of("user_name")));
        Mockito.when(icebergConfig.tableConfigs()).thenReturn(tableConfigMap);
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("user_name", "Alice")
                .addField("data", "record1")
                .build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec contains 'user_name' only
        Assertions.assertTrue(
                table.spec().fields().stream().anyMatch(f -> f.name().equals("user_name")),
                "Partition spec should contain 'user_name' field"
        );
        Assertions.assertFalse(
                table.spec().fields().stream().anyMatch(f -> f.name().equals("id")),
                "Partition spec should not contain 'id' field if not specified"
        );
    }

    @Test
    void testLoadIcebergTable_tableLevelPartitionByWhenUpsertFalse() {
        String tableName = "test_table_append_tablelevel";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert disabled and table-level partitionBy
        Mockito.when(icebergConfig.upsert()).thenReturn(false);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.empty());
        Map<String, IcebergTableConfig> tableConfigMap = Map.of(fullTableName, () -> Optional.of(List.of("user_name")));
        Mockito.when(icebergConfig.tableConfigs()).thenReturn(tableConfigMap);
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("user_name", "Bob")
                .addField("data", "record1")
                .build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec is unpartitioned (when upsert is false)
        Assertions.assertTrue(table.spec().isUnpartitioned(), "Partition spec should be unpartitioned");
    }

    @Test
    void testLoadIcebergTable_unpartitionedWhenUpsertFalse() {
        String tableName = "test_table_tablelevel_false";
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        // Mock upsert disabled and table-level partitionBy
        Mockito.when(icebergConfig.upsert()).thenReturn(false);
        Mockito.when(icebergConfig.partitionBy()).thenReturn(Optional.empty());
        Mockito.when(icebergConfig.tableConfigs()).thenReturn(null);
        TableIdentifier tableId = consumer.mapDestination(fullTableName);
        EventConverter event = eventBuilder.destination(fullTableName)
                .addKeyField("id", 1)
                .addField("user_name", "Bob")
                .addField("data", "record1")
                .build();
        Table table = consumer.loadIcebergTable(tableId, event);
        Assertions.assertNotNull(table, "Table should be created");
        // Validate partition spec is unpartitioned
        Assertions.assertTrue(table.spec().isUnpartitioned(), "Partition spec should be unpartitioned");
    }
}
