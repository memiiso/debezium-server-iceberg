package io.debezium.server.iceberg;

import io.smallrye.config.SmallRyeConfigBuilder;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergConfigTest {

    private IcebergConfig getIcebergConfig(Map<String, String> configProps) {
        return new SmallRyeConfigBuilder()
                .withMapping(IcebergConfig.class)
                .withDefaultValue("debezium.sink.iceberg.destination-uppercase-table-names", "false")
                .withDefaultValue("debezium.sink.iceberg.destination-lowercase-table-names", "false")
                .withDefaultValue("debezium.sink.iceberg.upsert", "false")
                .withDefaultValue("debezium.sink.iceberg.upsert-keep-deletes", "true")
                .withDefaultValue("debezium.sink.iceberg.create-identifier-fields", "true")
                .withDefaultValue("debezium.sink.iceberg.allow-field-addition", "true")
                .withDefaultValue("debezium.sink.iceberg.preserve-required-property", "false")
                .withDefaultValue("debezium.sink.iceberg.nested-as-variant", "false")
                .withDefaultValue("debezium.sink.iceberg.warehouse", "/tmp/iceberg-warehouse")
                .withSources(new io.smallrye.config.PropertiesConfigSource(configProps, "test-config", 100))
                .build()
                .getConfigMapping(IcebergConfig.class);
    }

    @Test
    void testSimplePartitionBy() {
        Map<String, String> config = Map.of(
                "debezium.sink.iceberg.partition-by", "field1,field2,field3");
        IcebergConfig icebergConfig = getIcebergConfig(config);

        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(3, partitions.size());
        assertEquals("field1", partitions.get(0));
        assertEquals("field2", partitions.get(1));
        assertEquals("field3", partitions.get(2));
    }

    @Test
    void testPartitionByWithParentheses() {
        Map<String, String> config = Map.of(
                "debezium.sink.iceberg.partition-by", "bucket(10\\, id),year(ts),month(ts)");
        IcebergConfig icebergConfig = getIcebergConfig(config);

        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(3, partitions.size());
        assertEquals("bucket(10, id)", partitions.get(0));
        assertEquals("year(ts)", partitions.get(1));
        assertEquals("month(ts)", partitions.get(2));
    }

    @Test
    void testComplexPartitionByWithSpaces() {
        Map<String, String> config = Map.of(
                // Note: In properties file, backslash escape might be needed for commas,
                // but here we are passing a map directly so the comma splitting logic in
                // SmallRye/Converter handles it.
                // We need to ensure the standard converter handles "bucket(10, id)" correctly
                // as one item if it splits by comma.
                // SmallRye Config's default list converter splits by comma.
                // If the value contains commas inside parentheses, the default converter might
                // split it incorrectly
                // unless we have a custom converter or escape it.
                // The original test used backslashes in properties: "bucket(10\\, id)"

                "debezium.sink.iceberg.partition-by",
                "bucket(10\\, id), year(created_at), month(created_at), day(created_at)");
        IcebergConfig icebergConfig = getIcebergConfig(config);

        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(4, partitions.size());
        assertEquals("bucket(10, id)", partitions.get(0));
        assertEquals(" year(created_at)", partitions.get(1));
        assertEquals(" month(created_at)", partitions.get(2));
        assertEquals(" day(created_at)", partitions.get(3));
    }

    @Test
    void testEmptyPartitionBy() {
        IcebergConfig icebergConfig = getIcebergConfig(Collections.emptyMap());
        assertFalse(icebergConfig.partitionBy().isPresent());
        assertTrue(icebergConfig.partitionBy().isEmpty());
    }

    @Test
    void testMixedPartitionFormats() {
        Map<String, String> config = Map.of(
                "debezium.sink.iceberg.partition-by",
                "country, year(created_at), bucket(10\\, customer_id), region, truncate(5\\, name)");
        IcebergConfig icebergConfig = getIcebergConfig(config);

        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(5, partitions.size());
        assertEquals("country", partitions.get(0));
        assertEquals(" year(created_at)", partitions.get(1));
        assertEquals(" bucket(10, customer_id)", partitions.get(2));
        assertEquals(" region", partitions.get(3));
        assertEquals(" truncate(5, name)", partitions.get(4));
    }
}
