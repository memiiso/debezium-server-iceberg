package io.debezium.server.iceberg;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for IcebergConfig, specifically testing the partitionBy field
 * with custom delimiter support through PartitionByConverter.
 */
@QuarkusTest
@TestProfile(IcebergConfigTest.SimplePartitionByProfile.class)
class IcebergConfigTest {

    @Inject
    IcebergConfig icebergConfig;

    @Test
    void testSimplePartitionBy() {
        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(3, partitions.size());
        assertEquals("field1", partitions.get(0));
        assertEquals("field2", partitions.get(1));
        assertEquals("field3", partitions.get(2));
    }

    public static class SimplePartitionByProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.partition-by", "field1,field2,field3");
            config.put("debezium.sink.iceberg.warehouse", "/tmp/test-warehouse");
            return config;
        }
    }
}

@QuarkusTest
@TestProfile(IcebergConfigParenthesesTest.ParenthesesPartitionByProfile.class)
class IcebergConfigParenthesesTest {

    @Inject
    IcebergConfig icebergConfig;

    @Test
    void testPartitionByWithParentheses() {
        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(3, partitions.size());
        assertEquals("bucket(10, id)", partitions.get(0));
        assertEquals("year(ts)", partitions.get(1));
        assertEquals("month(ts)", partitions.get(2));
    }

    public static class ParenthesesPartitionByProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.partition-by", "bucket(10, id),year(ts),month(ts)");
            config.put("debezium.sink.iceberg.warehouse", "/tmp/test-warehouse");
            return config;
        }
    }
}

@QuarkusTest
@TestProfile(IcebergConfigComplexPartitionByTest.ComplexPartitionByProfile.class)
class IcebergConfigComplexPartitionByTest {

    @Inject
    IcebergConfig icebergConfig;

    @Test
    void testComplexPartitionByWithSpaces() {
        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(4, partitions.size());
        assertEquals("bucket(10, id)", partitions.get(0));
        assertEquals("year(created_at)", partitions.get(1));
        assertEquals("month(created_at)", partitions.get(2));
        assertEquals("day(created_at)", partitions.get(3));
    }

    public static class ComplexPartitionByProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.partition-by",
                "bucket(10, id), year(created_at), month(created_at), day(created_at)");
            config.put("debezium.sink.iceberg.warehouse", "/tmp/test-warehouse");
            return config;
        }
    }
}

@QuarkusTest
@TestProfile(IcebergConfigEmptyPartitionByTest.EmptyPartitionByProfile.class)
class IcebergConfigEmptyPartitionByTest {

    @Inject
    IcebergConfig icebergConfig;

    @Test
    void testEmptyPartitionBy() {
        assertTrue(icebergConfig.partitionBy().isPresent());
        assertTrue(icebergConfig.partitionBy().get().isEmpty());
    }

    public static class EmptyPartitionByProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.warehouse", "/tmp/test-warehouse");
            return config;
        }
    }
}

@QuarkusTest
@TestProfile(IcebergConfigTableSpecificPartitionByTest.TableSpecificPartitionByProfile.class)
class IcebergConfigTableSpecificPartitionByTest {

    @Inject
    IcebergConfig icebergConfig;

    @Test
    void testTableSpecificPartitionBy() {
        // Test global partitionBy
        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> globalPartitions = icebergConfig.partitionBy().get();
        assertEquals(2, globalPartitions.size());
        assertEquals("year(created_at)", globalPartitions.get(0));
        assertEquals("month(created_at)", globalPartitions.get(1));

        // Test table-specific partitionBy
        assertTrue(icebergConfig.partitionByForTable("users").isPresent());
        List<String> tablePartitions = icebergConfig.partitionByForTable("users").get();
        assertEquals(3, tablePartitions.size());
        assertEquals("bucket(5, user_id)", tablePartitions.get(0));
        assertEquals("year(signup_date)", tablePartitions.get(1));
        assertEquals("month(signup_date)", tablePartitions.get(2));

        // Test fallback to global partitionBy for non-configured table
        assertTrue(icebergConfig.partitionByForTable("orders").isPresent());
        List<String> fallbackPartitions = icebergConfig.partitionByForTable("orders").get();
        assertEquals(globalPartitions, fallbackPartitions);
    }

    public static class TableSpecificPartitionByProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.partition-by", "year(created_at),month(created_at)");
            config.put("debezium.sink.iceberg.table.users.partition-by",
                "bucket(5, user_id),year(signup_date),month(signup_date)");
            config.put("debezium.sink.iceberg.warehouse", "/tmp/test-warehouse");
            return config;
        }
    }
}

@QuarkusTest
@TestProfile(IcebergConfigMixedPartitionByTest.MixedPartitionByProfile.class)
class IcebergConfigMixedPartitionByTest {

    @Inject
    IcebergConfig icebergConfig;

    @Test
    void testMixedPartitionFormats() {
        assertTrue(icebergConfig.partitionBy().isPresent());
        List<String> partitions = icebergConfig.partitionBy().get();
        assertEquals(5, partitions.size());
        assertEquals("country", partitions.get(0));
        assertEquals("year(created_at)", partitions.get(1));
        assertEquals("bucket(10, customer_id)", partitions.get(2));
        assertEquals("region", partitions.get(3));
        assertEquals("truncate(5, name)", partitions.get(4));
    }

    public static class MixedPartitionByProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.partition-by",
                "country, year(created_at), bucket(10, customer_id), region, truncate(5, name)");
            config.put("debezium.sink.iceberg.warehouse", "/tmp/test-warehouse");
            return config;
        }
    }
}

