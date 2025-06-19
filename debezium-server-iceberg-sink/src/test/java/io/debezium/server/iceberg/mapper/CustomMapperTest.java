package io.debezium.server.iceberg.mapper;

import io.debezium.server.iceberg.BaseSparkTest;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(CustomMapperTest.TestProfile.class)
public class CustomMapperTest extends BaseSparkTest {

    @Test
    public void testCustomMapper() throws Exception {
        assertEquals(sinkType, "iceberg");
        String sql = """
                DROP TABLE IF EXISTS inventory.sample;
                CREATE TABLE IF NOT EXISTS inventory.sample (id INTEGER, val INTEGER);
        """;
        SourcePostgresqlDB.runSQL(sql);
        SourcePostgresqlDB.runSQL("INSERT INTO inventory.sample (id, val) VALUES (1, 123)");
        Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
            try {
              var df = spark.newSession().table(ICEBERG_CATALOG_TABLE_NAMESPACE + ".custom_mapper_sample");
                Assertions.assertEquals(1, df.count());

                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            config.put("debezium.sink.iceberg.table-mapper", "custom-mapper");
            return config;
        }
    }
}