/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerConnectTest.TestProfile.class)
public class IcebergChangeConsumerConnectTest extends BaseSparkTest {

  @Test
  public void testDebeziumConfig() {
    assertEquals("connect", config.debezium().keyValueChangeEventFormat());
  }

  @Test
  @Disabled
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show(false);
        return ds.count() >= 3;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    });

    // test nested data(struct) consumed
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.geom");
        ds.show(false);
        return ds.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> d = getTableDataV2(TableIdentifier.of(ICEBERG_CATALOG_TABLE_NAMESPACE, "debezium_offset_storage_table"));
        System.out.println(Lists.newArrayList(d));
        return Lists.newArrayList(d).size() == 1;
      } catch (Exception e) {
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.format.value", "connect");
      config.put("debezium.format.key", "connect");
      return config;
    }
  }

}
