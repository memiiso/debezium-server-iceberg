/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.history;

import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergSchemaHistoryTest.TestProfile.class)
public class IcebergSchemaHistoryTest extends BaseSparkTest {
  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        return ds.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    // test nested data(struct) consumed
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData(ICEBERG_CATALOG_TABLE_NAMESPACE, "debezium_database_history_storage_table");
//        System.out.println(ds.count());
        return ds.count() >= 1;
      } catch (Exception e) {
        return false;
      }
    });
  }


  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("quarkus.profile", "mysql");
      config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
//      config.put("%mysql.debezium.source.table.whitelist", "inventory.*");
      config.put("debezium.source.schema.history.internal", "io.debezium.server.iceberg.history.IcebergSchemaHistory");
      config.put("debezium.source.schema.history.internal.iceberg.table-name", "debezium_database_history_storage_table");
      config.put("debezium.source.table.whitelist", "inventory.customers");
      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }
}