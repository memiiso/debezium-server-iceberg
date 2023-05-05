/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.history;

import io.debezium.server.iceberg.testresources.BaseTest;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.iceberg.catalog.TableIdentifier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@Disabled // @TODO remove spark with antlr4 version
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergSchemaHistoryTest.TestProfile.class)
public class IcebergSchemaHistoryTest extends BaseTest {
  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        return Lists.newArrayList(getTableDataV2("testc.inventory.customers")).size() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    // test nested data(struct) consumed
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        return Lists.newArrayList(getTableDataV2(TableIdentifier.of("mycatalog", "debezium_database_history_storage_test"))).size() >= 5;
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
      config.put("quarkus.profile", "mysql");
      config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
      config.put("debezium.source.schema.history", "io.debezium.server.iceberg.history.IcebergSchemaHistory");
      config.put("debezium.source.schema.history.iceberg.table-name", "debezium_database_history_storage_test");
      config.put("debezium.source.schema.history.internal", "io.debezium.server.iceberg.history.IcebergSchemaHistory");
      config.put("debezium.source.schema.history.internal.iceberg.table-name", "debezium_database_history_storage_test");
      config.put("debezium.source.table.whitelist", "inventory.customers");
      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }
}