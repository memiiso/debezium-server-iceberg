/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerMysqlTestUnwrapped.TestProfile.class)
public class IcebergChangeConsumerMysqlTestUnwrapped extends BaseTest {

  @Test
  public void testSimpleUpload() throws Exception {

    // make sure its not unwrapped
    assertEquals(config.debezium().isEventFlatteningEnabled(), false);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.customers");
        printTableData(result);
        return Lists.newArrayList(result).size() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    // test nested data(struct) consumed
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.geom");
        return Lists.newArrayList(result).size() >= 3;
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

  @Test
  public void testDeleteEvents() throws Exception {

    // make sure its not unwrapped
    assertEquals(config.debezium().isEventFlatteningEnabled(), false);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.customers");
        printTableData(result);
        return Lists.newArrayList(result).size() >= 4;
      } catch (Exception e) {
        return false;
      }
    });

    SourceMysqlDB.runSQL("ALTER TABLE inventory.addresses DROP FOREIGN KEY addresses_ibfk_1;");
    SourceMysqlDB.runSQL("DELETE FROM inventory.customers where id = 1004 ;");

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.customers");
        printTableData(result);
        return Lists.newArrayList(result).size() >= 5;
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
      config.put("%mysql.debezium.source.table.whitelist", "inventory.customers,inventory.test_delete_table");
      config.put("debezium.transforms", ",");
      config.put("debezium.sink.iceberg.upsert", "false");
      config.put("debezium.sink.iceberg.create-identifier-fields", "false");
      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }

}
