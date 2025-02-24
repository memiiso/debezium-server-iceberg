/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;
import com.google.common.collect.Lists;
import io.debezium.server.iceberg.testresources.CatalogRest;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.List;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = CatalogRest.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class IcebergChangeConsumerRestCatalogTest extends BaseTest {

  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.customers");
        return Lists.newArrayList(result).size() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    List<TableIdentifier> tables = consumer.icebergCatalog.listTables(Namespace.of(consumer.config.iceberg().namespace()));
    Assertions.assertTrue(tables.contains(TableIdentifier.of(Namespace.of(consumer.config.iceberg().namespace()), "debezium_offset_storage_table")));
    Assertions.assertTrue(tables.contains(TableIdentifier.of(Namespace.of(consumer.config.iceberg().namespace()), "debeziumcdc_testc_inventory_customers")));
  }
}
