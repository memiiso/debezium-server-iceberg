/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

/**
 * Reads from the PostgreSQL database and writes to a REST catalog, run against each REST catalog
 * implementation started by the subclasses.
 */
public abstract class BaseRestCatalogTest extends BaseTest {

  @Test
  @DisabledForJreRange(min = JRE.JAVA_24) // Hadoop incompatibility with Java 25
  public void testSimpleUpload() {
    Awaitility.await()
        .atMost(Duration.ofSeconds(120))
        .until(
            () -> {
              try {
                CloseableIterable<Record> result = getTableDataV2("testc.inventory.customers");
                return Lists.newArrayList(result).size() >= 3;
              } catch (Exception e) {
                return false;
              }
            });

    Namespace namespace = Namespace.of(consumer.config.iceberg().namespace());
    List<TableIdentifier> tables = consumer.icebergCatalog.listTables(namespace);
    Assertions.assertTrue(
        tables.contains(TableIdentifier.of(namespace, "debezium_offset_storage_table")));
    Assertions.assertTrue(
        tables.contains(TableIdentifier.of(namespace, "debeziumcdc_testc_inventory_customers")));
  }
}
