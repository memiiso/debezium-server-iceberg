/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.debezium.server.iceberg.offset.IcebergOffsetBackingStore;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the tables and the offset are committed in one transaction, run against each REST
 * catalog supporting multi-table transactions.
 */
public abstract class BaseAtomicOffsetCommitTest extends BaseTest {

  @Test
  public void testTablesAndOffsetCommittedTogether() throws Exception {
    Awaitility.await()
        .atMost(Duration.ofSeconds(120))
        .until(
            () -> {
              try {
                CloseableIterable<Record> customers = getTableDataV2("testc.inventory.customers");
                CloseableIterable<Record> orders = getTableDataV2("testc.inventory.orders");
                return Lists.newArrayList(customers).size() >= 4
                    && Lists.newArrayList(orders).size() >= 4;
              } catch (Exception e) {
                return false;
              }
            });

    IcebergOffsetBackingStore offsetStore = IcebergOffsetBackingStore.instance().orElseThrow();
    List<Record> offsetRows = Lists.newArrayList(getTableDataV2(offsetStore.tableIdentifier()));
    Assertions.assertEquals(1, offsetRows.size());
    Map<String, String> offsetData =
        new ObjectMapper()
            .readValue(
                (String) offsetRows.get(0).getField("offset_data"), new TypeReference<>() {});
    // the engine flushes the same offsets after the transaction, a second key means its key differs
    // from the one committed with the data
    Assertions.assertEquals(1, offsetData.size(), offsetData.toString());
    Assertions.assertTrue(offsetData.values().iterator().next().contains("lsn"));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.atomic-offset-commit", "true");
      return config;
    }
  }
}
