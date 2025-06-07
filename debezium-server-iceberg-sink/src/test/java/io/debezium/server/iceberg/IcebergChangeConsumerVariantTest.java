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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerVariantTest.TestProfile.class)
public class IcebergChangeConsumerVariantTest extends BaseSparkTest {

  @Test
  public void testSimpleUpload() {
//    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
//      try {
//        CloseableIterable<Record> result = getTableDataV2("testc.inventory.customers");
//        printTableData(result);
//        return Lists.newArrayList(result).size() >= 4;
//      } catch (Exception e) {
////        e.printStackTrace();
//        return false;
//      }
//    });
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.geom");
        Record row = result.iterator().next();
        Assertions.assertEquals("variant", row.struct().field("g").type().toString());
        Assertions.assertEquals("variant", row.struct().field("h").type().toString());
        printTableData(result);
        return Lists.newArrayList(result).size() >= 3;
      } catch (Exception e) {
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.nested-as-variant", "true");
//      config.put("debezium.transforms", ",");
      return config;
    }
  }

}
