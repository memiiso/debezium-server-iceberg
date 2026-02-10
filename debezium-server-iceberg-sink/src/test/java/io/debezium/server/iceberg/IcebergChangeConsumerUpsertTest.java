/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.debezium.server.iceberg.testresources.TestUtil;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerUpsertTest.TestProfile.class)
public class IcebergChangeConsumerUpsertTest extends BaseSparkTest {

  static final Long TEST_EPOCH_MS = 1577840461000L;

  @Test
  public void testSimpleUpsert() throws Exception {
    String dest = "testc.inventory.customers_upsert";
    // test simple inserts
    List<EmbeddedEngineChangeEvent> records = new ArrayList<>();
    records.add(eventFactory.of(dest, 1, "c"));
    records.add(eventFactory.of(dest, 2, "c"));
    records.add(eventFactory.of(dest, 3, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert");
    Assertions.assertEquals(ds.count(), 3);
    Assertions.assertEquals(ds.where("id = 3").count(), 1);

    // 3 records should be updated 4th one should be inserted
    records.clear();
    records.add(eventFactory.of(dest, 1, "r"));
    records.add(eventFactory.of(dest, 2, "d"));
    records.add(eventFactory.of(dest, 3, "u", "UpdatednameV1"));
    records.add(eventFactory.of(dest, 4, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(4, ds.count());
    Assertions.assertEquals(ds.where("id = 1 AND __op= 'r'").count(), 1);
    Assertions.assertEquals(ds.where("id = 2 AND __op= 'd'").count(), 1);
    Assertions.assertEquals(ds.where("id = 3 AND __op= 'u'").count(), 1);
    Assertions.assertEquals(ds.where("id = 3 AND first_name= 'UpdatednameV1'").count(), 1);
    Assertions.assertEquals(ds.where("id = 4 AND __op= 'c'").count(), 1);

    records.clear();
    // in case of duplicate records it should only keep the latest one
    records.add(eventFactory.of(dest, 3, "r", "UpdatednameV2", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.of(dest, 3, "u", "UpdatednameV3", TEST_EPOCH_MS + 2L));
    records.add(eventFactory.of(dest, 3, "u", "UpdatednameV4", TEST_EPOCH_MS + 3L));
    records.add(eventFactory.of(dest, 4, "u", "Updatedname-4-V1", TEST_EPOCH_MS + 4L));
    records.add(eventFactory.of(dest, 4, "u", "Updatedname-4-V2", TEST_EPOCH_MS + 5L));
    records.add(eventFactory.of(dest, 4, "r", "Updatedname-4-V3", TEST_EPOCH_MS + 6L));
    records.add(eventFactory.of(dest, 5, "r", TEST_EPOCH_MS + 7L));
    records.add(eventFactory.of(dest, 6, "r", TEST_EPOCH_MS + 8L));
    records.add(eventFactory.of(dest, 6, "d", TEST_EPOCH_MS + 8L));
    records.add(eventFactory.of(dest, 6, "c", TEST_EPOCH_MS + 8L));
    records.add(eventFactory.of(dest, 6, "u", "Updatedname-6-V1", TEST_EPOCH_MS + 8L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.sort("id").show(false);
    Assertions.assertEquals(ds.count(), 6);
    Assertions.assertEquals(
        ds.where("id = 3 AND __op= 'u' AND first_name= 'UpdatednameV4'").count(), 1);
    Assertions.assertEquals(
        ds.where("id = 4 AND __op= 'r' AND first_name= 'Updatedname-4-V3'").count(), 1);
    Assertions.assertEquals(ds.where("id = 5 AND __op= 'r' ").count(), 1);
    Assertions.assertEquals(
        ds.where("id = 6 AND __op= 'u' AND first_name= 'Updatedname-6-V1'").count(), 1);

    // if its not standard insert followed by update! should keep latest one
    records.clear();
    records.add(eventFactory.of(dest, 7, "u", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.of(dest, 7, "u", TEST_EPOCH_MS + 2L));
    records.add(eventFactory.of(dest, 7, "r", TEST_EPOCH_MS + 3L));
    records.add(eventFactory.of(dest, 7, "u", "Updatedname-7-V1", TEST_EPOCH_MS + 4L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(
        ds.where("id = 7 AND __op= 'u' AND first_name= 'Updatedname-7-V1'").count(), 1);
    //    S3Minio.listFiles();
  }

  @Test
  public void testSimpleUpsertCompositeKey() throws Exception {
    String dest = "testc.inventory.customers_upsert_compositekey";
    // test simple inserts
    List<EmbeddedEngineChangeEvent> records = new ArrayList<>();
    records.add(eventFactory.ofCompositeKey(dest, 1, "c", "user1", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.ofCompositeKey(dest, 1, "c", "user2", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.ofCompositeKey(dest, 1, "u", "user1", TEST_EPOCH_MS + 2L));
    records.add(eventFactory.ofCompositeKey(dest, 1, "r", "user1", TEST_EPOCH_MS + 3L));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert_compositekey");
    ds.show();
    Assertions.assertEquals(ds.count(), 2);
    Assertions.assertEquals(ds.where("id = 1").count(), 2);

    records.clear();
    records.add(eventFactory.ofCompositeKey(dest, 1, "u", "user2", TEST_EPOCH_MS + 1L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert_compositekey");
    ds.show();
    Assertions.assertEquals(ds.count(), 2);
    Assertions.assertEquals(ds.where("id = 1 AND __op= 'u' AND first_name= 'user2'").count(), 1);
  }

  @Test
  public void testSimpleUpsertNoKey() throws Exception {
    String debeziumFormatValue = System.getenv("DEBEZIUM_FORMAT_VALUE");
    if (debeziumFormatValue != null) {
      Assertions.assertEquals(debeziumFormatValue, config.debezium().keyValueChangeEventFormat());
    }
    LOGGER.warn("DEBEZIUM_FORMAT_VALUE is set to:" + config.debezium().keyValueChangeEventFormat());

    String dest = "testc.inventory.customers_upsert_nokey";
    // when there is no PK it should fall back to append mode
    List<EmbeddedEngineChangeEvent> records = new ArrayList<>();
    records.add(eventFactory.ofNoKey(dest, 1, "c", "user1", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.ofNoKey(dest, 1, "c", "user2", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.ofNoKey(dest, 1, "u", "user1", TEST_EPOCH_MS + 2L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert_nokey");
    ds.show();
    Assertions.assertEquals(ds.count(), 3);
    Assertions.assertEquals(ds.where("id = 1").count(), 3);

    records.clear();
    records.add(eventFactory.ofNoKey(dest, 1, "c", "user2", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.ofNoKey(dest, 1, "u", "user2", TEST_EPOCH_MS + 1L));
    records.add(eventFactory.ofNoKey(dest, 1, "r", "user1", TEST_EPOCH_MS + 3L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert_nokey");
    ds.show();
    Assertions.assertEquals(ds.count(), 6);
    Assertions.assertEquals(ds.where("id = 1 AND __op= 'c' AND first_name= 'user2'").count(), 2);
  }

  @Test
  public void testTableUpsertNokey() throws SQLException, ClassNotFoundException {
    String sql =
        "\n"
            + "        DROP TABLE IF EXISTS inventory.table_without_pk;\n"
            + "        CREATE TABLE IF NOT EXISTS inventory.table_without_pk (\n"
            + "            c_id INTEGER ,\n"
            + "            c_varchar VARCHAR\n"
            + "          );"
            + "ALTER TABLE inventory.table_without_pk REPLICA IDENTITY FULL;";
    SourcePostgresqlDB.runSQL(sql);
    SourcePostgresqlDB.runSQL(
        "INSERT INTO inventory.table_without_pk (c_id, c_varchar) VALUES (1, 'STRING-DATA-1');"
            + "INSERT INTO inventory.table_without_pk (c_id, c_varchar) VALUES (2, 'STRING-DATA-2');");
    Awaitility.await()
        .atMost(Duration.ofSeconds(180))
        .until(
            () -> {
              try {
                Dataset<Row> ds = getTableData("testc.inventory.table_without_pk");
                ds.show();
                return ds.count() == 2;
              } catch (Exception e) {
                return false;
              }
            });
    SourcePostgresqlDB.runSQL(
        "UPDATE inventory.table_without_pk SET c_varchar='STRING-UPDATE-1'; ");
    Awaitility.await()
        .atMost(Duration.ofSeconds(180))
        .until(
            () -> {
              try {
                Dataset<Row> ds = getTableData("testc.inventory.table_without_pk");
                ds.show();
                return ds.count() == 4 && ds.where("__op == 'u'").count() == 2;
              } catch (Exception e) {
                return false;
              }
            });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.upsert", "true");
      config.put("debezium.sink.iceberg.upsert-keep-deletes", "true");
      return config;
    }
  }
}
