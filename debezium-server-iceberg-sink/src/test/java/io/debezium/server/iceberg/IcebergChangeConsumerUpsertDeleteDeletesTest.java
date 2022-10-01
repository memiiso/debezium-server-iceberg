/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.TestChangeEvent;
import io.debezium.server.iceberg.testresources.TestUtil;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerUpsertDeleteDeletesTest.IcebergChangeConsumerUpsertTestDeleteDeletesProfile.class)
public class IcebergChangeConsumerUpsertDeleteDeletesTest extends BaseSparkTest {

  @Inject
  IcebergChangeConsumer consumer;
  final static Long TEST_EPOCH_MS = 1577840461000L;

  @Test
  public void testSimpleUpsert() throws Exception {

    String dest = "testc.inventory.customers_upsert";
    List<io.debezium.engine.ChangeEvent<Object, Object>> records = new ArrayList<>();
    records.add(TestChangeEvent.of(dest, 1, "c"));
    records.add(TestChangeEvent.of(dest, 2, "c"));
    records.add(TestChangeEvent.of(dest, 3, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert");
    Assertions.assertEquals(ds.count(), 3);
    Assertions.assertEquals(ds.where("id = 3").count(), 1);

    // 3 records should be updated 4th one should be inserted
    records.clear();
    records.add(TestChangeEvent.of(dest, 1, "r"));
    records.add(TestChangeEvent.of(dest, 2, "d"));
    records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV1"));
    records.add(TestChangeEvent.of(dest, 4, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.count(), 3);
    Assertions.assertEquals(ds.where("id = 1 AND __op= 'r'").count(), 1);
    Assertions.assertEquals(ds.where("id = 2").count(), 0);
    Assertions.assertEquals(ds.where("id = 3 AND __op= 'u'").count(), 1);
    Assertions.assertEquals(ds.where("id = 3 AND first_name= 'UpdatednameV1'").count(), 1);
    Assertions.assertEquals(ds.where("id = 4 AND __op= 'c'").count(), 1);

    records.clear();
    // incase of duplicate records it should only keep the latest by epoch ts
    records.add(TestChangeEvent.of(dest, 3, "r", "UpdatednameV2", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV3", TEST_EPOCH_MS + 2L));
    records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV4", TEST_EPOCH_MS + 3L));
    records.add(TestChangeEvent.of(dest, 4, "u", "Updatedname-4-V1", TEST_EPOCH_MS + 4L));
    records.add(TestChangeEvent.of(dest, 4, "u", "Updatedname-4-V2", TEST_EPOCH_MS + 5L));
    records.add(TestChangeEvent.of(dest, 4, "d", "Updatedname-4-V3", TEST_EPOCH_MS + 6L));
    records.add(TestChangeEvent.of(dest, 5, "d", TEST_EPOCH_MS + 7L));
    records.add(TestChangeEvent.of(dest, 6, "r", TEST_EPOCH_MS + 8L));
    records.add(TestChangeEvent.of(dest, 6, "r", TEST_EPOCH_MS + 9L));
    records.add(TestChangeEvent.of(dest, 6, "u", TEST_EPOCH_MS + 10L));
    records.add(TestChangeEvent.of(dest, 6, "u", "Updatedname-6-V1", TEST_EPOCH_MS + 11L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.count(), 3);
    Assertions.assertEquals(ds.where("id = 3 AND __op= 'u' AND first_name= 'UpdatednameV4'").count(), 1);
    Assertions.assertEquals(ds.where("id = 4 ").count(), 0);
    Assertions.assertEquals(ds.where("id = 5 ").count(), 0);
    Assertions.assertEquals(ds.where("id = 6 AND __op= 'u' AND first_name= 'Updatedname-6-V1'").count(), 1);

    // in case of duplicate records including epoch ts, its should keep latest one based on operation priority
    // ("c", 1, "r", 2, "u", 3, "d", 4);
    records.clear();
    records.add(TestChangeEvent.of(dest, 3, "d", "UpdatednameV5", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV6", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.of(dest, 6, "c", "Updatedname-6-V2", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.of(dest, 6, "r", "Updatedname-6-V3", TEST_EPOCH_MS + 1L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.where("id = 3 ").count(), 0);
    Assertions.assertEquals(ds.where("id = 6 AND __op= 'r' AND first_name= 'Updatedname-6-V3'").count(), 1);

    // if its not standard insert followed by update! should keep latest one
    records.clear();
    records.add(TestChangeEvent.of(dest, 7, "u", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.of(dest, 7, "d", TEST_EPOCH_MS + 2L));
    records.add(TestChangeEvent.of(dest, 7, "r", TEST_EPOCH_MS + 3L));
    records.add(TestChangeEvent.of(dest, 7, "u", "Updatedname-7-V1", TEST_EPOCH_MS + 4L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.where("id = 7 AND __op= 'u' AND first_name= 'Updatedname-7-V1'").count(), 1);

  }

  @Test
  public void testSimpleUpsertCompositeKey() throws Exception {
    String dest = "testc.inventory.customers_upsert_compositekey";
    // test simple inserts
    List<io.debezium.engine.ChangeEvent<Object, Object>> records = new ArrayList<>();
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "c", "user1", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "c", "user2", TEST_EPOCH_MS + 1L));
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "u", "user1", TEST_EPOCH_MS + 2L));
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "r", "user1", TEST_EPOCH_MS + 3L));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert_compositekey");
    ds.show();
    Assertions.assertEquals(ds.count(), 2);
    Assertions.assertEquals(ds.where("id = 1").count(), 2);

    records.clear();
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "u", "user1", TEST_EPOCH_MS + 2L));
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "r", "user1", TEST_EPOCH_MS + 3L));
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "d", "user1", TEST_EPOCH_MS + 3L));
    records.add(TestChangeEvent.ofCompositeKey(dest, 1, "d", "user2", TEST_EPOCH_MS + 1L));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert_compositekey");
    ds.show();
    Assertions.assertEquals(ds.count(), 0);
    Assertions.assertEquals(ds.where("first_name= 'user2'").count(), 0);
  }

  public static class IcebergChangeConsumerUpsertTestDeleteDeletesProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.upsert", "true");
      config.put("debezium.sink.iceberg.upsert-keep-deletes", "false");
      return config;
    }
  }

}
