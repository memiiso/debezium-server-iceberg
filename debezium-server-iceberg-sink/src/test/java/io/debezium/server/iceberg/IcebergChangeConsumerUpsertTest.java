/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.testresource.*;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(IcebergChangeConsumerUpsertTestProfile.class)
public class IcebergChangeConsumerUpsertTest extends BaseSparkTest {

  @Inject
  IcebergChangeConsumer consumer;


  // dedup
  // @TODO add test table without PK {insert update delete}  ins del up on same record
  // @TODO add test table with PK {insert update delete}
  // @TODO add test table with PK {insert update delete} keep deletes = true
  // @TODO make column and the value configurable
  @Test
  public void testSimpleUpsert() throws Exception {

    List<ChangeEvent<Object, Object>> records = new ArrayList<>();
    records.add(getCustomerRecord(1, "c"));
    records.add(getCustomerRecord(2, "c"));
    records.add(getCustomerRecord(3, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert");
    Assertions.assertEquals(ds.count(), 3);
    Assertions.assertEquals(ds.where("id == 3").count(), 1);

    // 3 records should be updated 4th one should be inserted
    records.clear();
    records.add(getCustomerRecord(1, "r"));
    records.add(getCustomerRecord(2, "d"));
    records.add(getCustomerRecord(3, "u", "UpdatednameV1"));
    records.add(getCustomerRecord(4, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.count(), 4);
    Assertions.assertEquals(ds.where("id == 1 AND __op= 'r'").count(), 1);
    Assertions.assertEquals(ds.where("id == 2 AND __op= 'd'").count(), 1);
    Assertions.assertEquals(ds.where("id == 3 AND __op= 'u'").count(), 1);
    Assertions.assertEquals(ds.where("id == 3 AND first_name= 'UpdatednameV1'").count(), 1);
    Assertions.assertEquals(ds.where("id == 4 AND __op= 'c'").count(), 1);

    records.clear();
    // incase of duplicate records should only keep the latest
    records.add(getCustomerRecord(3, "r", "UpdatednameV2"));
    Thread.sleep(10);
    records.add(getCustomerRecord(3, "u", "UpdatednameV3"));
    Thread.sleep(10);
    records.add(getCustomerRecord(3, "u", "UpdatednameV4"));
    Thread.sleep(10);
    records.add(getCustomerRecord(4, "u", "Updatedname-4-V1"));
    Thread.sleep(10);
    records.add(getCustomerRecord(4, "u", "Updatedname-4-V2"));
    Thread.sleep(10);
    records.add(getCustomerRecord(4, "r", "Updatedname-4-V3"));
    Thread.sleep(10);
    records.add(getCustomerRecord(5, "r"));
    Thread.sleep(10);
    records.add(getCustomerRecord(6, "r"));
    Thread.sleep(10);
    records.add(getCustomerRecord(6, "r"));
    Thread.sleep(10);
    records.add(getCustomerRecord(6, "u"));
    Thread.sleep(10);
    records.add(getCustomerRecord(6, "u", "Updatedname-6-V1"));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.count(), 6);
    Assertions.assertEquals(ds.where("id == 3 AND __op= 'u' AND first_name= 'UpdatednameV4'").count(), 1);
    Assertions.assertEquals(ds.where("id == 4 AND __op= 'r' AND first_name= 'Updatedname-4-V3'").count(), 1);
    Assertions.assertEquals(ds.where("id == 5 AND __op= 'r' ").count(), 1);
    Assertions.assertEquals(ds.where("id == 6 AND __op= 'u' AND first_name= 'Updatedname-6-V1'").count(), 1);

    // in case of duplicate records including ts, its should keep based on operation priority
    // ("c", 1, "r", 2, "u", 3, "d", 4);
    records.clear();
    records.add(getCustomerRecord(3, "d", "UpdatednameV5"));
    records.add(getCustomerRecord(3, "u", "UpdatednameV6"));
    records.add(getCustomerRecord(6, "c", "Updatedname-6-V2"));
    records.add(getCustomerRecord(6, "r", "Updatedname-6-V3"));
    consumer.handleBatch(records, TestUtil.getCommitter());
    ds = getTableData("testc.inventory.customers_upsert");
    ds.show();
    Assertions.assertEquals(ds.where("id == 3 AND __op= 'd' AND first_name= 'UpdatednameV5'").count(), 1);
    Assertions.assertEquals(ds.where("id == 6 AND __op= 'r' AND first_name= 'Updatedname-6-V3'").count(), 1);

  }

  private TestChangeEvent<Object, Object> getCustomerRecord(Integer id, String operation, String name) {
    String key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false," + "\"field\":\"id\"}]," +
        "\"optional\":false,\"name\":\"testc.inventory.customers.Key\"}," +
        "\"payload\":{\"id\":" + id + "}}";
    String val = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"email\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__op\"}," +
        "{\"type\":\"string\",\"optional\":true,\"field\":\"__table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"__lsn\"}," +
        "{\"type\":\"int64\",\"optional\":true,\"field\":\"__source_ts_ms\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__deleted\"}]," +
        "\"optional\":false,\"name\":\"testc.inventory.customers.Value\"}," +
        "\"payload\":{\"id\":" + id + ",\"first_name\":\"" + name + "\",\"last_name\":\"Walker\",\"email\":\"ed@walker" +
        ".com\"," +
        "\"__op\":\"" + operation + "\",\"__table\":\"customers\",\"__lsn\":33832960,\"__source_ts_ms\":" + Instant.now().toEpochMilli() + "," +
        "\"__deleted\":\"" + operation.equals("d") + "\"}} ";
    return new TestChangeEvent<>(key, val, "testc.inventory.customers_upsert");
  }

  private TestChangeEvent<Object, Object> getCustomerRecord(Integer id, String operation) {
    return this.getCustomerRecord(id, operation, TestUtil.randomString(12));
  }

}
