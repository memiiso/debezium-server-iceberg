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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
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

  @Test
  @Disabled
  public void testSimpleUpsert() throws Exception {

    List<ChangeEvent<Object, Object>> records = new ArrayList<>();
    records.add(getCustomerRecord(1, "c"));
    records.add(getCustomerRecord(2, "c"));
    records.add(getCustomerRecord(3, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show();
        return ds.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });
  }

  private TestChangeEvent getCustomerRecord(Integer id, String operation) {
    String key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false," + "\"field\":\"id\"}]," +
        "\"optional\":false,\"name\":\"testc.inventory.customers.Key\"}," +
        "\"payload\":{\"id\":" + id + "}}";
    String val = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"email\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__op\"}," +
        "{\"type\":\"string\",\"optional\":true,\"field\":\"__table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"__lsn\"}," +
        "{\"type\":\"int64\",\"optional\":true,\"field\":\"__source_ts_ms\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__deleted\"}]," +
        "\"optional\":false,\"name\":\"testc.inventory.customers.Value\"}," +
        "\"payload\":{\"id\":" + id + ",\"first_name\":\"Edward\",\"last_name\":\"Walker\",\"email\":\"ed@walker.com\"," +
        "\"__op\":\"" + operation + "\",\"__table\":\"customers\",\"__lsn\":33832960,\"__source_ts_ms\":" + Instant.now().toEpochMilli() + "," +
        "\"__deleted\":\"" + operation.equals("d") + "\"}} ";
    return new TestChangeEvent(key, val, "testc.inventory.customers");
  }

}
