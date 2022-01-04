/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.IcebergChangeEvent;

import java.time.Instant;

/**
 * helper class used to generate test customer change events
 *
 * @author Ismail Simsek
 */
public class CustomerChangeEvent {

  public static TestChangeEvent<Object, Object> of(Integer id, String operation, String name, Long epoch) {
    IcebergChangeEvent t = new IcebergChangeEventBuilder().
        addKeyField("id", id)
        .addField("first_name", name)
        .addField("last_name", "Walker")
        .addField("email", "ed@walker.com")
        .addField("__op", operation)
        .addField("__source_ts_ms", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    String key = "{" +
                 "\"schema\":" + t.jsonSchema().keySchema() + "," +
                 "\"payload\":" + t.key() +
                 "} ";
    String val = "{" +
                 "\"schema\":" + t.jsonSchema().valueSchema() + "," +
                 "\"payload\":" + t.value() +
                 "} ";
    return new TestChangeEvent<>(key, val, "testc.inventory.customers_upsert_compositekey");
  }

  public static TestChangeEvent<Object, Object> ofCompositeKey(Integer id, String operation, String name,
                                                               Long epoch) {
    IcebergChangeEvent t = new IcebergChangeEventBuilder().
        addKeyField("id", id)
        .addKeyField("first_name", name)
        .addField("last_name", "Walker")
        .addField("email", "ed@walker.com")
        .addField("__op", operation)
        .addField("__source_ts_ms", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    String key = "{" +
                 "\"schema\":" + t.jsonSchema().keySchema() + "," +
                 "\"payload\":" + t.key() +
                 "} ";
    String val = "{" +
                 "\"schema\":" + t.jsonSchema().valueSchema() + "," +
                 "\"payload\":" + t.value() +
                 "} ";

    return new TestChangeEvent<>(key, val, "testc.inventory.customers_upsert_compositekey");
  }

  public static TestChangeEvent<Object, Object> of(Integer id, String operation) {
    return of(id, operation, TestUtil.randomString(12), Instant.now().toEpochMilli());
  }

  public static TestChangeEvent<Object, Object> of(Integer id, String operation, String name) {
    return of(id, operation, name, Instant.now().toEpochMilli());
  }

  public static TestChangeEvent<Object, Object> of(Integer id, String operation, Long epoch) {
    return of(id, operation, TestUtil.randomString(12), epoch);
  }

  public static TestChangeEvent<Object, Object> ofNoKey(Integer id, String operation, String name,
                                                        Long epoch) {
    IcebergChangeEvent t = new IcebergChangeEventBuilder().
        addField("id", id)
        .addField("first_name", name)
        .addField("last_name", "Walker")
        .addField("email", "ed@walker.com")
        .addField("__op", operation)
        .addField("__source_ts_ms", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    String val = "{" +
                 "\"schema\":" + t.jsonSchema().valueSchema() + "," +
                 "\"payload\":" + t.value() +
                 "} ";
    return new TestChangeEvent<>(null, val, "testc.inventory.customers_upsert_compositekey");
  }

}
