/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;


import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;
import io.debezium.server.iceberg.testresources.TestUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;

/**
 * helper class used to generate test debezium change events
 *
 * @author Ismail Simsek
 */
@ApplicationScoped
public class TestChangeEventFactory {

  @Inject
  IcebergChangeEventBuilder builder;

  public TestChangeEvent<Object, Object> of(String destination, Integer id, String operation, String name,
                                                   Long epoch) {
    final RecordConverter t = builder
        .destination(destination)
        .addKeyField("id", id)
        .addField("first_name", name)
        .addField("__op", operation)
        .addField("__source_ts_ms", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    final String key = "{" +
        "\"schema\":" + t.schemaConverter().keySchema() + "," +
        "\"payload\":" + t.key() +
        "} ";
    final String val = "{" +
        "\"schema\":" + t.schemaConverter().valueSchema() + "," +
        "\"payload\":" + t.value() +
        "} ";
    return new TestChangeEvent<>(key, val, destination);
  }

  public TestChangeEvent<Object, Object> ofCompositeKey(String destination, Integer id, String operation, String name,
                                                               Long epoch) {
    final RecordConverter t = builder
        .destination(destination)
        .addKeyField("id", id)
        .addKeyField("first_name", name)
        .addField("__op", operation)
        .addField("__source_ts_ms", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    final String key = "{" +
        "\"schema\":" + t.schemaConverter().keySchema() + "," +
        "\"payload\":" + t.key() +
        "} ";
    final String val = "{" +
        "\"schema\":" + t.schemaConverter().valueSchema() + "," +
        "\"payload\":" + t.value() +
        "} ";

    return new TestChangeEvent<>(key, val, destination);
  }

  public TestChangeEvent<Object, Object> of(String destination, Integer id, String operation) {
    return of(destination, id, operation, TestUtil.randomString(12), Instant.now().toEpochMilli());
  }

  public TestChangeEvent<Object, Object> of(String destination, Integer id, String operation, String name) {
    return of(destination, id, operation, name, Instant.now().toEpochMilli());
  }

  public TestChangeEvent<Object, Object> of(String destination, Integer id, String operation, Long epoch) {
    return of(destination, id, operation, TestUtil.randomString(12), epoch);
  }

  public TestChangeEvent<Object, Object> ofNoKey(String destination, Integer id, String operation, String name,
                                                        Long epoch) {
    final RecordConverter t = builder
        .destination(destination)
        .addField("id", id)
        .addField("first_name", name)
        .addField("__op", operation)
        .addField("__source_ts_ms", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    final String val = "{" +
        "\"schema\":" + t.schemaConverter().valueSchema() + "," +
        "\"payload\":" + t.value() +
        "} ";
    return new TestChangeEvent<>(null, val, destination);
  }

  public TestChangeEvent<String, String> of(String key, String val) {
    return new TestChangeEvent<>(key, val, "test");
  }
}
