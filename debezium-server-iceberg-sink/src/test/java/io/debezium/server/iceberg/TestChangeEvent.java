/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;


import io.debezium.engine.ChangeEvent;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;
import io.debezium.server.iceberg.testresources.TestUtil;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * helper class used to generate test debezium change events
 *
 * @author Ismail Simsek
 */
public class TestChangeEvent<K, V> implements ChangeEvent<K, V>, RecordChangeEvent<V> {

  private final K key;
  private final V value;
  private final String destination;

  public TestChangeEvent(K key, V value, String destination) {
    this.key = key;
    this.value = value;
    this.destination = destination;
  }

  public TestChangeEvent(V value) {
    this(null, value, null);
  }

  public byte[] getKeyBytes() {
    return this.key.toString().getBytes(StandardCharsets.UTF_8);
  }

  public byte[] getValueBytes() {
    return this.value.toString().getBytes(StandardCharsets.UTF_8);
  }

  public RecordConverter toIcebergChangeEvent() {
    return new RecordConverter(this.destination(), this.getValueBytes(), this.getKeyBytes());
  }

  public static TestChangeEvent<Object, Object> of(String destination, Integer id, String operation, String name,
                                                   Long epoch) {
    final RecordConverter t = new IcebergChangeEventBuilder()
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

  public static TestChangeEvent<Object, Object> ofCompositeKey(String destination, Integer id, String operation, String name,
                                                               Long epoch) {
    final RecordConverter t = new IcebergChangeEventBuilder()
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

  public static TestChangeEvent<Object, Object> of(String destination, Integer id, String operation) {
    return of(destination, id, operation, TestUtil.randomString(12), Instant.now().toEpochMilli());
  }

  public static TestChangeEvent<Object, Object> of(String destination, Integer id, String operation, String name) {
    return of(destination, id, operation, name, Instant.now().toEpochMilli());
  }

  public static TestChangeEvent<Object, Object> of(String destination, Integer id, String operation, Long epoch) {
    return of(destination, id, operation, TestUtil.randomString(12), epoch);
  }

  public static TestChangeEvent<Object, Object> ofNoKey(String destination, Integer id, String operation, String name,
                                                        Long epoch) {
    final RecordConverter t = new IcebergChangeEventBuilder()
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

  @Override
  public K key() {
    return key;
  }

  @Override
  public V value() {
    return value;
  }

  @Override
  public V record() {
    return value;
  }

  @Override
  public String destination() {
    return destination;
  }
  public Integer partition() {
    return 0;
  }
  @Override
  public String toString() {
    return "EmbeddedEngineChangeEvent [key=" + key + ", value=" + value + ", sourceRecord=" + destination + "]";
  }

}
