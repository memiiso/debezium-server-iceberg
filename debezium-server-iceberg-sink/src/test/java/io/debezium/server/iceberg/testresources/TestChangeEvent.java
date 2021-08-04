/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.RecordChangeEvent;

public class TestChangeEvent<K, V> implements ChangeEvent<K, V>, RecordChangeEvent<V> {

  private final K key;
  private final V value;
  private final String destination;

  public TestChangeEvent(K key, V value, String destination) {
    this.key = key;
    this.value = value;
    this.destination = destination;
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

  @Override
  public String toString() {
    return "EmbeddedEngineChangeEvent [key=" + key + ", value=" + value + ", sourceRecord=" + destination + "]";
  }
}
