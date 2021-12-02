/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.IcebergChangeEvent;

public class TestIcebergChangeEvent<K, V> extends IcebergChangeEvent<K, V> {

  private final K key;
  private final V value;
  private final String destination;

  public TestIcebergChangeEvent(K key, V value, String destination) {
    super(null);
    this.key = key;
    this.value = value;
    this.destination = destination;
  }
  
  public TestIcebergChangeEvent(K key) {
    this(key,null,null);
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
  public String destination() {
    return destination;
  }

  @Override
  public String toString() {
    return "EmbeddedEngineChangeEvent [key=" + key + ", value=" + value + ", sourceRecord=" + destination + "]";
  }
}
