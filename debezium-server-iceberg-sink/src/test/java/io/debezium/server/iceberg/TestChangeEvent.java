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

import java.nio.charset.StandardCharsets;

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

  public byte[] getKeyBytes() {
    return this.key.toString().getBytes(StandardCharsets.UTF_8);
  }

  public byte[] getValueBytes() {
    return this.value.toString().getBytes(StandardCharsets.UTF_8);
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

  public RecordConverter toIcebergChangeEvent(IcebergConsumerConfig config) {
    return new RecordConverter(this.destination(), this.getValueBytes(), this.getKeyBytes(), config);
  }

}
