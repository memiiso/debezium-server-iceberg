/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.internal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Internal thread-safe bounded LRU cache that computes missing values outside
 * the lock so failed
 * computations never pollute the cache.
 */
public final class LruCache<K, V> {

  private final Map<K, V> map;

  public LruCache(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be > 0, got: " + maxSize);
    }
    this.map = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
      }
    };
  }

  public V get(K key) {
    synchronized (map) {
      return map.get(key);
    }
  }

  public void put(K key, V value) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    synchronized (map) {
      map.put(key, value);
    }
  }

  public void putIfAbsent(K key, V value) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    synchronized (map) {
      map.putIfAbsent(key, value);
    }
  }

  public V getOrCompute(K key, Function<K, V> loader) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(loader, "loader must not be null");
    synchronized (map) {
      V existing = map.get(key);
      if (existing != null) {
        return existing;
      }
    }
    V computed = Objects.requireNonNull(loader.apply(key), "computed value must not be null");
    synchronized (map) {
      V raced = map.get(key);
      if (raced != null) {
        return raced;
      }
      map.put(key, computed);
      return computed;
    }
  }

  public int size() {
    synchronized (map) {
      return map.size();
    }
  }
}
