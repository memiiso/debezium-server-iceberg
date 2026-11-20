/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LruCacheTest {

  @Test
  void testInvalidMaxSizeRejected() {
    assertThrows(IllegalArgumentException.class, () -> new LruCache<>(0));
    assertThrows(IllegalArgumentException.class, () -> new LruCache<>(-5));
  }

  @Test
  void testAccessOrderEvictionAndPutIfAbsent() {
    LruCache<String, Integer> cache = new LruCache<>(2);
    cache.put("a", 1);
    cache.put("b", 2);
    assertEquals(1, cache.get("a")); // promotes "a"
    cache.put("c", 3); // evicts "b"

    assertEquals(2, cache.size());
    assertEquals(1, cache.get("a"));
    assertNull(cache.get("b"));
    assertEquals(3, cache.get("c"));

    cache.putIfAbsent("a", 99);
    assertEquals(1, cache.get("a"));
  }

  @Test
  void testGetOrComputeAndNullRejection() {
    LruCache<String, String> cache = new LruCache<>(4);
    AtomicInteger calls = new AtomicInteger();

    assertEquals(
        "val-k1",
        cache.getOrCompute("k1", k -> "val-" + k + (calls.incrementAndGet() > 0 ? "" : "")));
    assertEquals("val-k1", cache.getOrCompute("k1", k -> "other-" + calls.incrementAndGet()));
    assertEquals(1, calls.get());

    assertThrows(NullPointerException.class, () -> cache.put(null, "v"));
    assertThrows(NullPointerException.class, () -> cache.put("k", null));
    assertThrows(NullPointerException.class, () -> cache.putIfAbsent(null, "v"));
    assertThrows(NullPointerException.class, () -> cache.putIfAbsent("k", null));
    assertThrows(NullPointerException.class, () -> cache.getOrCompute(null, k -> "v"));
    assertThrows(NullPointerException.class, () -> cache.getOrCompute("k", null));
    assertThrows(NullPointerException.class, () -> cache.getOrCompute("k2", k -> null));
    assertNull(cache.get("k2"));
  }
}
