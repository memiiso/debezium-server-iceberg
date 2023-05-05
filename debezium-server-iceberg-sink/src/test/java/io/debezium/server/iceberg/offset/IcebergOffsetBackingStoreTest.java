/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.offset;

import io.debezium.server.iceberg.testresources.S3Minio;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(IcebergOffsetBackingStoreTest.TestProfile.class)
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
public class IcebergOffsetBackingStoreTest {

  private static final Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();
  private static final Map<ByteBuffer, ByteBuffer> secondSet = new HashMap<>();

  public static String fromByteBuffer(ByteBuffer data) {
    return (data != null) ? String.valueOf(StandardCharsets.UTF_16.decode(data.asReadOnlyBuffer())) : null;
  }

  public static ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_16)) : null;
  }

  @BeforeClass
  public static void setup() {
    firstSet.put(toByteBuffer("key"), toByteBuffer("value"));
    firstSet.put(toByteBuffer("key2"), null);
    secondSet.put(toByteBuffer("key1secondSet"), toByteBuffer("value1secondSet"));
    secondSet.put(toByteBuffer("key2secondSet"), toByteBuffer("value2secondSet"));
  }

  public Map<String, String> config() {
    Map<String, String> conf = new HashMap<>();
    for (String propName : ConfigProvider.getConfig().getPropertyNames()) {
      if (propName.startsWith("debezium")) {
        try {
          conf.put(propName, ConfigProvider.getConfig().getValue(propName, String.class));
        } catch (Exception e) {
          conf.put(propName, "");
        }
      }
    }
    return conf;
  }

  @Test
  public void testInitialize() {
    // multiple initialization should not fail
    // first one should create the table and following ones should use the created table
    IcebergOffsetBackingStore store = new IcebergOffsetBackingStore();
    store.configure(new TestWorkerConfig(config()));
    store.start();
    store.start();
    store.start();
    store.stop();
  }

  @Test
  public void testGetSet() throws Exception {
    Callback<Void> cb = (error, result) -> {
    };

    IcebergOffsetBackingStore store = new IcebergOffsetBackingStore();
    store.configure(new TestWorkerConfig(config()));
    store.start();
    store.set(firstSet, cb).get();

    Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(toByteBuffer("key"), toByteBuffer("bad"))).get();
    assertEquals(toByteBuffer("value"), values.get(toByteBuffer("key")));
    Assert.assertNull(values.get(toByteBuffer("bad")));
  }

  @Test
  public void testSaveRestore() throws Exception {
    Callback<Void> cb = (error, result) -> {
    };

    IcebergOffsetBackingStore store = new IcebergOffsetBackingStore();
    store.configure(new TestWorkerConfig(config()));
    store.start();
    store.set(firstSet, cb).get();
    store.set(secondSet, cb).get();
    store.stop();
    // Restore into a new store mand make sure its correctly reload
    IcebergOffsetBackingStore restore = new IcebergOffsetBackingStore();
    restore.configure(new TestWorkerConfig(config()));
    restore.start();
    Map<ByteBuffer, ByteBuffer> values = restore.get(Collections.singletonList(toByteBuffer("key"))).get();
    Map<ByteBuffer, ByteBuffer> values2 = restore.get(Collections.singletonList(toByteBuffer("key1secondSet"))).get();
    Map<ByteBuffer, ByteBuffer> values3 = restore.get(Collections.singletonList(toByteBuffer("key2secondSet"))).get();
    assertEquals("value", fromByteBuffer(values.get(toByteBuffer("key"))));
    assertEquals(toByteBuffer("value1secondSet"), values2.get(toByteBuffer("key1secondSet")));
    assertEquals(toByteBuffer("value2secondSet"), values3.get(toByteBuffer("key2secondSet")));
  }

  public static class TestWorkerConfig extends WorkerConfig {
    public TestWorkerConfig(Map<String, String> props) {
      super(new ConfigDef(), props);
    }
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.source.offset.storage", "io.debezium.server.iceberg.offset.IcebergOffsetBackingStore");
      config.put("debezium.source.offset.flush.interval.ms", "60000");
      config.put("debezium.source.offset.storage.iceberg.table-name", "debezium_offset_storage_custom_table");
      return config;
    }
  }
}