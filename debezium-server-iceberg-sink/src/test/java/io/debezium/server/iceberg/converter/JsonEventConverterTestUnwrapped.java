/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.server.iceberg.BaseTest;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@QuarkusTest
@TestProfile(JsonEventConverterTestUnwrapped.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class JsonEventConverterTestUnwrapped extends BaseTest {

  @Test
  public void testIcebergSchemaConverterWithNestedKey() throws IOException {

    Assertions.assertFalse(config.debezium().isEventFlatteningEnabled());

    String key = Files.readString(Path.of("src/test/resources/json/serde-unnested-order-key-withschema.json"));
    String val = Files.readString(Path.of("src/test/resources/json/serde-unnested-order-val-withschema.json"));
    EmbeddedEngineChangeEvent dbzEvent = EventFactory.createMockChangeEvent(key, val, "test");

    Exception exception = assertThrows(RuntimeException.class, () -> {
      EventFactory.toIcebergChangeEvent(dbzEvent,config).icebergSchema();
    });
    assertTrue(exception.getMessage().contains("Identifier fields are not supported for unnested events"));

    when(config.iceberg().createIdentifierFields()).thenReturn(false);
    Schema schema = EventFactory.toIcebergChangeEvent(dbzEvent,config).icebergSchema();
    assertEquals(config.debezium().temporalPrecisionMode(), TemporalPrecisionMode.ADAPTIVE);
    assertEquals("""
        table {
          1: before: optional struct<2: order_number: optional int, 3: order_date: optional date, 4: purchaser: optional int, 5: quantity: optional int, 6: product_id: optional int>
          7: after: optional struct<8: order_number: optional int, 9: order_date: optional date, 10: purchaser: optional int, 11: quantity: optional int, 12: product_id: optional int>
          13: source: optional struct<14: version: optional string, 15: connector: optional string, 16: name: optional string, 17: ts_ms: optional long, 18: snapshot: optional string, 19: db: optional string, 20: sequence: optional string, 21: ts_us: optional long, 22: ts_ns: optional long, 23: table: optional string, 24: server_id: optional long, 25: gtid: optional string, 26: file: optional string, 27: pos: optional long, 28: row: optional int, 29: thread: optional long, 30: query: optional string>
          31: transaction: optional struct<32: id: optional string, 33: total_order: optional long, 34: data_collection_order: optional long>
          35: op: optional string
          36: ts_ms: optional long
          37: ts_us: optional long
          38: ts_ns: optional long
        }""", schema.toString());
    assertEquals(Set.of(), schema.identifierFieldIds());
  }

  @Test
  public void testIcebergSchemaConverterWithDelete() throws IOException {

    assertFalse(config.debezium().isEventFlatteningEnabled());

    String key = Files.readString(Path.of("src/test/resources/json/serde-unnested-delete-key-withschema.json"));
    String val = Files.readString(Path.of("src/test/resources/json/serde-unnested-delete-val-withschema.json"));
    EmbeddedEngineChangeEvent dbzEvent = EventFactory.createMockChangeEvent(key, val, "test");
    JsonEventConverter ie = EventFactory.toIcebergChangeEvent(dbzEvent,config);

    Exception exception = assertThrows(RuntimeException.class, () -> {
      ie.icebergSchema();
    });
    assertTrue(exception.getMessage().contains("Identifier fields are not supported for unnested events"));
    // print converted event value!
    // System.out.println(ie.convert(ie.icebergSchema(false),"__op"));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.transforms", ",");
      config.put("debezium.transforms.unwrap.type", "null");

      return config;
    }

  }
}
