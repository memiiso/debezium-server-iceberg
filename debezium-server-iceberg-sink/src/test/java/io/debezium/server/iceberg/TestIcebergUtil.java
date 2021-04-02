/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.serde.DebeziumSerdes;
import io.debezium.util.Testing;

import java.io.IOException;
import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TestIcebergUtil {
  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");

  @Test
  public void testNestedJsonRecord() throws JsonProcessingException {
    Exception exception = assertThrows(Exception.class, () -> IcebergUtil.getIcebergSchema(new ObjectMapper().readTree(serdeWithSchema).get("schema")));
    assertEquals("Event schema containing nested data 'before' cannot process nested data!", exception.getMessage());
  }

  @Test
  public void testUnwrapJsonRecord() throws IOException, InterruptedException {
    JsonNode event = new ObjectMapper().readTree(unwrapWithSchema).get("payload");
    Schema schema = IcebergUtil.getIcebergSchema(new ObjectMapper().readTree(unwrapWithSchema).get("schema"));
    GenericRecord record = IcebergUtil.getIcebergRecord(schema.asStruct(), event);
    assertEquals("orders", record.getField("__table").toString());
    assertEquals(16850, record.getField("order_date"));
  }

  @Test
  public void valuePayloadWithSchemaAsJsonNode() {
    // testing Debezium deserializer
    final Serde<JsonNode> valueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    valueSerde.configure(Collections.emptyMap(), false);
    JsonNode deserializedData = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    System.out.println(deserializedData.getClass().getSimpleName());
    System.out.println(deserializedData.has("payload"));
    assertEquals(deserializedData.getClass().getSimpleName(), "ObjectNode");
    System.out.println(deserializedData);
    assertTrue(deserializedData.has("after"));
    assertTrue(deserializedData.has("op"));
    assertTrue(deserializedData.has("before"));
    assertFalse(deserializedData.has("schema"));

    valueSerde.configure(Collections.singletonMap("from.field", "schema"), false);
    JsonNode deserializedSchema = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    System.out.println(deserializedSchema);
    assertFalse(deserializedSchema.has("schema"));
  }

}
