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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TestIcebergUtil {
  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");
  final String unwrapWithGeomSchema = Testing.Files.readResourceAsString("json/serde-with-schema_geom.json");

  @Test
  public void testNestedJsonRecord() {
    Exception exception = assertThrows(Exception.class, () -> IcebergUtil.getIcebergSchema(new ObjectMapper().readTree(serdeWithSchema).get("schema")));
    assertTrue(exception.getMessage().contains("nested data type"));
  }

  @Test
  public void testUnwrapJsonRecord() throws IOException {
    JsonNode event = new ObjectMapper().readTree(unwrapWithSchema).get("payload");
    List<Types.NestedField> fileds = IcebergUtil.getIcebergSchema(new ObjectMapper().readTree(unwrapWithSchema)
        .get("schema"));
    Schema schema = new Schema(fileds);
    GenericRecord record = IcebergUtil.getIcebergRecord(schema.asStruct(), event);
    assertEquals("orders", record.getField("__table").toString());
    assertEquals(16850, record.getField("order_date"));
  }

  @Test
  public void testNestedGeomJsonRecord() throws JsonProcessingException {
    JsonNode jsonData = new ObjectMapper().readTree(unwrapWithGeomSchema);
    JsonNode jsonPayload = jsonData.get("payload");
    JsonNode jsonSchema = jsonData.get("schema");
    List<Types.NestedField> schemaFields = IcebergUtil.getIcebergSchema(jsonSchema);
    Schema schema = new Schema(schemaFields);
    System.out.println(schema);
    assertTrue(schema.toString().contains("g: optional struct<3: wkb: optional binary, 4: srid: optional int>"));

    GenericRecord record = IcebergUtil.getIcebergRecord(schema.asStruct(), jsonPayload);
    GenericRecord g = (GenericRecord) record.getField("g");
    GenericRecord h = (GenericRecord) record.getField("h");
    assertEquals(123, g.get(1, Types.IntegerType.get().typeId().javaClass()));
    ByteBuffer gwkbBb = (ByteBuffer) g.get(0, Types.BinaryType.get().typeId().javaClass());
    assertEquals("java.nio.HeapByteBuffer[pos=0 lim=21 cap=21]", gwkbBb.toString());
    assertEquals("Record(null, null)", h.toString());
    assertNull(h.get(0, Types.BinaryType.get().typeId().javaClass()));
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
