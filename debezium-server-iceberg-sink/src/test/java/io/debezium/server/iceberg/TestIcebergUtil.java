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
  final String unwrapWithArraySchema = Testing.Files.readResourceAsString("json/serde-with-array.json");

  @Test
  public void testNestedJsonRecord() throws JsonProcessingException {
    List<Types.NestedField> d = IcebergUtil.getIcebergSchema(new ObjectMapper().readTree(serdeWithSchema).get("schema"));
    assertTrue(d.toString().contains("before: optional struct<2: id: optional int, 3: first_name: optional string, 4:"));
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
  public void testNestedArrayJsonRecord() throws JsonProcessingException {
    JsonNode jsonData = new ObjectMapper().readTree(unwrapWithArraySchema);
    JsonNode jsonPayload = jsonData.get("payload");
    JsonNode jsonSchema = jsonData.get("schema");
    List<Types.NestedField> schemaFields = IcebergUtil.getIcebergSchema(jsonSchema);
    Schema schema = new Schema(schemaFields);
    assertTrue(schema.asStruct().toString().contains("struct<1: name: optional string, 2: pay_by_quarter: optional list<int>, 4: schedule: optional list<string>, 6:"));
    //System.out.println(schema.asStruct());
    GenericRecord record = IcebergUtil.getIcebergRecord(schema.asStruct(), jsonPayload);
    //System.out.println(record);
    assertTrue( record.toString().contains("[10000, 10001, 10002, 10003]"));
  }
  
  @Test
  public void testNestedGeomJsonRecord() throws JsonProcessingException {
    JsonNode jsonData = new ObjectMapper().readTree(unwrapWithGeomSchema);
    JsonNode jsonPayload = jsonData.get("payload");
    JsonNode jsonSchema = jsonData.get("schema");
    List<Types.NestedField> schemaFields = IcebergUtil.getIcebergSchema(jsonSchema);
    Schema schema = new Schema(schemaFields);
    GenericRecord record = IcebergUtil.getIcebergRecord(schema.asStruct(), jsonPayload);
    //System.out.println(schema);
    //System.out.println(record);
    assertTrue(schema.toString().contains("g: optional struct<3: wkb: optional string, 4: srid: optional int>"));
    GenericRecord g = (GenericRecord) record.getField("g");
    GenericRecord h = (GenericRecord) record.getField("h");
    assertEquals("AQEAAAAAAAAAAADwPwAAAAAAAPA/", g.get(0, Types.StringType.get().typeId().javaClass()));
    assertEquals(123, g.get(1, Types.IntegerType.get().typeId().javaClass()));
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
