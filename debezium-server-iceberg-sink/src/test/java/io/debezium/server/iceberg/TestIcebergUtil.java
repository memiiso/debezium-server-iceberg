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
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;
import static io.debezium.server.iceberg.IcebergChangeConsumer.mapper;
import static org.junit.jupiter.api.Assertions.*;

class TestIcebergUtil {
  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");
  final String unwrapWithGeomSchema = Testing.Files.readResourceAsString("json/serde-with-schema_geom.json");
  final String unwrapWithArraySchema = Testing.Files.readResourceAsString("json/serde-with-array.json");
  final String unwrapWithArraySchema2 = Testing.Files.readResourceAsString("json/serde-with-array2.json");

  @Test
  public void testNestedJsonRecord() throws JsonProcessingException {
    IcebergChangeEvent e = new IcebergChangeEvent("test",
        mapper.readTree(serdeWithSchema).get("payload"), null,
        mapper.readTree(serdeWithSchema).get("schema"), null);
    Schema schema = e.icebergSchema();
    assertTrue(schema.toString().contains("before: optional struct<2: id: optional int, 3: first_name: optional string, " +
                                          "4:"));
  }

  @Test
  public void testUnwrapJsonRecord() throws IOException {
    IcebergChangeEvent e = new IcebergChangeEvent("test",
        mapper.readTree(unwrapWithSchema).get("payload"), null,
        mapper.readTree(unwrapWithSchema).get("schema"), null);
    Schema schema = e.icebergSchema();
    GenericRecord record = e.asIcebergRecord(schema);
    assertEquals("orders", record.getField("__table").toString());
    assertEquals(16850, record.getField("order_date"));
    System.out.println(schema);
    System.out.println(record);
  }

  @Test
  public void testNestedArrayJsonRecord() throws JsonProcessingException {
    IcebergChangeEvent e = new IcebergChangeEvent("test",
        mapper.readTree(unwrapWithArraySchema).get("payload"), null,
        mapper.readTree(unwrapWithArraySchema).get("schema"), null);
    Schema schema = e.icebergSchema();
    assertTrue(schema.asStruct().toString().contains("struct<1: name: optional string, 2: pay_by_quarter: optional list<int>, 4: schedule: optional list<string>, 6:"));
    System.out.println(schema.asStruct());
    System.out.println(schema.findField("pay_by_quarter").type().asListType().elementType());
    System.out.println(schema.findField("schedule").type().asListType().elementType());
    assertEquals(schema.findField("pay_by_quarter").type().asListType().elementType().toString(), "int");
    assertEquals(schema.findField("schedule").type().asListType().elementType().toString(), "string");
    GenericRecord record = e.asIcebergRecord(schema);
    //System.out.println(record);
    assertTrue(record.toString().contains("[10000, 10001, 10002, 10003]"));
  }

  @Test
  public void testNestedArray2JsonRecord() throws JsonProcessingException {
    assertThrows(RuntimeException.class, () -> {
      IcebergChangeEvent e = new IcebergChangeEvent("test",
          mapper.readTree(unwrapWithArraySchema2).get("payload"), null,
          mapper.readTree(unwrapWithArraySchema2).get("schema"), null);
      Schema schema = e.icebergSchema();
      System.out.println(schema.asStruct());
      System.out.println(schema);
      System.out.println(schema.findField("tableChanges"));
      System.out.println(schema.findField("tableChanges").type().asListType().elementType());
    });
    //GenericRecord record = IcebergUtil.getIcebergRecord(schema.asStruct(), jsonPayload);
    //System.out.println(record);
  }

  @Test
  public void testNestedGeomJsonRecord() throws JsonProcessingException {
    IcebergChangeEvent e = new IcebergChangeEvent("test",
        mapper.readTree(unwrapWithGeomSchema).get("payload"), null,
        mapper.readTree(unwrapWithGeomSchema).get("schema"), null);
    Schema schema = e.icebergSchema();
    GenericRecord record = e.asIcebergRecord(schema);
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
