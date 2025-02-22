/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class RecordConverterTest {
  final String serdeWithSchema = Files.readString(Path.of("src/test/resources/json/serde-with-schema.json"));
  final String unwrapWithSchema = Files.readString(Path.of("src/test/resources/json/unwrap-with-schema.json"));
  final String unwrapWithGeomSchema = Files.readString(Path.of("src/test/resources/json/serde-with-schema_geom.json"));
  final String unwrapWithArraySchema = Files.readString(Path.of("src/test/resources/json/serde-with-array.json"));
  final String unwrapWithArraySchema2 = Files.readString(Path.of("src/test/resources/json/serde-with-array2.json"));

  @Inject
  IcebergChangeEventBuilder eventBuilder;
  @Inject
  IcebergConsumerConfig config;

  RecordConverterTest() throws IOException {
  }

  @BeforeAll
  static void setup() {
    // configure and set
    IcebergChangeConsumer.valSerde.configure(Collections.emptyMap(), false);
    IcebergChangeConsumer.valDeserializer = IcebergChangeConsumer.valSerde.deserializer();
    // configure and set
    IcebergChangeConsumer.keySerde.configure(Collections.emptyMap(), true);
    IcebergChangeConsumer.keyDeserializer = IcebergChangeConsumer.keySerde.deserializer();
  }

  @Test
  public void testNestedJsonRecord() {
    RecordConverter e = new RecordConverter("test",
        serdeWithSchema.getBytes(StandardCharsets.UTF_8), null, config);
    Schema schema = e.icebergSchema();
    System.out.println(schema.toString());
    assertEquals(schema.toString(), ("""
        table {
          1: before: optional struct<2: id: optional int, 3: first_name: optional string, 4: last_name: optional string, 5: email: optional string>
          6: after: optional struct<7: id: optional int, 8: first_name: optional string, 9: last_name: optional string, 10: email: optional string>
          11: source: optional struct<12: version: optional string, 13: connector: optional string, 14: name: optional string, 15: ts_ms: optional long, 16: snapshot: optional boolean, 17: db: optional string, 18: table: optional string, 19: server_id: optional long, 20: gtid: optional string, 21: file: optional string, 22: pos: optional long, 23: row: optional int, 24: thread: optional long, 25: query: optional string>
          26: op: optional string
          27: ts_ms: optional long
        }"""));
    assertEquals(schema.identifierFieldIds(), Set.of());
  }

  @Test
  public void testUnwrapJsonRecord() {
    RecordConverter e = new RecordConverter("test",
        unwrapWithSchema.getBytes(StandardCharsets.UTF_8), null, config);
    Schema schema = e.icebergSchema();
    RecordWrapper record = e.convert(schema);
    assertEquals("orders", record.getField("__table").toString());
    assertEquals(LocalDate.parse("2016-02-19"), record.getField("order_date"));
    assertEquals(schema.toString(), """
        table {
          1: id: optional int
          2: order_date: optional date
          3: purchaser: optional int
          4: quantity: optional int
          5: product_id: optional int
          6: __op: optional string
          7: __table: optional string
          8: __lsn: optional long
          9: __source_ts_ms: optional timestamptz
          10: __deleted: optional string
        }""");

    assertEquals(schema.identifierFieldIds(), Set.of());
  }

  @Test
  public void testNestedArrayJsonRecord() {
    RecordConverter e = new RecordConverter("test",
        unwrapWithArraySchema.getBytes(StandardCharsets.UTF_8), null, config);

    Schema schema = e.icebergSchema();
    assertEquals(schema.toString(), """
        table {
          1: name: optional string
          2: pay_by_quarter: optional list<int>
          5: schedule: optional list<string>
          8: __op: optional string
          9: __table: optional string
          10: __source_ts_ms: optional timestamptz
          11: __db: optional string
          12: __deleted: optional string
        }""");
    assertEquals(schema.identifierFieldIds(), Set.of());
    assertEquals(schema.findField("pay_by_quarter").type().asListType().elementType().toString(), "int");
    assertEquals(schema.findField("schedule").type().asListType().elementType().toString(), "string");
    RecordWrapper record = e.convert(schema);
    //System.out.println(record);
    assertTrue(record.toString().contains("[10000, 10001, 10002, 10003]"));
  }

  @Test
  public void testNestedArray2JsonRecord() {
    RecordConverter e = new RecordConverter("test",
        unwrapWithArraySchema2.getBytes(StandardCharsets.UTF_8), null, config);
    Schema schema = e.icebergSchema();
    System.out.println(schema);
    assertEquals(schema.toString(), """
        table {
          1: source: optional struct<2: version: optional string, 3: connector: optional string, 4: name: optional string, 5: ts_ms: optional long, 6: snapshot: optional string, 7: db: optional string, 8: sequence: optional string, 9: table: optional string, 10: server_id: optional long, 11: gtid: optional string, 12: file: optional string, 13: pos: optional long, 14: row: optional int, 15: thread: optional long, 16: query: optional string>
          17: databaseName: optional string
          18: schemaName: optional string
          19: ddl: optional string
          20: tableChanges: optional list<struct<22: type: optional string, 23: id: optional string, 24: table: optional struct<25: defaultCharsetName: optional string, 26: primaryKeyColumnNames: optional list<string>, 29: columns: optional list<struct<31: name: optional string, 32: jdbcType: optional int, 33: nativeType: optional int, 34: typeName: optional string, 35: typeExpression: optional string, 36: charsetName: optional string, 37: length: optional int, 38: scale: optional int, 39: position: optional int, 40: optional: optional boolean, 41: autoIncremented: optional boolean, 42: generated: optional boolean>>>>>
        }""");
    assertEquals(schema.identifierFieldIds(), Set.of());
  }

  @Test
  public void testNestedGeomJsonRecord() {
    RecordConverter e = new RecordConverter("test",
        unwrapWithGeomSchema.getBytes(StandardCharsets.UTF_8), null, config);
    Schema schema = e.icebergSchema();
    RecordWrapper record = e.convert(schema);
    assertEquals(schema.toString(), """
        table {
          1: id: optional int
          2: g: optional struct<3: wkb: optional string, 4: srid: optional int>
          5: h: optional struct<6: wkb: optional string, 7: srid: optional int>
          8: __op: optional string
          9: __table: optional string
          10: __source_ts_ms: optional timestamptz
          11: __db: optional string
          12: __deleted: optional string
        }""");
    assertEquals(schema.identifierFieldIds(), Set.of());
    GenericRecord g = (GenericRecord) record.getField("g");
    GenericRecord h = (GenericRecord) record.getField("h");
    assertEquals("AQEAAAAAAAAAAADwPwAAAAAAAPA/", g.get(0, Types.StringType.get().typeId().javaClass()));
    assertEquals(123, g.get(1, Types.IntegerType.get().typeId().javaClass()));
    assertEquals(null, h);
    assertNull(h.get(0, Types.BinaryType.get().typeId().javaClass()));
  }

  @Test
  public void valuePayloadWithSchemaAsJsonNode() {
    // testing Debezium deserializer
    final Serde<JsonNode> valueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    valueSerde.configure(Collections.emptyMap(), false);
    JsonNode deserializedData = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    assertEquals(deserializedData.getClass().getSimpleName(), "ObjectNode");
    assertTrue(deserializedData.has("after"));
    assertTrue(deserializedData.has("op"));
    assertTrue(deserializedData.has("before"));
    assertFalse(deserializedData.has("schema"));
    assertFalse(deserializedData.has("payload"));

    valueSerde.configure(Collections.singletonMap("from.field", "schema"), false);
    JsonNode deserializedSchema = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    assertFalse(deserializedSchema.has("schema"));

  }

  @Test
  public void testIcebergSchemaConverterWithKey() {
    final RecordConverter t1 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "name")
        .addField("__op", "u")
        .addField("__source_ts_ms", 2L)
        .addField("__deleted", false)
        .build();

    Schema schema = t1.icebergSchema();
    assertEquals(schema.toString(), """
        table {
          1: id: required int (id)
          2: first_name: required string (id)
          3: __op: optional string
          4: __source_ts_ms: optional timestamptz
          5: __deleted: optional boolean
        }""");
    assertEquals(schema.identifierFieldIds(), Set.of(1, 2));
  }

}
