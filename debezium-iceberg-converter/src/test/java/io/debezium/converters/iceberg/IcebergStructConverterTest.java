/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.converters.iceberg.geospatial.GeospatialTypes;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class IcebergStructConverterTest {

  // SRID=4326;POINT Z(1 2 3)
  private static final byte[] EWKB_POINT_Z =
      HexFormat.of().parseHex("01010000a0e6100000000000000000f03f00000000000000400000000000000840");
  // POINT Z(1 2 3)
  private static final byte[] ISO_POINT_Z =
      HexFormat.of().parseHex("01e9030000000000000000f03f00000000000000400000000000000840");

  @Test
  void testConvertFullSourceRecordWithAllTypesAndPrimaryKey() {
    Schema nestedSchema =
        SchemaBuilder.struct()
            .name("Nested")
            .field("nested_str", Schema.STRING_SCHEMA)
            .field("nested_long", Schema.INT64_SCHEMA)
            .optional()
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("col_int", Schema.OPTIONAL_INT32_SCHEMA)
            .field("col_long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("col_float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("col_double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("col_bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("col_string", Schema.OPTIONAL_STRING_SCHEMA)
            .field("col_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .field(
                "col_decimal",
                Decimal.builder(2).optional().parameter("connect.decimal.precision", "6").build())
            .field("col_uuid", Uuid.builder().optional().build())
            .field("col_date", Date.builder().optional().build())
            .field("col_time_micros", MicroTime.builder().optional().build())
            .field("col_ts_micros", MicroTimestamp.builder().optional().build())
            .field("col_ts_zoned", ZonedTimestamp.builder().optional().build())
            .field(
                "col_connect_ts",
                org.apache.kafka.connect.data.Timestamp.builder().optional().build())
            .field("col_list", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(
                "col_map",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .field("col_struct", nestedSchema)
            .field("__ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    Schema keySchema = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();

    UUID uuid = UUID.randomUUID();
    int dateDays = (int) LocalDate.of(2023, 5, 18).toEpochDay();
    long tsMicros = 1651838589000000L;
    long tsMillis = 1651838589000L;
    String zonedTs = "2022-05-06T14:43:09+02:00";

    Struct key = new Struct(keySchema).put("id", 7);
    Struct value =
        new Struct(valueSchema)
            .put("id", 7)
            .put("col_int", 123)
            .put("col_long", 456L)
            .put("col_float", 12.34f)
            .put("col_double", 56.78d)
            .put("col_bool", true)
            .put("col_string", "hello")
            .put("col_bytes", new byte[] {1, 2, 3})
            .put("col_decimal", new BigDecimal("1234.56"))
            .put("col_uuid", uuid.toString())
            .put("col_date", dateDays)
            .put("col_time_micros", 45789000000L)
            .put("col_ts_micros", tsMicros)
            .put("col_ts_zoned", zonedTs)
            .put("col_connect_ts", new java.util.Date(tsMillis))
            .put("col_list", List.of("a", "b"))
            .put("col_map", Map.of("k", 1))
            .put(
                "col_struct",
                new Struct(nestedSchema).put("nested_str", "ns").put("nested_long", 99L))
            .put("__ts_ms", tsMillis);

    SourceRecord sourceRecord =
        new SourceRecord(
            Map.of(), Map.of(), "db.public.orders", null, keySchema, key, valueSchema, value);

    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults());
    ConvertedIcebergRecord converted = converter.convert(sourceRecord);

    assertEquals("db.public.orders", converted.destination());
    assertFalse(converted.schemaChangeEvent());
    assertEquals(Set.of("id"), converted.schema().identifierFieldNames());
    assertEquals(1, converted.sortOrder().fields().size());
    assertEquals(
        "id", converted.schema().findColumnName(converted.sortOrder().fields().get(0).sourceId()));

    GenericRecord record = converted.record();
    assertNotNull(record);
    assertEquals(7, record.getField("id"));
    assertEquals(123, record.getField("col_int"));
    assertEquals(456L, record.getField("col_long"));
    assertEquals(12.34f, record.getField("col_float"));
    assertEquals(56.78d, record.getField("col_double"));
    assertEquals(true, record.getField("col_bool"));
    assertEquals("hello", record.getField("col_string"));
    assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), record.getField("col_bytes"));
    assertEquals(
        0, new BigDecimal("1234.56").compareTo((BigDecimal) record.getField("col_decimal")));
    assertEquals(uuid, record.getField("col_uuid"));
    assertEquals(LocalDate.of(2023, 5, 18), record.getField("col_date"));
    assertEquals(45789000000L, record.getField("col_time_micros"));
    assertEquals(
        LocalDateTime.ofInstant(Instant.ofEpochMilli(tsMillis), ZoneOffset.UTC),
        record.getField("col_ts_micros"));
    assertEquals(OffsetDateTime.parse(zonedTs), record.getField("col_ts_zoned"));
    assertEquals(
        LocalDateTime.ofInstant(Instant.ofEpochMilli(tsMillis), ZoneOffset.UTC),
        record.getField("col_connect_ts"));
    assertEquals(List.of("a", "b"), record.getField("col_list"));
    assertEquals(Map.of("k", 1), record.getField("col_map"));
    GenericRecord nested = (GenericRecord) record.getField("col_struct");
    assertEquals("ns", nested.getField("nested_str"));
    assertEquals(99L, nested.getField("nested_long"));
    assertEquals(
        Instant.ofEpochMilli(tsMillis).atOffset(ZoneOffset.UTC), record.getField("__ts_ms"));
  }

  @Test
  void testTombstoneWithAndWithoutValueSchema() {
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Struct key = new Struct(keySchema).put("id", 1);

    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults());

    // Tombstone with valueSchema present -> schema preserved, record is null
    SourceRecord tombstoneWithSchema =
        new SourceRecord(Map.of(), Map.of(), "db.t", null, keySchema, key, valueSchema, null);
    ConvertedIcebergRecord resultWithSchema = converter.convert(tombstoneWithSchema);
    assertEquals("db.t", resultWithSchema.destination());
    assertNotNull(resultWithSchema.schema());
    assertEquals(Set.of("id"), resultWithSchema.schema().identifierFieldNames());
    assertNull(resultWithSchema.record());
    assertFalse(resultWithSchema.schemaChangeEvent());

    // Tombstone without valueSchema -> schema null, record null, unsorted
    SourceRecord tombstoneNoSchema =
        new SourceRecord(Map.of(), Map.of(), "db.t", null, keySchema, key, null, null);
    ConvertedIcebergRecord resultNoSchema = converter.convert(tombstoneNoSchema);
    assertEquals("db.t", resultNoSchema.destination());
    assertNull(resultNoSchema.schema());
    assertEquals(SortOrder.unsorted(), resultNoSchema.sortOrder());
    assertNull(resultNoSchema.record());
    assertFalse(resultNoSchema.schemaChangeEvent());
  }

  @Test
  void testNestedAsVariantSuppressesIdentifierFieldsAndSortOrder() {
    Schema nestedSchema =
        SchemaBuilder.struct().field("k", Schema.STRING_SCHEMA).optional().build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("payload", nestedSchema)
            .build();
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

    Struct value =
        new Struct(valueSchema).put("id", 1).put("payload", new Struct(nestedSchema).put("k", "v"));
    SourceRecord sr =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "db.variant_tbl",
            null,
            keySchema,
            new Struct(keySchema).put("id", 1),
            valueSchema,
            value);

    IcebergConverterConfig config =
        ConnectIcebergConverterConfig.fromMap(
            Map.of("create.identifier.fields", "true", "nested.as.variant", "true"));
    IcebergStructConverter converter = new IcebergStructConverter(config);

    ConvertedIcebergRecord converted = converter.convert(sr);
    assertTrue(converted.schema().identifierFieldIds().isEmpty());
    assertEquals(SortOrder.unsorted(), converted.sortOrder());
    assertEquals(Types.VariantType.get(), converted.schema().findType("payload"));
    Variant variant = assertInstanceOf(Variant.class, converted.record().getField("payload"));
    assertEquals("v", variant.value().asObject().get("k").asPrimitive().get());
  }

  @Test
  void testSchemaChangeEventDetectionAndIdentifierSuppression() {
    Schema ddlSchema =
        SchemaBuilder.struct()
            .field("ddl", Schema.OPTIONAL_STRING_SCHEMA)
            .field("databaseName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tableChanges", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema keySchema =
        SchemaBuilder.struct().field("databaseName", Schema.OPTIONAL_STRING_SCHEMA).build();

    assertTrue(
        new StructSchemaConverter(ddlSchema, keySchema, ConnectIcebergConverterConfig.defaults())
            .isSchemaChangeEvent());
    assertFalse(
        new StructSchemaConverter(null, null, ConnectIcebergConverterConfig.defaults())
            .isSchemaChangeEvent());
    assertFalse(
        new StructSchemaConverter(
                Schema.STRING_SCHEMA, null, ConnectIcebergConverterConfig.defaults())
            .isSchemaChangeEvent());
    assertFalse(
        new StructSchemaConverter(keySchema, null, ConnectIcebergConverterConfig.defaults())
            .isSchemaChangeEvent());

    Struct value =
        new Struct(ddlSchema)
            .put("ddl", "ALTER TABLE t ADD COLUMN c INT")
            .put("databaseName", "mydb")
            .put("tableChanges", "[]");
    SourceRecord sr =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "mydb",
            null,
            keySchema,
            new Struct(keySchema).put("databaseName", "mydb"),
            ddlSchema,
            value);

    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults());
    ConvertedIcebergRecord converted = converter.convert(sr);
    assertTrue(converted.schemaChangeEvent());
    assertTrue(converted.schema().identifierFieldIds().isEmpty());
    assertEquals(SortOrder.unsorted(), converted.sortOrder());
    assertEquals("ALTER TABLE t ADD COLUMN c INT", converted.record().getField("ddl"));
  }

  @Test
  void testGeospatialStructAndArrayConversion() {
    Schema geomSchema =
        SchemaBuilder.struct()
            .name(GeospatialTypes.GEOMETRY_LOGICAL_NAME)
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("g", geomSchema)
            .field("ga", SchemaBuilder.array(geomSchema).optional().build())
            .build();

    Struct pointWithSrid = new Struct(geomSchema).put("wkb", EWKB_POINT_Z).put("srid", 4326);
    Struct pointNoSrid = new Struct(geomSchema).put("wkb", EWKB_POINT_Z);

    IcebergStructConverter strictConverter =
        new IcebergStructConverter(
            ConnectIcebergConverterConfig.fromMap(Map.of("geospatial.types.enabled", "true")));
    SourceRecord okRecord =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "geo",
            null,
            null,
            null,
            valueSchema,
            new Struct(valueSchema)
                .put("id", 1)
                .put("g", pointWithSrid)
                .put("ga", List.of(pointWithSrid)));
    ConvertedIcebergRecord ok = strictConverter.convert(okRecord);
    assertEquals(Types.GeometryType.crs84(), ok.schema().findType("g"));
    assertEquals(ByteBuffer.wrap(ISO_POINT_Z), ok.record().getField("g"));
    assertEquals(List.of(ByteBuffer.wrap(ISO_POINT_Z)), ok.record().getField("ga"));

    SourceRecord missingSridRecord =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "geo",
            null,
            null,
            null,
            valueSchema,
            new Struct(valueSchema).put("id", 2).put("g", pointNoSrid));
    assertThrows(DebeziumException.class, () -> strictConverter.convert(missingSridRecord));

    IcebergStructConverter lenientConverter =
        new IcebergStructConverter(
            ConnectIcebergConverterConfig.fromMap(
                Map.of(
                    "geospatial.types.enabled", "true", "geospatial.allow.missing.srid", "true")));
    ConvertedIcebergRecord lenient = lenientConverter.convert(missingSridRecord);
    assertEquals(ByteBuffer.wrap(ISO_POINT_Z), lenient.record().getField("g"));
  }

  @Test
  void testReplaceNullWithDefaultAndExcludedColumns() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field(
                "col_default", SchemaBuilder.string().optional().defaultValue("fallback").build())
            .field("secret", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Struct value =
        new Struct(valueSchema).put("id", 1).put("col_default", null).put("secret", "hidden");
    SourceRecord sr =
        new SourceRecord(Map.of(), Map.of(), "t", null, null, null, valueSchema, value);

    IcebergStructConverter defaultOff =
        new IcebergStructConverter(
            ConnectIcebergConverterConfig.fromMap(Map.of("excluded.columns", "secret")));
    ConvertedIcebergRecord r1 = defaultOff.convert(sr);
    assertNull(r1.schema().findField("secret"));
    assertNull(r1.record().getField("col_default"));

    IcebergStructConverter defaultOn =
        new IcebergStructConverter(
            ConnectIcebergConverterConfig.fromMap(
                Map.of("replace.null.with.default", "true", "excluded.columns", "secret")));
    ConvertedIcebergRecord r2 = defaultOn.convert(sr);
    assertEquals("fallback", r2.record().getField("col_default"));
  }

  @Test
  void testSchemaCachingHitBoundedEvictionAndDisabled() {
    Schema s1 = SchemaBuilder.struct().name("S1").field("id", Schema.INT32_SCHEMA).build();
    Schema s2 = SchemaBuilder.struct().name("S2").field("id", Schema.INT32_SCHEMA).build();
    Schema s3 = SchemaBuilder.struct().name("S3").field("id", Schema.INT32_SCHEMA).build();

    SourceRecord sr1 =
        new SourceRecord(
            Map.of(), Map.of(), "t1", null, null, null, s1, new Struct(s1).put("id", 1));
    SourceRecord sr2 =
        new SourceRecord(
            Map.of(), Map.of(), "t2", null, null, null, s2, new Struct(s2).put("id", 2));
    SourceRecord sr3 =
        new SourceRecord(
            Map.of(), Map.of(), "t3", null, null, null, s3, new Struct(s3).put("id", 3));

    IcebergStructConverter cached =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults(), 2);

    // Derived Iceberg schema is cached across convert() calls
    ConvertedIcebergRecord c1 = cached.convert(sr1);
    ConvertedIcebergRecord c2 = cached.convert(sr1);
    assertSame(c1.schema(), c2.schema());
    assertEquals(1, cached.currentSchemaCacheSize());

    // Bounded LRU eviction at size 2
    cached.convert(sr2);
    assertEquals(2, cached.currentSchemaCacheSize());
    cached.convert(sr3);
    assertEquals(2, cached.currentSchemaCacheSize());
    assertNotSame(c1.schema(), cached.convert(sr1).schema());

    // Cache disabled (size = 0)
    IcebergStructConverter uncached =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults(), 0);
    ConvertedIcebergRecord u1 = uncached.convert(sr1);
    ConvertedIcebergRecord u2 = uncached.convert(sr1);
    assertNotSame(u1.schema(), u2.schema());
    assertEquals(0, uncached.currentSchemaCacheSize());
  }

  @Test
  void testErrorPathsAndDirectConvertToIcebergRecord() {
    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults());

    assertThrows(
        IllegalArgumentException.class,
        () -> new IcebergStructConverter(ConnectIcebergConverterConfig.defaults(), -1));
    assertThrows(NullPointerException.class, () -> converter.convert(null));

    SourceRecord nonStructValue =
        new SourceRecord(
            Map.of(), Map.of(), "t", null, null, null, Schema.STRING_SCHEMA, "not-a-struct");
    assertThrows(DebeziumException.class, () -> converter.convert(nonStructValue));

    // Direct convertToIcebergRecord with null struct returns empty GenericRecord
    org.apache.iceberg.Schema targetSchema =
        new org.apache.iceberg.Schema(Types.NestedField.optional(1, "x", Types.StringType.get()));
    GenericRecord empty = converter.convertToIcebergRecord(targetSchema.asStruct(), null);
    assertNotNull(empty);
    assertNull(empty.getField("x"));

    // Type mismatch on complex fields throws IllegalArgumentException
    org.apache.iceberg.Schema structTarget =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(
                1,
                "s",
                Types.StructType.of(Types.NestedField.optional(2, "a", Types.StringType.get()))));
    Schema badConnectSchema = SchemaBuilder.struct().field("s", Schema.STRING_SCHEMA).build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.convertToIcebergRecord(
                structTarget.asStruct(), new Struct(badConnectSchema).put("s", "wrong")));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testListAndMapWithLogicalTemporalAndStructElements() {
    Schema itemSchema =
        SchemaBuilder.struct()
            .name("Item")
            .field("item_id", Schema.INT32_SCHEMA)
            .field("ts_nano", NanoTimestamp.builder().optional().build())
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("ComplexCollections")
            .field("id", Schema.INT32_SCHEMA)
            .field(
                "ts_millis_list",
                SchemaBuilder.array(Timestamp.builder().optional().build()).optional().build())
            .field(
                "ts_nanos_list",
                SchemaBuilder.array(NanoTimestamp.builder().optional().build()).optional().build())
            .field(
                "ts_map",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.builder().optional().build())
                    .optional()
                    .build())
            .field(
                "zoned_map",
                SchemaBuilder.map(Schema.STRING_SCHEMA, ZonedTimestamp.builder().optional().build())
                    .optional()
                    .build())
            .field("struct_list", SchemaBuilder.array(itemSchema).optional().build())
            .field(
                "struct_map",
                SchemaBuilder.map(Schema.STRING_SCHEMA, itemSchema).optional().build())
            .build();

    long millis = 1700000000123L;
    long nanos = millis * 1_000_000L;
    LocalDateTime expectedLdt =
        Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).toLocalDateTime();
    String zoned = "2023-11-14T22:13:20.123+02:00";

    Struct item = new Struct(itemSchema).put("item_id", 10).put("ts_nano", nanos);
    Struct value =
        new Struct(valueSchema)
            .put("id", 1)
            .put("ts_millis_list", List.of(millis))
            .put("ts_nanos_list", List.of(nanos))
            .put("ts_map", Map.of("k1", millis))
            .put("zoned_map", Map.of("z1", zoned))
            .put("struct_list", List.of(item))
            .put("struct_map", Map.of("i1", item));

    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults());
    ConvertedIcebergRecord converted =
        converter.convert(
            new SourceRecord(
                Map.of(), Map.of(), "collections", null, null, null, valueSchema, value));

    GenericRecord record = converted.record();
    assertEquals(List.of(expectedLdt), record.getField("ts_millis_list"));
    assertEquals(List.of(expectedLdt), record.getField("ts_nanos_list"));
    assertEquals(Map.of("k1", expectedLdt), record.getField("ts_map"));
    assertEquals(Map.of("z1", OffsetDateTime.parse(zoned)), record.getField("zoned_map"));

    List<GenericRecord> structList = (List<GenericRecord>) record.getField("struct_list");
    assertEquals(1, structList.size());
    assertEquals(10, structList.get(0).getField("item_id"));
    assertEquals(expectedLdt, structList.get(0).getField("ts_nano"));

    Map<String, GenericRecord> structMap =
        (Map<String, GenericRecord>) record.getField("struct_map");
    assertEquals(10, structMap.get("i1").getField("item_id"));
    assertEquals(expectedLdt, structMap.get("i1").getField("ts_nano"));
  }

  @Test
  void testGeospatialMapErrorReportsFieldName() {
    Schema geomSchema =
        SchemaBuilder.struct()
            .name(GeospatialTypes.GEOMETRY_LOGICAL_NAME)
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("geo_map", SchemaBuilder.map(Schema.STRING_SCHEMA, geomSchema).build())
            .build();
    Struct badPoint = new Struct(geomSchema).put("wkb", EWKB_POINT_Z).put("srid", 3857);
    Struct value = new Struct(valueSchema).put("geo_map", Map.of("a", badPoint));

    IcebergStructConverter converter =
        new IcebergStructConverter(
            ConnectIcebergConverterConfig.fromMap(Map.of("geospatial.types.enabled", "true")));
    DebeziumException ex =
        assertThrows(
            DebeziumException.class,
            () ->
                converter.convert(
                    new SourceRecord(
                        Map.of(), Map.of(), "geo", null, null, null, valueSchema, value)));
    assertTrue(ex.getMessage().contains("field: geo_map.value:"));
  }

  @Test
  void testPrimitiveKeyAndFallbackSchemaInferenceAndNoCachePollutionOnFailure() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("Val")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema keySchema = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();

    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults(), 4);

    // Primitive key schema is handled cleanly without identifier fields
    SourceRecord primitiveKeyRecord =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "t_prim",
            null,
            Schema.INT32_SCHEMA,
            7,
            valueSchema,
            new Struct(valueSchema).put("id", 7).put("name", "ok"));
    ConvertedIcebergRecord primResult = converter.convert(primitiveKeyRecord);
    assertTrue(primResult.schema().identifierFieldIds().isEmpty());
    assertEquals(SortOrder.unsorted(), primResult.sortOrder());
    assertEquals(7, primResult.record().getField("id"));
    assertEquals(1, converter.currentSchemaCacheSize());

    // Null valueSchema / keySchema inferred from Struct.schema()
    SourceRecord inferredRecord =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "t_inferred",
            null,
            null,
            new Struct(keySchema).put("id", 9),
            null,
            new Struct(valueSchema).put("id", 9).put("name", "inferred"));
    ConvertedIcebergRecord inferred = converter.convert(inferredRecord);
    assertEquals(Set.of("id"), inferred.schema().identifierFieldNames());
    assertEquals("inferred", inferred.record().getField("name"));
    assertEquals(2, converter.currentSchemaCacheSize());

    // Non-struct valueSchema tombstone throws without polluting cache
    SourceRecord badTombstone =
        new SourceRecord(Map.of(), Map.of(), "t_bad", null, null, null, Schema.STRING_SCHEMA, null);
    assertThrows(DebeziumException.class, () -> converter.convert(badTombstone));
    assertEquals(2, converter.currentSchemaCacheSize());

    // Empty struct schema throws without polluting cache
    Schema emptyStructSchema = SchemaBuilder.struct().name("Empty").build();
    SourceRecord emptyStructRecord =
        new SourceRecord(
            Map.of(),
            Map.of(),
            "t_empty",
            null,
            null,
            null,
            emptyStructSchema,
            new Struct(emptyStructSchema));
    assertThrows(RuntimeException.class, () -> converter.convert(emptyStructRecord));
    assertEquals(2, converter.currentSchemaCacheSize());
  }

  @Test
  void testConcurrentSchemaCacheAccessStaysBoundedAndConsistent() throws Exception {
    int cacheLimit = 8;
    int numSchemas = 24;
    int threads = 8;
    int iterationsPerThread = 50;

    IcebergStructConverter converter =
        new IcebergStructConverter(ConnectIcebergConverterConfig.defaults(), cacheLimit);

    List<Schema> schemas = new ArrayList<>();
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    for (int i = 0; i < numSchemas; i++) {
      schemas.add(
          SchemaBuilder.struct()
              .name("Schema_" + i)
              .field("id", Schema.INT32_SCHEMA)
              .field("col_" + i, Schema.OPTIONAL_STRING_SCHEMA)
              .build());
    }

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Callable<Void>> tasks = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int threadId = t;
        tasks.add(
            () -> {
              for (int iter = 0; iter < iterationsPerThread; iter++) {
                int idx = (threadId * 7 + iter) % numSchemas;
                Schema vs = schemas.get(idx);
                Struct key = new Struct(keySchema).put("id", iter);
                Struct val =
                    new Struct(vs).put("id", iter).put("col_" + idx, "v_" + threadId + "_" + iter);
                ConvertedIcebergRecord result =
                    converter.convert(
                        new SourceRecord(
                            Map.of(), Map.of(), "topic_" + idx, null, keySchema, key, vs, val));
                assertEquals(Set.of("id"), result.schema().identifierFieldNames());
                assertEquals(iter, result.record().getField("id"));
                assertEquals("v_" + threadId + "_" + iter, result.record().getField("col_" + idx));
              }
              return null;
            });
      }
      List<Future<Void>> futures = pool.invokeAll(tasks);
      for (Future<Void> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdownNow();
    }

    assertTrue(
        converter.currentSchemaCacheSize() <= cacheLimit,
        "Cache size should not exceed limit: " + converter.currentSchemaCacheSize());
  }
}
