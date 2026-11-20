/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.converters.iceberg.geospatial.GeospatialTypes;
import io.debezium.converters.iceberg.spi.DeserializedIcebergRecord;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTimestamp;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.variants.Variant;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergConverterTest {

  private static final byte[] SAMPLE_ISO_WKB_POINT =
      new byte[] {
        0x01,
        0x01,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        (byte) 0xF0,
        0x3F,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x40
      };

  private static Schema buildEventSchema() {
    Schema nestedSchema =
        SchemaBuilder.struct()
            .optional()
            .field("city", Schema.OPTIONAL_STRING_SCHEMA)
            .field("code", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    Schema geomSchema =
        SchemaBuilder.struct()
            .name(GeospatialTypes.GEOMETRY_LOGICAL_NAME)
            .optional()
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    return SchemaBuilder.struct()
        .name("test.orders.Value")
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
        .field(
            "amount",
            Decimal.builder(2).parameter("connect.decimal.precision", "10").optional().build())
        .field("uid", Uuid.builder().optional().build())
        .field("day", Date.builder().optional().build())
        .field("created_at", MicroTimestamp.builder().optional().build())
        .field("updated_at", ZonedTimestamp.builder().optional().build())
        .field("raw", Schema.OPTIONAL_BYTES_SCHEMA)
        .field("tags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
        .field(
            "meta",
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build())
        .field("address", nestedSchema)
        .field("loc", geomSchema)
        .field("excluded_col", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
  }

  private static Struct buildEventStruct(Schema schema) {
    Struct address =
        new Struct(schema.field("address").schema()).put("city", "Berlin").put("code", 10115);
    Struct loc =
        new Struct(schema.field("loc").schema()).put("wkb", SAMPLE_ISO_WKB_POINT).put("srid", 4326);

    return new Struct(schema)
        .put("id", 100L)
        .put("name", "widget")
        .put("amount", new BigDecimal("19.95"))
        .put("uid", "123e4567-e89b-12d3-a456-426614174000")
        .put("day", (int) LocalDate.of(2026, 11, 19).toEpochDay())
        .put("created_at", 1_795_000_000_123_456L)
        .put("updated_at", "2026-11-19T12:34:56.123456Z")
        .put("raw", new byte[] {9, 8, 7})
        .put("tags", List.of("cdc", "iceberg"))
        .put("meta", Map.of("v", 3))
        .put("address", address)
        .put("loc", loc)
        .put("excluded_col", "drop-me");
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro-envelope", "avro-single-object"})
  void serializesAndDeserializesIcebergRecordsAcrossAvroCodecs(String serializerType) {
    IcebergConverter converter = new IcebergConverter();
    // Deliberately omit converter.type to match Debezium ConverterBuilder behavior.
    converter.configure(
        Map.of(
            "serializer.type",
            serializerType,
            "geospatial.types.enabled",
            "true",
            "excluded.columns",
            "excluded_col"),
        false);

    Schema connectSchema = buildEventSchema();
    Struct connectStruct = buildEventStruct(connectSchema);

    byte[] encoded =
        converter.fromConnectData("server.public.orders", connectSchema, connectStruct);
    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    DeserializedIcebergRecord iceberg = converter.deserializeIcebergRecord(encoded);
    assertNull(iceberg.schema().findField("excluded_col"));
    GenericRecord rec = iceberg.record();
    assertEquals(100L, rec.getField("id"));
    assertEquals("widget", rec.getField("name"));
    assertEquals(new BigDecimal("19.95"), rec.getField("amount"));
    assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), rec.getField("uid"));
    assertEquals(LocalDate.of(2026, 11, 19), rec.getField("day"));
    assertEquals(
        LocalDateTime.of(2026, 11, 18, 11, 6, 40, 123_456_000), rec.getField("created_at"));
    assertEquals(
        OffsetDateTime.of(2026, 11, 19, 12, 34, 56, 123_456_000, ZoneOffset.UTC),
        rec.getField("updated_at"));
    assertArrayEquals(new byte[] {9, 8, 7}, toArray((ByteBuffer) rec.getField("raw")));
    assertEquals(List.of("cdc", "iceberg"), rec.getField("tags"));
    assertEquals(Map.of("v", 3), rec.getField("meta"));
    GenericRecord addr = (GenericRecord) rec.getField("address");
    assertEquals("Berlin", addr.getField("city"));
    assertEquals(10115, addr.getField("code"));
    assertArrayEquals(SAMPLE_ISO_WKB_POINT, toArray((ByteBuffer) rec.getField("loc")));
  }

  @Test
  void nestedAsVariantSerializesToIcebergVariant() {
    IcebergConverter converter = new IcebergConverter();
    converter.configure(Map.of("nested.as.variant", "true"), false);

    Schema nestedSchema = SchemaBuilder.struct().field("k", Schema.STRING_SCHEMA).build();
    Schema rootSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("payload", nestedSchema)
            .build();
    Struct root =
        new Struct(rootSchema)
            .put("id", 1)
            .put("payload", new Struct(nestedSchema).put("k", "hello"));

    byte[] bytes = converter.fromConnectData("topic", rootSchema, root);
    DeserializedIcebergRecord iceberg = converter.deserializeIcebergRecord(bytes);
    Variant variant = assertInstanceOf(Variant.class, iceberg.record().getField("payload"));
    assertEquals("hello", variant.value().asObject().get("k").asPrimitive().get());
  }

  @Test
  void toConnectDataThrowsUnsupportedOperationException() {
    IcebergConverter converter = new IcebergConverter();
    converter.configure(Map.of(), false);
    assertThrows(
        UnsupportedOperationException.class, () -> converter.toConnectData("topic", new byte[0]));
    assertThrows(
        UnsupportedOperationException.class,
        () -> converter.toConnectData("topic", null, new byte[0]));
  }

  @Test
  void handlesTombstonesAndNulls() {
    IcebergConverter converter = new IcebergConverter();
    converter.configure(Map.of(), false);

    Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    assertNull(converter.fromConnectData("topic", null, null));
    assertNull(converter.fromConnectData("topic", schema, null));
    assertThrows(
        DataException.class,
        () -> converter.fromConnectData("topic", null, new Struct(schema).put("id", 1)));
    assertNull(converter.deserializeIcebergRecord(null));
    assertThrows(NullPointerException.class, () -> new IcebergConverter().configure(null, false));
  }

  @Test
  void valueConverterRejectsNonStructAndKeyConverterSupportsStructAndPrimitives() {
    IcebergConverter valueConverter = new IcebergConverter();
    valueConverter.configure(Map.of(), false);
    assertThrows(
        DataException.class,
        () -> valueConverter.fromConnectData("topic", Schema.INT64_SCHEMA, 42L));

    Schema structSchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    assertThrows(
        DataException.class,
        () -> valueConverter.fromConnectData("topic", structSchema, "not-a-struct"));

    IcebergConverter keyConverter = new IcebergConverter();
    keyConverter.configure(Map.of(), true);

    byte[] structKeyBytes =
        keyConverter.fromConnectData("topic", structSchema, new Struct(structSchema).put("id", 7));
    assertEquals(7, keyConverter.deserializeIcebergRecord(structKeyBytes).record().getField("id"));

    byte[] primitiveKeyBytes = keyConverter.fromConnectData("topic", Schema.INT64_SCHEMA, 999L);
    DeserializedIcebergRecord primDecoded =
        keyConverter.deserializeIcebergRecord(primitiveKeyBytes);
    assertEquals(999L, primDecoded.record().getField(IcebergConverter.PRIMITIVE_KEY_FIELD_NAME));
  }

  @Test
  void unconfiguredConverterThrowsDataException() {
    IcebergConverter converter = new IcebergConverter();
    assertThrows(DataException.class, () -> converter.fromConnectData("topic", null, null));
    assertThrows(DataException.class, () -> converter.deserializeIcebergRecord(null));
  }

  @Test
  void fromConnectDataWrapsConversionErrorsInDataException() {
    IcebergConverter converter = new IcebergConverter();
    converter.configure(Map.of(), false);

    Schema emptyStructSchema = SchemaBuilder.struct().name("Empty").build();
    Struct emptyStruct = new Struct(emptyStructSchema);
    assertThrows(
        DataException.class,
        () -> converter.fromConnectData("topic", emptyStructSchema, emptyStruct));
  }

  @Test
  void schemaCacheSizeZeroWorksForValuesAndPrimitiveKeys() {
    IcebergConverter valueConverter = new IcebergConverter();
    valueConverter.configure(Map.of("schema.cache.size", "0"), false);

    Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    byte[] bytes =
        valueConverter.fromConnectData("topic", schema, new Struct(schema).put("id", 11));
    assertEquals(11, valueConverter.deserializeIcebergRecord(bytes).record().getField("id"));

    IcebergConverter keyConverter = new IcebergConverter();
    keyConverter.configure(Map.of("schema.cache.size", "0"), true);
    byte[] keyBytes = keyConverter.fromConnectData("topic", Schema.INT64_SCHEMA, 55L);
    assertEquals(
        55L,
        keyConverter
            .deserializeIcebergRecord(keyBytes)
            .record()
            .getField(IcebergConverter.PRIMITIVE_KEY_FIELD_NAME));
  }

  @Test
  void metadataAndServiceLoaderDiscovery() {
    IcebergConverter converter = new IcebergConverter();
    assertNotNull(converter.config());
    assertArrayEquals(
        ConnectIcebergConverterConfig.allFields().asArray(), converter.getConfigFields().asArray());
    assertNotNull(converter.version());
    assertFalse(converter.version().isBlank());
    assertFalse(converter.version().contains("${"));

    boolean discovered = false;
    for (Converter found : ServiceLoader.load(Converter.class)) {
      if (found instanceof IcebergConverter) {
        discovered = true;
        break;
      }
    }
    assertTrue(discovered, "IcebergConverter must be discoverable via ServiceLoader<Converter>");
  }

  private static byte[] toArray(ByteBuffer buffer) {
    ByteBuffer dup = buffer.duplicate();
    byte[] bytes = new byte[dup.remaining()];
    dup.get(bytes);
    return bytes;
  }
}
