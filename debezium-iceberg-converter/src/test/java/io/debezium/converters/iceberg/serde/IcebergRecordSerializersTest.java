/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.serde;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.converters.iceberg.StructVariantObject;
import io.debezium.converters.iceberg.spi.DeserializedIcebergRecord;
import io.debezium.converters.iceberg.spi.IcebergRecordSerializer;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.message.SchemaStore;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergRecordSerializersTest {

  private static final byte[] SAMPLE_ISO_WKB_POINT = new byte[] {
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

  private static Schema buildRichSchema() {
    Types.StructType nestedStruct = Types.StructType.of(
        Types.NestedField.optional(101, "street", Types.StringType.get()),
        Types.NestedField.optional(102, "zip", Types.IntegerType.get()));

    return new Schema(
        List.of(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "int_col", Types.IntegerType.get()),
            Types.NestedField.optional(3, "float_col", Types.FloatType.get()),
            Types.NestedField.optional(4, "double_col", Types.DoubleType.get()),
            Types.NestedField.optional(5, "bool_col", Types.BooleanType.get()),
            Types.NestedField.optional(6, "str_col", Types.StringType.get()),
            Types.NestedField.optional(7, "dec_col", Types.DecimalType.of(12, 3)),
            Types.NestedField.optional(8, "uuid_col", Types.UUIDType.get()),
            Types.NestedField.optional(9, "bin_col", Types.BinaryType.get()),
            Types.NestedField.optional(10, "date_col", Types.DateType.get()),
            Types.NestedField.optional(11, "time_col", Types.TimeType.get()),
            Types.NestedField.optional(12, "ts_col", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(13, "tstz_col", Types.TimestampType.withZone()),
            Types.NestedField.optional(14, "nested", nestedStruct),
            Types.NestedField.optional(
                15, "tags", Types.ListType.ofOptional(103, Types.StringType.get())),
            Types.NestedField.optional(
                16,
                "attrs",
                Types.MapType.ofOptional(
                    104, 105, Types.StringType.get(), Types.IntegerType.get())),
            Types.NestedField.optional(
                17,
                "int_map",
                Types.MapType.ofOptional(
                    106, 107, Types.IntegerType.get(), Types.StringType.get())),
            Types.NestedField.optional(18, "nullable_col", Types.StringType.get()),
            Types.NestedField.optional(19, "geom_col", Types.GeometryType.crs84()),
            Types.NestedField.optional(20, "geog_col", Types.GeographyType.crs84()),
            Types.NestedField.optional(21, "variant_col", Types.VariantType.get())),
        Set.of(1));
  }

  private static GenericRecord buildRichRecord(Schema schema) {
    GenericRecord nested = GenericRecord.create(schema.findType("nested").asStructType());
    nested.setField("street", "Main St");
    nested.setField("zip", 10115);

    org.apache.kafka.connect.data.Schema connectVariantSchema = SchemaBuilder.struct()
        .field("k", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("n", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
        .build();
    Struct connectVariantStruct = new Struct(connectVariantSchema).put("k", "v").put("n", 7);
    StructVariantObject variantObj = new StructVariantObject(connectVariantStruct);
    Variant variant = Variant.of(variantObj.metadata(), variantObj);

    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", 42L);
    record.setField("int_col", 7);
    record.setField("float_col", 1.5f);
    record.setField("double_col", 2.75d);
    record.setField("bool_col", true);
    record.setField("str_col", "hello");
    record.setField("dec_col", new BigDecimal("12345.678"));
    record.setField("uuid_col", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
    record.setField("bin_col", ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));
    record.setField("date_col", LocalDate.of(2026, 11, 19));
    record.setField("time_col", LocalTime.of(14, 35, 12, 123_456_000));
    record.setField("ts_col", LocalDateTime.of(2026, 11, 19, 14, 35, 12, 123_456_000));
    record.setField(
        "tstz_col", OffsetDateTime.of(2026, 11, 19, 14, 35, 12, 123_456_000, ZoneOffset.UTC));
    record.setField("nested", nested);
    record.setField("tags", List.of("a", "b"));
    record.setField("attrs", Map.of("x", 1, "y", 2));
    record.setField("int_map", Map.of(10, "ten", 20, "twenty"));
    record.setField("nullable_col", null);
    record.setField("geom_col", ByteBuffer.wrap(SAMPLE_ISO_WKB_POINT));
    record.setField("geog_col", ByteBuffer.wrap(SAMPLE_ISO_WKB_POINT));
    record.setField("variant_col", variant);
    return record;
  }

  @ParameterizedTest
  @ValueSource(strings = { "avro-envelope", "avro-single-object" })
  void roundTripAllIcebergTypes(String serializerType) {
    IcebergRecordSerializer serializer = IcebergRecordSerializers.create(serializerType, Collections.emptyMap());
    assertEquals(serializerType, serializer.type());

    Schema schema = buildRichSchema();
    GenericRecord record = buildRichRecord(schema);

    byte[] encoded = serializer.serialize(schema, record);
    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    DeserializedIcebergRecord decoded = serializer.deserialize(encoded);
    assertEquals(schema.asStruct(), decoded.schema().asStruct());
    assertEquals(Set.of(1), decoded.schema().identifierFieldIds());

    GenericRecord out = decoded.record();
    assertEquals(42L, out.getField("id"));
    assertEquals(7, out.getField("int_col"));
    assertEquals(1.5f, out.getField("float_col"));
    assertEquals(2.75d, out.getField("double_col"));
    assertEquals(true, out.getField("bool_col"));
    assertEquals("hello", out.getField("str_col"));
    assertEquals(new BigDecimal("12345.678"), out.getField("dec_col"));
    assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), out.getField("uuid_col"));
    assertArrayEquals(new byte[] { 1, 2, 3, 4 }, toArray((ByteBuffer) out.getField("bin_col")));
    assertEquals(LocalDate.of(2026, 11, 19), out.getField("date_col"));
    assertEquals(LocalTime.of(14, 35, 12, 123_456_000), out.getField("time_col"));
    assertEquals(LocalDateTime.of(2026, 11, 19, 14, 35, 12, 123_456_000), out.getField("ts_col"));
    assertEquals(
        OffsetDateTime.of(2026, 11, 19, 14, 35, 12, 123_456_000, ZoneOffset.UTC),
        out.getField("tstz_col"));

    GenericRecord outNested = (GenericRecord) out.getField("nested");
    assertEquals("Main St", outNested.getField("street"));
    assertEquals(10115, outNested.getField("zip"));
    assertEquals(List.of("a", "b"), out.getField("tags"));
    assertEquals(Map.of("x", 1, "y", 2), out.getField("attrs"));
    assertEquals(Map.of(10, "ten", 20, "twenty"), out.getField("int_map"));
    assertNull(out.getField("nullable_col"));
    assertArrayEquals(SAMPLE_ISO_WKB_POINT, toArray((ByteBuffer) out.getField("geom_col")));
    assertArrayEquals(SAMPLE_ISO_WKB_POINT, toArray((ByteBuffer) out.getField("geog_col")));

    Variant outVariant = assertInstanceOf(Variant.class, out.getField("variant_col"));
    assertEquals("v", outVariant.value().asObject().get("k").asPrimitive().get());
    assertEquals(7, outVariant.value().asObject().get("n").asPrimitive().get());
  }

  @Test
  void avroEnvelopeIsSelfDescribingAcrossSeparateInstances() {
    Schema schema = new Schema(
        List.of(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())),
        Set.of(1));
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", 99);
    record.setField("name", "alice");

    byte[] bytes = new AvroEnvelopeRecordSerializer().serialize(schema, record);
    assertEquals(AvroEnvelopeRecordSerializer.MAGIC_0, bytes[0]);
    assertEquals(AvroEnvelopeRecordSerializer.MAGIC_1, bytes[1]);
    assertEquals(AvroEnvelopeRecordSerializer.FORMAT_VERSION, bytes[2]);

    DeserializedIcebergRecord decoded = new AvroEnvelopeRecordSerializer().deserialize(bytes);
    assertEquals(Set.of(1), decoded.schema().identifierFieldIds());
    assertEquals(99, decoded.record().getField("id"));
    assertEquals("alice", decoded.record().getField("name"));
  }

  @Test
  void avroEnvelopeRejectsCorruptOrTruncatedPayloads() {
    AvroEnvelopeRecordSerializer serializer = new AvroEnvelopeRecordSerializer();
    assertThrows(DataException.class, () -> serializer.deserialize(new byte[] { 0x49, 0x43 }));
    assertThrows(
        DataException.class,
        () -> serializer.deserialize(new byte[] { 0x00, 0x00, 0x01, 0, 0, 0, 1, 'x' }));
    assertThrows(
        DataException.class,
        () -> serializer.deserialize(new byte[] { 0x49, 0x43, 0x09, 0, 0, 0, 1, 'x' }));
    assertThrows(
        DataException.class,
        () -> serializer.deserialize(new byte[] { 0x49, 0x43, 0x01, 0, 0, 0, 50, '{', '}' }));
    // Crafted schemaLen near Integer.MAX_VALUE must not overflow FIXED_HEADER_BYTES
    // + schemaLen
    assertThrows(
        DataException.class,
        () -> serializer.deserialize(
            new byte[] {
                0x49, 0x43, 0x01, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, '{', '}'
            }));
    assertThrows(
        DataException.class,
        () -> serializer.deserialize(
            new byte[] {
                0x49, 0x43, 0x01, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFA, '{', '}'
            }));
  }

  @Test
  void singleObjectResolvesExternalSchemaStoreAndFailsWhenMissing() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", 5);
    record.setField("name", "bob");

    byte[] bytes = new AvroSingleObjectRecordSerializer().serialize(schema, record);
    assertEquals(AvroSingleObjectRecordSerializer.HEADER_0, bytes[0]);
    assertEquals(AvroSingleObjectRecordSerializer.HEADER_1, bytes[1]);

    AvroSingleObjectRecordSerializer reader = new AvroSingleObjectRecordSerializer();
    assertThrows(DataException.class, () -> reader.deserialize(bytes, null));

    SchemaStore.Cache store = new SchemaStore.Cache();
    store.addSchema(AvroSchemaUtil.convert(schema, "table"));
    DeserializedIcebergRecord decoded = reader.deserialize(bytes, store);
    assertEquals(5, decoded.record().getField("id"));
    assertEquals("bob", decoded.record().getField("name"));

    AvroSingleObjectRecordSerializer registeredReader = new AvroSingleObjectRecordSerializer();
    registeredReader.registerSchema(schema);
    DeserializedIcebergRecord decodedRegistered = registeredReader.deserialize(bytes);
    assertEquals(5, decodedRegistered.record().getField("id"));
    assertEquals("bob", decodedRegistered.record().getField("name"));

    // Configured via avro.schema.store.class
    TestSchemaStore.SHARED.addSchema(AvroSchemaUtil.convert(schema, "table"));
    IcebergRecordSerializer configuredReader = IcebergRecordSerializers.create(
        AvroSingleObjectRecordSerializer.TYPE,
        Map.of("avro.schema.store.class", TestSchemaStore.class.getName()));
    DeserializedIcebergRecord decodedFromConfig = configuredReader.deserialize(bytes);
    assertEquals(5, decodedFromConfig.record().getField("id"));
    assertEquals("bob", decodedFromConfig.record().getField("name"));

    assertThrows(
        ConfigException.class,
        () -> IcebergRecordSerializers.create(
            AvroSingleObjectRecordSerializer.TYPE,
            Map.of("avro.schema.store.class", "java.lang.String")));
  }

  @Test
  void singleObjectRejectsInvalidHeaderOrTruncatedBytes() {
    AvroSingleObjectRecordSerializer serializer = new AvroSingleObjectRecordSerializer();
    assertThrows(DataException.class, () -> serializer.deserialize(new byte[] { (byte) 0xC3, 0x01 }));
    assertThrows(
        DataException.class,
        () -> serializer.deserialize(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));
  }

  @Test
  void factoryResolvesBuiltInsAndCustomFqcn() {
    assertThrows(ConfigException.class, () -> IcebergRecordSerializers.create(null, Map.of()));
    assertThrows(ConfigException.class, () -> IcebergRecordSerializers.create("", Map.of()));
    assertThrows(
        NullPointerException.class, () -> IcebergRecordSerializers.create("avro-envelope", null));
    assertInstanceOf(
        AvroEnvelopeRecordSerializer.class,
        IcebergRecordSerializers.create("avro-envelope", Map.of()));
    assertInstanceOf(
        AvroSingleObjectRecordSerializer.class,
        IcebergRecordSerializers.create("AVRO-SINGLE-OBJECT", Map.of()));
    assertInstanceOf(
        TestCustomSerializer.class,
        IcebergRecordSerializers.create(TestCustomSerializer.class.getName(), Map.of()));

    assertThrows(
        ConfigException.class, () -> IcebergRecordSerializers.create("java.lang.String", Map.of()));
    assertThrows(
        ConfigException.class,
        () -> IcebergRecordSerializers.create("no.such.SerializerClass", Map.of()));
  }

  private static byte[] toArray(ByteBuffer buffer) {
    ByteBuffer dup = buffer.duplicate();
    byte[] bytes = new byte[dup.remaining()];
    dup.get(bytes);
    return bytes;
  }

  public static class TestSchemaStore implements SchemaStore {
    static final SchemaStore.Cache SHARED = new SchemaStore.Cache();

    @Override
    public org.apache.avro.Schema findByFingerprint(long fingerprint) {
      return SHARED.findByFingerprint(fingerprint);
    }
  }

  public static class TestCustomSerializer implements IcebergRecordSerializer {
    @Override
    public String type() {
      return "test-custom";
    }

    @Override
    public byte[] serialize(Schema schema, GenericRecord record) {
      return new byte[0];
    }

    @Override
    public DeserializedIcebergRecord deserialize(byte[] bytes) {
      throw new UnsupportedOperationException();
    }
  }
}
