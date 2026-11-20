/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.converters.iceberg.serde.AvroEnvelopeRecordSerializer;
import io.debezium.converters.iceberg.serde.IcebergEncoderRecordSerializer;
import io.debezium.converters.iceberg.spi.DeserializedIcebergRecord;
import io.debezium.embedded.ClientProvided;
import io.debezium.embedded.Connect;
import io.debezium.embedded.ConverterBuilder;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import org.apache.avro.message.SchemaStore;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies Layer 1 ({@link IcebergStructConverter} with {@link Connect}) and Layer 2 ({@link
 * IcebergConverter} with {@link ClientProvided}) through Debezium Embedded's actual {@link
 * ConverterBuilder} and {@code KafkaConnectConverterAdapter}.
 */
class IcebergConverterEmbeddedTest {

  private static final Schema KEY_SCHEMA =
      SchemaBuilder.struct().name("test.users.Key").field("id", Schema.INT64_SCHEMA).build();

  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .name("test.users.Value")
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("secret", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static SourceRecord sampleRecord(long id, String name) {
    Struct key = new Struct(KEY_SCHEMA).put("id", id);
    Struct value =
        name == null
            ? null
            : new Struct(VALUE_SCHEMA).put("id", id).put("name", name).put("secret", "hidden");
    return new SourceRecord(
        Collections.singletonMap("server", "pg1"),
        Collections.singletonMap("lsn", 1L),
        "pg1.public.users",
        KEY_SCHEMA,
        key,
        VALUE_SCHEMA,
        value);
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro-envelope", "avro-single-object"})
  void converterBuilderWithClientProvidedKeyAndValueWorksAcrossAvroCodecs(String serializerType) {
    Properties props = new Properties();
    props.setProperty("key.converter.class", IcebergConverter.class.getName());
    props.setProperty("value.converter.class", IcebergConverter.class.getName());
    props.setProperty("converter.serializer.type", serializerType);
    props.setProperty("value.converter.excluded.columns", "secret");

    ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>> builder =
        new ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>>()
            .using(
                KeyValueHeaderChangeEventFormat.of(
                    ClientProvided.class, ClientProvided.class, Json.class))
            .using(props);

    Function<SourceRecord, EmbeddedEngineChangeEvent<Object, Object, Object>> toFormat =
        builder.toFormat(builder.headerConverter());

    EmbeddedEngineChangeEvent<Object, Object, Object> event =
        toFormat.apply(sampleRecord(10L, "alice"));

    byte[] keyBytes = assertInstanceOf(byte[].class, event.key());
    byte[] valueBytes = assertInstanceOf(byte[].class, event.value());

    DeserializedIcebergRecord decodedKey =
        decodeWithFreshReader(serializerType, keyBytes, KEY_SCHEMA, false);
    DeserializedIcebergRecord decodedValue =
        decodeWithFreshReader(serializerType, valueBytes, VALUE_SCHEMA, true);

    assertEquals(10L, decodedKey.record().getField("id"));
    assertEquals(10L, decodedValue.record().getField("id"));
    assertEquals("alice", decodedValue.record().getField("name"));
    assertNull(
        decodedValue.schema().findField("secret"),
        "value.converter.excluded.columns must propagate through ConverterBuilder");

    EmbeddedEngineChangeEvent<Object, Object, Object> tombstone =
        toFormat.apply(sampleRecord(10L, null));
    assertNotNull(tombstone.key());
    assertNull(tombstone.value());
  }

  @Test
  void converterBuilderWithSharedConverterClassWorksWhenNoHeaderFormatIsConfigured() {
    Properties props = new Properties();
    props.setProperty("converter.class", IcebergConverter.class.getName());

    ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>> builder =
        new ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>>()
            .using(
                KeyValueHeaderChangeEventFormat.of(
                    ClientProvided.class, ClientProvided.class, null))
            .using(props);

    assertNull(builder.headerConverter());
    Function<SourceRecord, EmbeddedEngineChangeEvent<Object, Object, Object>> toFormat =
        builder.toFormat(null);

    EmbeddedEngineChangeEvent<Object, Object, Object> event =
        toFormat.apply(sampleRecord(15L, "dave"));
    DeserializedIcebergRecord decoded =
        new AvroEnvelopeRecordSerializer().deserialize((byte[]) event.value());
    assertEquals(15L, decoded.record().getField("id"));
    assertEquals("dave", decoded.record().getField("name"));
  }

  @Test
  void converterBuilderWithJsonKeyAndClientProvidedValueWorks() {
    Properties props = new Properties();
    props.setProperty("key.converter.schemas.enable", "false");
    props.setProperty("value.converter.class", IcebergConverter.class.getName());
    props.setProperty("value.converter.serializer.type", "avro-envelope");
    props.setProperty("value.converter.excluded.columns", "secret");

    ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>> builder =
        new ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>>()
            .using(KeyValueHeaderChangeEventFormat.of(Json.class, ClientProvided.class, Json.class))
            .using(props);

    Function<SourceRecord, EmbeddedEngineChangeEvent<Object, Object, Object>> toFormat =
        builder.toFormat(builder.headerConverter());

    EmbeddedEngineChangeEvent<Object, Object, Object> event =
        toFormat.apply(sampleRecord(7L, "bob"));

    String jsonKey = assertInstanceOf(String.class, event.key());
    assertTrue(jsonKey.contains("\"id\":7"));

    byte[] valueBytes = assertInstanceOf(byte[].class, event.value());
    DeserializedIcebergRecord decoded = new AvroEnvelopeRecordSerializer().deserialize(valueBytes);
    assertEquals(7L, decoded.record().getField("id"));
    assertEquals("bob", decoded.record().getField("name"));
    assertNull(decoded.schema().findField("secret"));
  }

  @Test
  void converterBuilderRejectsConnectKeyMixedWithClientProvidedValue() {
    Properties props = new Properties();
    props.setProperty("value.converter.class", IcebergConverter.class.getName());

    ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>> builder =
        new ConverterBuilder<EmbeddedEngineChangeEvent<Object, Object, Object>>()
            .using(
                KeyValueHeaderChangeEventFormat.of(Connect.class, ClientProvided.class, Json.class))
            .using(props);

    DebeziumException ex =
        assertThrows(DebeziumException.class, () -> builder.toFormat(builder.headerConverter()));
    assertTrue(ex.getMessage().contains("Connect"));
  }

  @Test
  void layer1ZeroCopyWithDebeziumConnectFormatPreservesIdentifierFieldsAndSortOrder() {
    ConverterBuilder<EmbeddedEngineChangeEvent<Void, SourceRecord, Object>> builder =
        new ConverterBuilder<EmbeddedEngineChangeEvent<Void, SourceRecord, Object>>()
            .using(KeyValueHeaderChangeEventFormat.of(Connect.class, Connect.class, Connect.class))
            .using(new Properties());

    Function<SourceRecord, EmbeddedEngineChangeEvent<Void, SourceRecord, Object>> toFormat =
        builder.toFormat(builder.headerConverter());

    EmbeddedEngineChangeEvent<Void, SourceRecord, Object> event =
        toFormat.apply(sampleRecord(42L, "carol"));
    SourceRecord sourceRecord = event.value();

    IcebergStructConverter structConverter =
        new IcebergStructConverter(
            ConnectIcebergConverterConfig.fromMap(Map.of("excluded.columns", "secret")));
    ConvertedIcebergRecord converted = structConverter.convert(sourceRecord);

    assertEquals("pg1.public.users", converted.destination());
    assertEquals(Set.of(1), converted.schema().identifierFieldIds());
    assertTrue(converted.sortOrder().isSorted());
    assertNull(converted.schema().findField("secret"));

    GenericRecord record = converted.record();
    assertNotNull(record);
    assertEquals(42L, record.getField("id"));
    assertEquals("carol", record.getField("name"));
  }

  private static DeserializedIcebergRecord decodeWithFreshReader(
      String serializerType, byte[] bytes, Schema connectSchema, boolean excludeSecret) {
    return switch (serializerType) {
      case AvroEnvelopeRecordSerializer.TYPE ->
          new AvroEnvelopeRecordSerializer().deserialize(bytes);
      case IcebergEncoderRecordSerializer.TYPE -> {
        ConnectIcebergConverterConfig cfg =
            excludeSecret
                ? ConnectIcebergConverterConfig.fromMap(Map.of("excluded.columns", "secret"))
                : ConnectIcebergConverterConfig.defaults();
        org.apache.iceberg.Schema icebergSchema =
            new StructSchemaConverter(connectSchema, null, cfg).icebergSchema(false);
        SchemaStore.Cache store = new SchemaStore.Cache();
        store.addSchema(AvroSchemaUtil.convert(icebergSchema, "table"));
        yield new IcebergEncoderRecordSerializer().deserialize(bytes, store);
      }
      default -> throw new IllegalArgumentException("Unexpected serializerType: " + serializerType);
    };
  }
}
