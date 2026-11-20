/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import io.debezium.config.Field;
import io.debezium.converters.iceberg.serde.IcebergRecordSerializers;
import io.debezium.converters.iceberg.spi.DeserializedIcebergRecord;
import io.debezium.converters.iceberg.spi.IcebergRecordSerializer;
import io.debezium.metadata.ConfigDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

/**
 * Layer 2 pluggable Kafka Connect / Debezium {@link Converter} that emits Apache Iceberg {@link
 * org.apache.iceberg.data.GenericRecord} payloads serialized via a configurable {@link
 * IcebergRecordSerializer}.
 *
 * <p>Strictly Iceberg-oriented: use {@link #deserializeIcebergRecord(byte[])} (or the matching
 * {@link IcebergRecordSerializer}) to decode payloads directly to Iceberg {@code Schema} and {@code
 * GenericRecord}. Reverse conversion to Kafka Connect {@link SchemaAndValue} via {@link
 * #toConnectData} is intentionally not supported.
 */
public class IcebergConverter implements Converter, Versioned, ConfigDescriptor {

  /**
   * Field name used when wrapping a primitive Kafka Connect key ({@code isKey=true}) in a Struct.
   */
  public static final String PRIMITIVE_KEY_FIELD_NAME = "key";

  private boolean isKey;
  private IcebergStructConverter structConverter;
  private IcebergRecordSerializer serializer;

  @Override
  public ConfigDef config() {
    return ConnectIcebergConverterConfig.configDef();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Objects.requireNonNull(configs, "configs must not be null");
    Map<String, Object> conf = new HashMap<>(configs);
    // Debezium's ConverterBuilder does not set "converter.type" on key/value converters; inject it
    // unconditionally before constructing ConverterConfig (mirroring CloudEventsConverter).
    conf.put(
        ConverterConfig.TYPE_CONFIG,
        isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());

    ConnectIcebergConverterConfig config = new ConnectIcebergConverterConfig(conf);
    this.isKey = isKey;
    this.structConverter = new IcebergStructConverter(config);
    this.serializer = IcebergRecordSerializers.create(config.serializerType(), conf);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    ensureConfigured();
    if (value == null) {
      return null;
    }
    if (schema == null) {
      throw new DataException("Schema must not be null for non-null value");
    }

    Schema effectiveSchema;
    Struct effectiveStruct;
    if (schema.type() == Schema.Type.STRUCT) {
      if (!(value instanceof Struct struct)) {
        throw new DataException(
            "Mismatching value type for STRUCT schema: expected Struct, got "
                + value.getClass().getName());
      }
      effectiveSchema = schema;
      effectiveStruct = struct;
    } else if (isKey) {
      effectiveSchema = SchemaBuilder.struct().field(PRIMITIVE_KEY_FIELD_NAME, schema).build();
      effectiveStruct = new Struct(effectiveSchema).put(PRIMITIVE_KEY_FIELD_NAME, value);
    } else {
      throw new DataException("Mismatching schema: expected STRUCT, got " + schema.type());
    }

    ConvertedIcebergRecord converted =
        structConverter.convert(topic, effectiveSchema, effectiveStruct, null);
    return serializer.serialize(converted.schema(), converted.record());
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    throw new UnsupportedOperationException(
        "IcebergConverter only produces Apache Iceberg GenericRecord payloads; use"
            + " deserializeIcebergRecord(...) or IcebergRecordSerializer.deserialize(...) instead.");
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    return toConnectData(topic, value);
  }

  /**
   * Decodes a payload produced by {@link #fromConnectData} into Iceberg {@code Schema} + {@code
   * GenericRecord}.
   */
  public DeserializedIcebergRecord deserializeIcebergRecord(byte[] value) {
    ensureConfigured();
    return value == null ? null : serializer.deserialize(value);
  }

  @Override
  public String version() {
    return Module.version();
  }

  @Override
  public Field.Set getConfigFields() {
    return ConnectIcebergConverterConfig.allFields();
  }

  private void ensureConfigured() {
    if (structConverter == null) {
      throw new DataException(
          "IcebergConverter has not been configured; call configure(configs, isKey) first.");
    }
  }
}
