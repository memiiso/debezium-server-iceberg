/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.serde;

import io.debezium.converters.iceberg.IcebergConverterConfig;
import io.debezium.converters.iceberg.internal.LruCache;
import io.debezium.converters.iceberg.spi.DeserializedIcebergRecord;
import io.debezium.converters.iceberg.spi.IcebergRecordSerializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.message.SchemaStore;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.IcebergDecoder;
import org.apache.iceberg.data.avro.IcebergEncoder;
import org.apache.kafka.connect.errors.DataException;

/**
 * Compact Avro single-object serializer built on Iceberg's {@link IcebergEncoder} and {@link
 * IcebergDecoder} (10-byte header: {@code 0xC3 0x01} + 8-byte little-endian CRC-64-AVRO
 * fingerprint).
 */
public class IcebergEncoderRecordSerializer implements IcebergRecordSerializer {

  public static final String TYPE = "avro-single-object";
  public static final byte HEADER_0 = (byte) 0xC3;
  public static final byte HEADER_1 = (byte) 0x01;
  private static final int HEADER_LENGTH = 10;

  private final SchemaStore defaultSchemaStore;
  private LruCache<AvroSchemaKey, IcebergEncoder<GenericRecord>> encoderCache =
      new LruCache<>(IcebergConverterConfig.DEFAULT_SCHEMA_CACHE_SIZE);
  private LruCache<Long, DecoderEntry> fingerprintCache =
      new LruCache<>(IcebergConverterConfig.DEFAULT_SCHEMA_CACHE_SIZE);

  public IcebergEncoderRecordSerializer() {
    this(null);
  }

  public IcebergEncoderRecordSerializer(SchemaStore schemaStore) {
    this.defaultSchemaStore = schemaStore;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    int cacheSize = IcebergRecordSerializers.schemaCacheSize(configs);
    this.encoderCache = cacheSize == 0 ? null : new LruCache<>(cacheSize);
    this.fingerprintCache = cacheSize == 0 ? null : new LruCache<>(cacheSize);
  }

  /**
   * Registers an Iceberg {@link Schema} in this serializer's fingerprint cache so payloads encoded
   * with this schema can be decoded without an external {@link SchemaStore}.
   */
  public long registerSchema(Schema schema) {
    Objects.requireNonNull(schema, "schema must not be null");
    if (fingerprintCache == null) {
      throw new IllegalStateException("Cannot register schema when schema.cache.size=0");
    }
    long fp = fingerprint(schema);
    fingerprintCache.put(fp, new DecoderEntry(schema, new IcebergDecoder<>(schema)));
    return fp;
  }

  /** Returns the 64-bit CRC-64-AVRO fingerprint for the given Iceberg {@link Schema}. */
  public static long fingerprint(Schema schema) {
    Objects.requireNonNull(schema, "schema must not be null");
    return SchemaNormalization.parsingFingerprint64(
        AvroSchemaUtil.convert(schema, AvroSchemaKey.AVRO_ROOT_NAME));
  }

  @Override
  public byte[] serialize(Schema schema, GenericRecord record) {
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(record, "record must not be null");

    IcebergEncoder<GenericRecord> encoder =
        encoderCache == null
            ? new IcebergEncoder<>(schema, true)
            : encoderCache.getOrCompute(
                AvroSchemaKey.of(schema),
                k -> {
                  if (fingerprintCache != null) {
                    fingerprintCache.putIfAbsent(
                        fingerprint(schema),
                        new DecoderEntry(schema, new IcebergDecoder<>(schema)));
                  }
                  return new IcebergEncoder<>(schema, true);
                });
    try {
      ByteBuffer encoded = encoder.encode(record);
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      return bytes;
    } catch (IOException | RuntimeException e) {
      throw new DataException("Failed to serialize Iceberg GenericRecord using " + TYPE, e);
    }
  }

  @Override
  public DeserializedIcebergRecord deserialize(byte[] bytes) {
    return deserialize(bytes, defaultSchemaStore);
  }

  /**
   * Deserializes a single-object encoded payload, resolving unknown schema fingerprints against the
   * provided Avro {@link SchemaStore}.
   */
  public DeserializedIcebergRecord deserialize(byte[] bytes, SchemaStore schemaStore) {
    Objects.requireNonNull(bytes, "bytes must not be null");
    if (bytes.length < HEADER_LENGTH) {
      throw new DataException(
          "Truncated "
              + TYPE
              + " payload: expected at least "
              + HEADER_LENGTH
              + " bytes, got "
              + bytes.length);
    }
    if (bytes[0] != HEADER_0 || bytes[1] != HEADER_1) {
      throw new DataException(
          String.format(
              "Invalid %s header bytes: expected 0x%02X 0x%02X, got 0x%02X 0x%02X",
              TYPE, HEADER_0, HEADER_1, bytes[0], bytes[1]));
    }

    long fp = ByteBuffer.wrap(bytes, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    DecoderEntry decoderEntry = resolveDecoder(fp, schemaStore);
    try {
      GenericRecord record = decoderEntry.decoder.decode(new ByteArrayInputStream(bytes), null);
      return new DeserializedIcebergRecord(decoderEntry.schema, record);
    } catch (IOException | RuntimeException e) {
      throw new DataException("Failed to deserialize Iceberg GenericRecord using " + TYPE, e);
    }
  }

  private DecoderEntry resolveDecoder(long fp, SchemaStore schemaStore) {
    DecoderEntry cached = fingerprintCache == null ? null : fingerprintCache.get(fp);
    if (cached != null) {
      return cached;
    }
    if (schemaStore != null) {
      org.apache.avro.Schema avroSchema = schemaStore.findByFingerprint(fp);
      if (avroSchema != null) {
        Schema readSchema = AvroSchemaUtil.toIceberg(avroSchema);
        DecoderEntry resolved =
            new DecoderEntry(readSchema, new IcebergDecoder<>(readSchema, schemaStore));
        if (fingerprintCache != null) {
          fingerprintCache.putIfAbsent(fp, resolved);
        }
        return resolved;
      }
    }
    throw new DataException(
        "Cannot resolve Iceberg schema for "
            + TYPE
            + " fingerprint "
            + fp
            + ": schema is not in local cache and no matching entry was found in SchemaStore.");
  }

  private record DecoderEntry(Schema schema, IcebergDecoder<GenericRecord> decoder) {}
}
