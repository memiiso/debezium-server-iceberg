/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.serde;

import io.debezium.converters.iceberg.IcebergStructConverter;
import io.debezium.converters.iceberg.internal.LruCache;
import io.debezium.converters.iceberg.spi.DeserializedIcebergRecord;
import io.debezium.converters.iceberg.spi.IcebergRecordSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.avro.RawDecoder;
import org.apache.kafka.connect.errors.DataException;

/**
 * Self-describing Iceberg Avro envelope serializer (default for
 * {@code IcebergConverter}).
 *
 * <p>
 * Wire format:
 * {@code [2B 0x49 0x43][1B 0x01][4B schemaJsonLen][UTF-8 SchemaParser JSON][Avro
 * DataWriter datum]}. Requires zero external Schema Registry while preserving
 * Iceberg field IDs,
 * identifier fields, logical types, variants, and v3 geospatial types.
 */
public class AvroEnvelopeRecordSerializer implements IcebergRecordSerializer {

  public static final String TYPE = "avro-envelope";
  public static final byte MAGIC_0 = 0x49; // 'I'
  public static final byte MAGIC_1 = 0x43; // 'C'
  public static final byte FORMAT_VERSION = 0x01;
  private static final int FIXED_HEADER_BYTES = 7;

  private LruCache<AvroSchemaKey, WriteEntry> writeCache =

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    int cacheSize = IcebergRecordSerializers.schemaCacheSize(configs);
    this.writeCache = cacheSize == 0 ? null : new LruCache<>(cacheSize);
    this.readCache = cacheSize == 0 ? null : new LruCache<>(cacheSize);
  }

  @Override
  public byte[] serialize(Schema schema, GenericRecord record) {
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(record, "record must not be null");

    WriteEntry entry =
        writeCache == null
            ? buildWriteEntry(schema)
            : writeCache.getOrCompute(AvroSchemaKey.of(schema), k -> buildWriteEntry(schema));
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(entry.headerBytes.length + 128);
      out.write(entry.headerBytes);
      BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
      entry.writer.write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException | RuntimeException e) {
      throw new DataException("Failed to serialize Iceberg GenericRecord using " + TYPE, e);
    }
  }

  @Override
  public DeserializedIcebergRecord deserialize(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes must not be null");
    if (bytes.length < FIXED_HEADER_BYTES) {
      throw new DataException(
          "Truncated "
              + TYPE
              + " payload: expected at least "
              + FIXED_HEADER_BYTES
              + " bytes, got "
              + bytes.length);
    }
    if (bytes[0] != MAGIC_0 || bytes[1] != MAGIC_1) {
      throw new DataException(
          String.format(
              "Invalid %s magic bytes: expected 0x%02X 0x%02X, got 0x%02X 0x%02X",
              TYPE, MAGIC_0, MAGIC_1, bytes[0], bytes[1]));
    }
    if (bytes[2] != FORMAT_VERSION) {
      throw new DataException(
          "Unsupported " + TYPE + " format version: " + Byte.toUnsignedInt(bytes[2]));
    }

    int schemaLen = ByteBuffer.wrap(bytes, 3, 4).getInt();
    if (schemaLen <= 0 || schemaLen > bytes.length - FIXED_HEADER_BYTES) {
      throw new DataException(
          "Invalid "
              + TYPE
              + " schema length "
              + schemaLen
              + " for payload of "
              + bytes.length
              + " bytes");
    }

    String schemaJson = new String(bytes, FIXED_HEADER_BYTES, schemaLen, StandardCharsets.UTF_8);
    ReadEntry entry =
        readCache == null
            ? buildReadEntry(schemaJson)
            : readCache.getOrCompute(schemaJson, AvroEnvelopeRecordSerializer::buildReadEntry);

    int payloadOffset = FIXED_HEADER_BYTES + schemaLen;
    try {
      GenericRecord record =
          entry.decoder.decode(
              new ByteArrayInputStream(bytes, payloadOffset, bytes.length - payloadOffset), null);
      return new DeserializedIcebergRecord(entry.schema, record);
    } catch (RuntimeException e) {
      throw new DataException("Failed to deserialize Iceberg GenericRecord using " + TYPE, e);
    }
  }

  private static WriteEntry buildWriteEntry(Schema schema) {
    byte[] schemaBytes = SchemaParser.toJson(schema).getBytes(StandardCharsets.UTF_8);
    ByteBuffer header = ByteBuffer.allocate(FIXED_HEADER_BYTES + schemaBytes.length);
    header
        .put(MAGIC_0)
        .put(MAGIC_1)
        .put(FORMAT_VERSION)
        .putInt(schemaBytes.length)
        .put(schemaBytes);

    org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(schema, AvroSchemaKey.AVRO_ROOT_NAME);
    return new WriteEntry(header.array(), DataWriter.create(avroSchema));
  }

  private static ReadEntry buildReadEntry(String schemaJson) {
    try {
      Schema schema = SchemaParser.fromJson(schemaJson);
      org.apache.avro.Schema avroSchema =
          AvroSchemaUtil.convert(schema, AvroSchemaKey.AVRO_ROOT_NAME);
      return new ReadEntry(
          schema, RawDecoder.create(schema, PlannedDataReader::create, avroSchema));
    } catch (RuntimeException e) {
      throw new DataException("Failed to parse Iceberg schema JSON in " + TYPE + " envelope", e);
    }
  }

  private record WriteEntry(byte[] headerBytes, DatumWriter<GenericRecord> writer) {
  }

  private record ReadEntry(Schema schema, RawDecoder<GenericRecord> decoder) {
  }
}
