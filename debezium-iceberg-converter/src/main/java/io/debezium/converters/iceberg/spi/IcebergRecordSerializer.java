/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.spi;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;

/**
 * Pluggable wire serializer/deserializer for Apache Iceberg {@link GenericRecord} instances used by
 * Layer 2 {@code IcebergConverter}.
 *
 * <p>Implementations must provide a public no-argument constructor so they can be instantiated via
 * {@code serializer.type=<FQCN>}.
 */
public interface IcebergRecordSerializer {

  /**
   * Short identifier for this serializer (for example {@code avro-envelope}, {@code
   * avro-single-object}, or {@code json}).
   */
  String type();

  /**
   * Configures this serializer with converter properties.
   *
   * @param configs converter configuration map (may be empty)
   */
  default void configure(Map<String, ?> configs) {}

  /**
   * Serializes an Iceberg {@link GenericRecord} together with its {@link Schema} into a byte array.
   *
   * @param schema Iceberg schema describing {@code record}
   * @param record Iceberg record to serialize
   * @return encoded bytes
   */
  byte[] serialize(Schema schema, GenericRecord record);

  /**
   * Deserializes a byte array previously produced by {@link #serialize(Schema, GenericRecord)}.
   *
   * @param bytes encoded payload
   * @return the resolved Iceberg {@link Schema} and {@link GenericRecord}
   */
  DeserializedIcebergRecord deserialize(byte[] bytes);
}
