/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.spi;

import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;

/**
 * Result of deserializing a byte payload produced by an {@link IcebergRecordSerializer}.
 *
 * @param schema the resolved Apache Iceberg {@link Schema}
 * @param record the deserialized Apache Iceberg {@link GenericRecord}
 */
public record DeserializedIcebergRecord(Schema schema, GenericRecord record) {

  public DeserializedIcebergRecord {
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(record, "record must not be null");
  }
}
