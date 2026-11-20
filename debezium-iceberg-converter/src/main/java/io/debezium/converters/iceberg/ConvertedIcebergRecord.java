/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;

/**
 * Immutable result of converting a Kafka Connect {@link
 * org.apache.kafka.connect.source.SourceRecord} into Apache Iceberg structures in memory (Layer 1).
 *
 * @param destination Target topic / destination name from the source record.
 * @param schema Derived Iceberg schema (may be {@code null} for tombstones without a value schema).
 * @param sortOrder Derived Iceberg sort order (or {@link SortOrder#unsorted()} when identifier
 *     fields are disabled/unavailable).
 * @param record Converted Iceberg {@link GenericRecord} ({@code null} for tombstone events).
 * @param schemaChangeEvent Whether the source record represents a Debezium DDL/schema-change event.
 */
public record ConvertedIcebergRecord(
    String destination,
    Schema schema,
    SortOrder sortOrder,
    GenericRecord record,
    boolean schemaChangeEvent) {}
