/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import io.debezium.jdbc.TemporalPrecisionMode;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.types.Types;

/**
 * Pure-Java configuration contract for converting Kafka Connect / Debezium
 * {@code Schema} and
 * {@code Struct} records into Apache Iceberg {@code Schema}, {@code SortOrder},
 * and {@code
 * GenericRecord} instances.
 */
public interface IcebergConverterConfig {

  List<String> TS_MS_FIELDS = List.of("__ts_ms", "__source_ts_ms");

  static IcebergConverterConfig defaults() {
    return DefaultIcebergConverterConfig.defaults();
  }

  static DefaultIcebergConverterConfig.Builder builder() {
    return DefaultIcebergConverterConfig.builder();
  }

  default TemporalPrecisionMode temporalPrecisionMode() {
    return TemporalPrecisionMode.ISOSTRING;
  }

  default boolean eventFlatteningEnabled() {
    return true;
  }

  default boolean createIdentifierFields() {
    return true;
  }

  default boolean preserveRequiredProperty() {
    return false;
  }

  default boolean replaceNullWithDefault() {
    return false;
  }

  default boolean nestedAsVariant() {
    return false;
  }

  default List<String> excludedColumns() {
    return Collections.emptyList();
  }

  default boolean geospatialTypesEnabled() {
    return false;
  }

  default String geometryCrs() {
    return Types.GeometryType.DEFAULT_CRS;
  }

  default boolean geospatialAllowMissingSrid() {
    return false;
  }


  /**
   * Formats a dot-case converter property name for user-facing error messages.
   * Adapters such as
   * {@code SinkIcebergConverterConfig} override this to emit their native
   * property naming (e.g.,
   * {@code debezium.sink.iceberg.*}).
   */
  default String propertyName(String connectKey) {
    return connectKey;
  }
}
