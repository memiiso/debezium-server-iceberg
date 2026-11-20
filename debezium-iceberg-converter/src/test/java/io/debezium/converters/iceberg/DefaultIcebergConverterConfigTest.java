/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.jdbc.TemporalPrecisionMode;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class DefaultIcebergConverterConfigTest {

  @Test
  void testDefaults() {
    IcebergConverterConfig config = IcebergConverterConfig.defaults();

    assertEquals(TemporalPrecisionMode.ISOSTRING, config.temporalPrecisionMode());
    assertTrue(config.eventFlatteningEnabled());
    assertTrue(config.createIdentifierFields());
    assertFalse(config.preserveRequiredProperty());
    assertFalse(config.replaceNullWithDefault());
    assertFalse(config.nestedAsVariant());
    assertEquals(List.of(), config.excludedColumns());
    assertFalse(config.geospatialTypesEnabled());
    assertEquals(Types.GeometryType.DEFAULT_CRS, config.geometryCrs());
    assertFalse(config.geospatialAllowMissingSrid());
    assertEquals("geometry.crs", config.propertyName("geometry.crs"));
  }

  @Test
  void testBuilderOverridesAndExcludedColumnsSanitization() {
    DefaultIcebergConverterConfig config =
        IcebergConverterConfig.builder()
            .temporalPrecisionMode(TemporalPrecisionMode.NANOSECONDS)
            .eventFlatteningEnabled(false)
            .createIdentifierFields(false)
            .preserveRequiredProperty(true)
            .replaceNullWithDefault(true)
            .nestedAsVariant(true)
            .excludedColumns(Arrays.asList(" col_a ", null, " ", "col_b"))
            .geospatialTypesEnabled(true)
            .geometryCrs("EPSG:3857")
            .geospatialAllowMissingSrid(true)
            .build();

    assertEquals(TemporalPrecisionMode.NANOSECONDS, config.temporalPrecisionMode());
    assertFalse(config.eventFlatteningEnabled());
    assertFalse(config.createIdentifierFields());
    assertTrue(config.preserveRequiredProperty());
    assertTrue(config.replaceNullWithDefault());
    assertTrue(config.nestedAsVariant());
    assertEquals(List.of("col_a", "col_b"), config.excludedColumns());
    assertTrue(config.geospatialTypesEnabled());
    assertEquals("EPSG:3857", config.geometryCrs());
    assertTrue(config.geospatialAllowMissingSrid());
  }

  @Test
  void testBuilderValidatesGeometryCrsWhenGeospatialEnabled() {
    assertThrows(
        DebeziumException.class,
        () ->
            DefaultIcebergConverterConfig.builder()
                .geospatialTypesEnabled(true)
                .geometryCrs("srid:3857")
                .build());

    // Ignored when geospatial types are disabled
    DefaultIcebergConverterConfig disabled =
        DefaultIcebergConverterConfig.builder()
            .geospatialTypesEnabled(false)
            .geometryCrs("srid:3857")
            .build();
    assertFalse(disabled.geospatialTypesEnabled());
  }
}
