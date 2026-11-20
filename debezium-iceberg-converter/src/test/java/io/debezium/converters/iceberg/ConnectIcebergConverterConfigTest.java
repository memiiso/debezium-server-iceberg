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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.jdbc.TemporalPrecisionMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.jupiter.api.Test;

class ConnectIcebergConverterConfigTest {

  @Test
  void testDefaults() {
    ConnectIcebergConverterConfig config = ConnectIcebergConverterConfig.defaults();

    assertEquals(ConverterType.VALUE, config.type());
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
    assertEquals(IcebergStructConverter.DEFAULT_SCHEMA_CACHE_SIZE, config.schemaCacheSize());
    assertEquals("avro-envelope", config.serializerType());
    assertEquals("", config.avroSchemaStoreClass());
  }

  @Test
  void testFromMapOverrides() {
    ConnectIcebergConverterConfig config = ConnectIcebergConverterConfig.fromMap(
        Map.ofEntries(
            Map.entry(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName()),
            Map.entry("temporal.precision.mode", "microseconds"),
            Map.entry("event.flattening.enabled", "false"),
            Map.entry("create.identifier.fields", "false"),
            Map.entry("preserve.required.property", "true"),
            Map.entry("replace.null.with.default", "true"),
            Map.entry("nested.as.variant", "true"),
            Map.entry("excluded.columns", " col_a , col_b , "),
            Map.entry("geospatial.types.enabled", "true"),
            Map.entry("geometry.crs", "EPSG:3857"),
            Map.entry("geospatial.allow.missing.srid", "true"),
            Map.entry("schema.cache.size", "64"),
            Map.entry("serializer.type", "avro-single-object")));

    assertEquals(ConverterType.KEY, config.type());
    assertEquals(TemporalPrecisionMode.MICROSECONDS, config.temporalPrecisionMode());
    assertFalse(config.eventFlatteningEnabled());
    assertFalse(config.createIdentifierFields());
    assertTrue(config.preserveRequiredProperty());
    assertTrue(config.replaceNullWithDefault());
    assertTrue(config.nestedAsVariant());
    assertEquals(List.of("col_a", "col_b"), config.excludedColumns());
    assertTrue(config.geospatialTypesEnabled());
    assertEquals("EPSG:3857", config.geometryCrs());
    assertTrue(config.geospatialAllowMissingSrid());
    assertEquals(64, config.schemaCacheSize());
    assertEquals("avro-single-object", config.serializerType());
  }

  @Test
  void testFromMapRejectsNull() {
    assertThrows(NullPointerException.class, () -> ConnectIcebergConverterConfig.fromMap(null));
  }

  @Test
  void testInvalidTemporalPrecisionModeThrows() {
    assertThrows(
        ConfigException.class,
        () -> ConnectIcebergConverterConfig.fromMap(Map.of("temporal.precision.mode", "invalid")));

    List<ConfigValue> validated = ConnectIcebergConverterConfig.configDef()
        .validate(
            Map.of(
                ConverterConfig.TYPE_CONFIG,
                ConverterType.VALUE.getName(),
                "temporal.precision.mode",
                "invalid"));
    ConfigValue modeVal = validated.stream()
        .filter(v -> "temporal.precision.mode".equals(v.name()))
        .findFirst()
        .orElseThrow();
    assertFalse(modeVal.errorMessages().isEmpty());
  }

  @Test
  void testNegativeSchemaCacheSizeThrows() {
    assertThrows(
        ConfigException.class,
        () -> ConnectIcebergConverterConfig.fromMap(Map.of("schema.cache.size", "-1")));
  }

  @Test
  void testDirectConstructorRequiresConverterType() {
    assertThrows(ConfigException.class, () -> new ConnectIcebergConverterConfig(Map.of()));
  }

  @Test
  void testInvalidGeometryCrsWhenGeospatialEnabledThrows() {
    assertThrows(
        DebeziumException.class,
        () -> ConnectIcebergConverterConfig.fromMap(
            Map.of("geospatial.types.enabled", "true", "geometry.crs", "srid:3857")));

    // Ignored when geospatial types are disabled
    ConnectIcebergConverterConfig disabled = ConnectIcebergConverterConfig.fromMap(
        Map.of("geospatial.types.enabled", "false", "geometry.crs", "srid:3857"));
    assertFalse(disabled.geospatialTypesEnabled());
  }

  @Test
  void testNullExcludedColumnsAndNullElementsAreHandledSafely() {
    Map<String, Object> withNullValue = new HashMap<>();
    withNullValue.put("excluded.columns", null);
    assertEquals(List.of(), ConnectIcebergConverterConfig.fromMap(withNullValue).excludedColumns());

    Map<String, Object> withNullElements = new HashMap<>();
    withNullElements.put("excluded.columns", Arrays.asList(" col_a ", null, "", "col_b"));
    assertEquals(
        List.of("col_a", "col_b"),
        ConnectIcebergConverterConfig.fromMap(withNullElements).excludedColumns());
  }

  @Test
  void testAllFieldsAndConfigDefAndModuleVersion() {
    assertNotNull(ConnectIcebergConverterConfig.configDef());
    assertEquals(13, ConnectIcebergConverterConfig.allFields().asArray().length);
    assertNotNull(Module.version());
    assertNotEquals("unknown", Module.version());
    assertFalse(Module.version().contains("${"));
  }
}
