/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.debezium.converters.iceberg.IcebergConverterConfig;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SinkIcebergConverterConfigTest {

  @Mock GlobalConfig globalConfig;
  @Mock IcebergConfig icebergConfig;
  @Mock DebeziumConfig debeziumConfig;

  @BeforeEach
  void setUp() {
    lenient().when(globalConfig.iceberg()).thenReturn(icebergConfig);
    lenient().when(globalConfig.debezium()).thenReturn(debeziumConfig);
  }

  @Test
  void testDelegatesAllPropertiesFromGlobalConfig() {
    when(debeziumConfig.temporalPrecisionMode()).thenReturn(TemporalPrecisionMode.NANOSECONDS);
    when(debeziumConfig.isEventFlatteningEnabled()).thenReturn(false);
    when(icebergConfig.createIdentifierFields()).thenReturn(false);
    when(icebergConfig.preserveRequiredProperty()).thenReturn(true);
    when(icebergConfig.replaceNullWithDefault()).thenReturn(true);
    when(icebergConfig.nestedAsVariant()).thenReturn(true);
    when(icebergConfig.excludedColumns()).thenReturn(Optional.of(List.of("col_a", "col_b")));
    when(icebergConfig.geospatialTypesEnabled()).thenReturn(true);
    when(icebergConfig.geometryCrs()).thenReturn("EPSG:3857");
    when(icebergConfig.geospatialAllowMissingSrid()).thenReturn(true);

    IcebergConverterConfig adapter = SinkIcebergConverterConfig.from(globalConfig);

    assertEquals(TemporalPrecisionMode.NANOSECONDS, adapter.temporalPrecisionMode());
    assertFalse(adapter.eventFlatteningEnabled());
    assertFalse(adapter.createIdentifierFields());
    assertTrue(adapter.preserveRequiredProperty());
    assertTrue(adapter.replaceNullWithDefault());
    assertTrue(adapter.nestedAsVariant());
    assertEquals(List.of("col_a", "col_b"), adapter.excludedColumns());
    assertTrue(adapter.geospatialTypesEnabled());
    assertEquals("EPSG:3857", adapter.geometryCrs());
    assertTrue(adapter.geospatialAllowMissingSrid());
  }

  @Test
  void testHandlesEmptyExcludedColumnsOptional() {
    when(icebergConfig.excludedColumns()).thenReturn(Optional.empty());
    IcebergConverterConfig adapter = SinkIcebergConverterConfig.from(globalConfig);
    assertEquals(List.of(), adapter.excludedColumns());
    assertEquals(0, adapter.schemaCacheSize());
  }

  @Test
  void testDelegatesLazilyWhenConfigChanges() {
    IcebergConverterConfig adapter = SinkIcebergConverterConfig.from(globalConfig);
    when(icebergConfig.geospatialAllowMissingSrid()).thenReturn(false);
    assertFalse(adapter.geospatialAllowMissingSrid());

    when(icebergConfig.geospatialAllowMissingSrid()).thenReturn(true);
    assertTrue(adapter.geospatialAllowMissingSrid());
  }
}
