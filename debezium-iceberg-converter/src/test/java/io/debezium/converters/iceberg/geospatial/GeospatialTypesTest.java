package io.debezium.converters.iceberg.geospatial;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import io.debezium.DebeziumException;
import io.debezium.converters.iceberg.IcebergConverterConfig;
import io.debezium.data.geometry.Point;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GeospatialTypesTest {

  @Mock IcebergConverterConfig config;

  @BeforeEach
  void setUp() {
    lenient().when(config.geospatialTypesEnabled()).thenReturn(true);
    lenient().when(config.geometryCrs()).thenReturn(Types.GeometryType.DEFAULT_CRS);
    lenient().when(config.propertyName(anyString())).thenCallRealMethod();
  }

  @Test
  void testIcebergType() {
    assertEquals(
        Types.GeometryType.crs84(),
        GeospatialTypes.icebergType(GeospatialTypes.GEOMETRY_LOGICAL_NAME, config));
    assertEquals(
        Types.GeographyType.crs84(),
        GeospatialTypes.icebergType(GeospatialTypes.GEOGRAPHY_LOGICAL_NAME, config));
    assertNull(GeospatialTypes.icebergType(Point.LOGICAL_NAME, config));
    assertNull(GeospatialTypes.icebergType("some.Struct", config));
    assertNull(GeospatialTypes.icebergType(null, config));

    lenient().when(config.geometryCrs()).thenReturn("EPSG:3857");
    assertEquals(
        Types.GeometryType.of("EPSG:3857"),
        GeospatialTypes.icebergType(GeospatialTypes.GEOMETRY_LOGICAL_NAME, config));

    lenient().when(config.geospatialTypesEnabled()).thenReturn(false);
    assertNull(GeospatialTypes.icebergType(GeospatialTypes.GEOMETRY_LOGICAL_NAME, config));
  }

  @Test
  void testSrid() {
    assertEquals(4326, GeospatialTypes.srid("OGC:CRS84"));
    assertEquals(3857, GeospatialTypes.srid("srid:3857"));
    assertEquals(2154, GeospatialTypes.srid("EPSG:2154"));
    assertNull(GeospatialTypes.srid("projjson:my_crs"));
    assertNull(GeospatialTypes.srid("srid:abc"));
  }

  @Test
  void testIsGeospatial() {
    assertTrue(GeospatialTypes.isGeospatial(Types.GeometryType.crs84()));
    assertTrue(GeospatialTypes.isGeospatial(Types.GeographyType.crs84()));
    assertFalse(GeospatialTypes.isGeospatial(Types.BinaryType.get()));
  }

  @Test
  void testValidateCrsAndConfig() {
    GeospatialTypes.validateConfig(config);

    for (String crs : List.of("EPSG:2154", "epsg:3857", "ogc:crs84", "srid:0")) {
      assertTrue(GeospatialTypes.isValidCrs(crs), crs);
      lenient().when(config.geometryCrs()).thenReturn(crs);
      GeospatialTypes.validateConfig(config);
    }

    assertFalse(GeospatialTypes.isValidCrs(null));
    // GeoParquet 2.0 readers may not resolve srid:<N>
    for (String crs :
        List.of(
            "projjson:my_crs",
            "srid:3857",
            "EPSG:0",
            "EPSG:-1",
            "EPSG:+3857",
            "EPSG:03857",
            "EPSG:",
            "srid:-0")) {
      assertFalse(GeospatialTypes.isValidCrs(crs), crs);
      lenient().when(config.geometryCrs()).thenReturn(crs);
      DebeziumException e =
          assertThrows(DebeziumException.class, () -> GeospatialTypes.validateConfig(config));
      assertTrue(e.getMessage().contains("geometry.crs"), crs);
    }

    lenient().when(config.geospatialTypesEnabled()).thenReturn(false);
    GeospatialTypes.validateConfig(config);
  }
}
