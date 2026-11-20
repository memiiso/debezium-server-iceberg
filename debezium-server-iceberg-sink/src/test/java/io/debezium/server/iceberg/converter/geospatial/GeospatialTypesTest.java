package io.debezium.server.iceberg.converter.geospatial;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.IcebergUtil;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GeospatialTypesTest {

  @Mock IcebergConfig icebergConfig;

  @BeforeEach
  void setUp() {
    lenient().when(icebergConfig.geospatialTypesEnabled()).thenReturn(true);
    lenient().when(icebergConfig.geometryCrs()).thenReturn(Types.GeometryType.DEFAULT_CRS);
    lenient().when(icebergConfig.writeFormat()).thenReturn("parquet");
    lenient().when(icebergConfig.formatVersion()).thenReturn("3");
  }

  @Test
  void testValidateGeospatialConfig() {
    IcebergUtil.validateGeospatialConfig(icebergConfig);
    lenient().when(icebergConfig.writeFormat()).thenReturn("avro");
    IcebergUtil.validateGeospatialConfig(icebergConfig);

    lenient().when(icebergConfig.writeFormat()).thenReturn("ORC");
    DebeziumException e =
        assertThrows(
            DebeziumException.class, () -> IcebergUtil.validateGeospatialConfig(icebergConfig));
    assertTrue(e.getMessage().contains("orc"));

    lenient().when(icebergConfig.writeFormat()).thenReturn("parquet");
    lenient().when(icebergConfig.formatVersion()).thenReturn("2");
    e =
        assertThrows(
            DebeziumException.class, () -> IcebergUtil.validateGeospatialConfig(icebergConfig));
    assertTrue(e.getMessage().contains("format-version"));

    lenient().when(icebergConfig.formatVersion()).thenReturn("v3");
    e =
        assertThrows(
            DebeziumException.class, () -> IcebergUtil.validateGeospatialConfig(icebergConfig));
    assertTrue(e.getMessage().contains("invalid debezium.sink.iceberg.format-version: v3"));

    lenient().when(icebergConfig.formatVersion()).thenReturn("3");
    for (String crs : List.of("EPSG:2154", "epsg:3857", "ogc:crs84", "srid:0")) {
      lenient().when(icebergConfig.geometryCrs()).thenReturn(crs);
      IcebergUtil.validateGeospatialConfig(icebergConfig);
    }

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
      lenient().when(icebergConfig.geometryCrs()).thenReturn(crs);
      e =
          assertThrows(
              DebeziumException.class, () -> IcebergUtil.validateGeospatialConfig(icebergConfig));
      assertTrue(e.getMessage().contains("geometry-crs"), crs);
    }

    lenient().when(icebergConfig.geospatialTypesEnabled()).thenReturn(false);
    IcebergUtil.validateGeospatialConfig(icebergConfig);
  }
}
