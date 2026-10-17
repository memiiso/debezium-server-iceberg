/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** End to end: PostGIS columns of every geospatial type to Iceberg v3 geometry/geography. */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerGeospatialTest.TestProfile.class)
public class IcebergChangeConsumerGeospatialTest extends BaseTest {

  // column -> {postgres type, value}
  private static final Map<String, String[]> GEOMETRY_COLUMNS = new LinkedHashMap<>();
  private static final Map<String, String[]> GEOGRAPHY_COLUMNS = new LinkedHashMap<>();
  // Iceberg 1.12 limit: no bounds for curves, PolyhedralSurface, TIN, Triangle
  private static final List<String> UNBOUNDED_COLUMNS =
      List.of(
          "g_circularstring",
          "g_compoundcurve",
          "g_curvepolygon",
          "g_multicurve",
          "g_multisurface",
          "g_polyhedralsurface",
          "g_tin",
          "g_triangle");

  static {
    geometry("g", "geometry", "SRID=4326;LINESTRING(0 0,1 1)");
    geometry("g_point", "geometry(Point,4326)", "SRID=4326;POINT(1 2)");
    geometry("g_linestring", "geometry(LineString,4326)", "SRID=4326;LINESTRING(0 0,1 1,2 3)");
    geometry(
        "g_polygon",
        "geometry(Polygon,4326)",
        "SRID=4326;POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 1))");
    geometry("g_multipoint", "geometry(MultiPoint,4326)", "SRID=4326;MULTIPOINT((1 2),(3 4))");
    geometry(
        "g_multilinestring",
        "geometry(MultiLineString,4326)",
        "SRID=4326;MULTILINESTRING((0 0,1 1),(2 2,3 3))");
    geometry(
        "g_multipolygon",
        "geometry(MultiPolygon,4326)",
        "SRID=4326;MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((5 5,6 5,6 6,5 5)))");
    geometry(
        "g_geometrycollection",
        "geometry(GeometryCollection,4326)",
        "SRID=4326;GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))");
    geometry("g_point_z", "geometry(PointZ,4326)", "SRID=4326;POINT Z(1 2 3)");
    geometry("g_point_m", "geometry(PointM,4326)", "SRID=4326;POINT M(1 2 4)");
    geometry("g_point_zm", "geometry(PointZM,4326)", "SRID=4326;POINT ZM(1 2 3 4)");
    geometry(
        "g_polygon_z", "geometry(PolygonZ,4326)", "SRID=4326;POLYGON Z((0 0 1,4 0 1,4 4 1,0 0 1))");
    geometry("g_empty", "geometry", "POINT EMPTY");
    geometry("g_circularstring", "geometry(CircularString)", "CIRCULARSTRING(0 0,1 1,2 0)");
    geometry(
        "g_compoundcurve",
        "geometry(CompoundCurve)",
        "COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,2 0),(2 0,3 0))");
    geometry(
        "g_curvepolygon",
        "geometry(CurvePolygon)",
        "CURVEPOLYGON(CIRCULARSTRING(0 0,4 0,4 4,0 4,0 0))");
    geometry(
        "g_multicurve",
        "geometry(MultiCurve)",
        "MULTICURVE((0 0,5 5),CIRCULARSTRING(4 0,4 4,8 4))");
    geometry(
        "g_multisurface",
        "geometry(MultiSurface)",
        "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(0 0,4 0,4 4,0 4,0 0)),((10 10,14 12,11 10,10 10)))");
    geometry(
        "g_polyhedralsurface",
        "geometry(PolyhedralSurfaceZ)",
        "POLYHEDRALSURFACE Z(((0 0 0,0 0 1,0 1 1,0 0 0)))");
    geometry("g_tin", "geometry(TinZ)", "TIN Z(((0 0 0,0 0 1,0 1 0,0 0 0)))");
    geometry("g_triangle", "geometry(Triangle)", "TRIANGLE((0 0,0 9,9 0,0 0))");
    GEOGRAPHY_COLUMNS.put("geog", new String[] {"geography", "SRID=4326;LINESTRING(0 0,1 1)"});
    GEOGRAPHY_COLUMNS.put(
        "geog_point", new String[] {"geography(Point,4326)", "POINT(2.35 48.85)"});
    GEOGRAPHY_COLUMNS.put(
        "geog_point_z", new String[] {"geography(PointZ,4326)", "POINT Z(2.35 48.85 35)"});
    GEOGRAPHY_COLUMNS.put(
        "geog_polygon", new String[] {"geography(Polygon,4326)", "POLYGON((0 0,1 0,1 1,0 0))"});
  }

  private static void geometry(String column, String type, String value) {
    GEOMETRY_COLUMNS.put(column, new String[] {type, value});
  }

  @Test
  public void testGeospatialTypes() throws Exception {
    Map<String, String[]> columns = new LinkedHashMap<>(GEOMETRY_COLUMNS);
    columns.putAll(GEOGRAPHY_COLUMNS);
    String columnDefinitions =
        columns.entrySet().stream()
            .map(c -> c.getKey() + " " + c.getValue()[0])
            .collect(Collectors.joining(",\n"));
    String values =
        columns.values().stream().map(c -> "'" + c[1] + "'").collect(Collectors.joining(","));
    SourcePostgresqlDB.runSQL(
        """
        SET search_path TO inventory, public;
        CREATE TABLE inventory.geo_types (id INTEGER PRIMARY KEY, %s,
          g_array geometry[], p point);
        INSERT INTO inventory.geo_types VALUES
          (1, %s, ARRAY['SRID=4326;POINT(1 2)'::geometry, 'SRID=4326;POINT Z(1 2 3)'::geometry],
           '(350,120)');
        INSERT INTO inventory.geo_types (id) VALUES (2);
        """
            .formatted(columnDefinitions, values));

    Awaitility.await()
        .atMost(Duration.ofSeconds(180))
        .until(
            () -> {
              try {
                return Lists.newArrayList(getTableDataV2("testc.inventory.geo_types")).size() == 2;
              } catch (Exception e) {
                return false;
              }
            });

    Table table =
        consumer.loadIcebergTable(
            TableIdentifier.of(
                TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE,
                "debeziumcdc_testc_inventory_geo_types"),
            null);
    printTableData(getTableDataV2("testc.inventory.geo_types"));
    Map<Integer, Record> rows =
        Lists.newArrayList(getTableDataV2("testc.inventory.geo_types")).stream()
            .collect(Collectors.toMap(r -> (Integer) r.getField("id"), r -> r));
    Map<String, String> expected = postgisIsoWkb(columns.keySet());

    // types
    assertEquals(3, org.apache.iceberg.TableUtil.formatVersion(table));
    GEOMETRY_COLUMNS
        .keySet()
        .forEach(c -> assertEquals(Types.GeometryType.crs84(), table.schema().findType(c), c));
    GEOGRAPHY_COLUMNS
        .keySet()
        .forEach(c -> assertEquals(Types.GeographyType.crs84(), table.schema().findType(c), c));
    assertEquals(
        Types.GeometryType.crs84(), table.schema().findType("g_array").asListType().elementType());
    assertTrue(table.schema().findType("p").isStructType());

    // values, compared to PostGIS ST_AsBinary
    for (String column : columns.keySet()) {
      assertEquals(expected.get(column), hex(rows.get(1).getField(column)), column);
      assertNull(rows.get(2).getField(column), column);
    }
    List<?> array = (List<?>) rows.get(1).getField("g_array");
    assertEquals(expected.get("g_array[1]"), hex(array.get(0)));
    assertEquals(expected.get("g_array[2]"), hex(array.get(1)));
    Record point = (Record) rows.get(1).getField("p");
    assertEquals(350.0, point.getField("x"));
    assertEquals(120.0, point.getField("y"));

    // bounds of the data file holding row 1
    DataFile dataFile =
        getDataFiles(table).stream()
            .filter(f -> f.recordCount() > 0)
            .filter(f -> f.lowerBounds().containsKey(table.schema().findField("g_point").fieldId()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no data file with geometry bounds"));
    for (String column : GEOMETRY_COLUMNS.keySet()) {
      int fieldId = table.schema().findField(column).fieldId();
      boolean hasBounds = dataFile.lowerBounds().containsKey(fieldId);
      if (UNBOUNDED_COLUMNS.contains(column) || column.equals("g_empty")) {
        assertFalse(hasBounds, column + " should have no bounds");
      } else {
        assertTrue(hasBounds, column + " should have bounds");
      }
    }
  }

  /** PostGIS ISO WKB of row 1, as hex. */
  private static Map<String, String> postgisIsoWkb(Iterable<String> columns) throws Exception {
    Map<String, String> result = new LinkedHashMap<>();
    try (Connection con = SourcePostgresqlDB.connection()) {
      // PostGIS is installed in the inventory schema
      con.createStatement().execute("SET search_path TO inventory, public");
      for (String column : columns) {
        result.put(column, isoWkb(con, column + "::geometry"));
      }
      result.put("g_array[1]", isoWkb(con, "g_array[1]"));
      result.put("g_array[2]", isoWkb(con, "g_array[2]"));
    }
    return result;
  }

  private static String isoWkb(Connection con, String expression) throws Exception {
    try (ResultSet rs =
        con.createStatement()
            .executeQuery(
                "SELECT encode(ST_AsBinary("
                    + expression
                    + "),'hex') FROM inventory.geo_types WHERE id=1")) {
      rs.next();
      return rs.getString(1);
    }
  }

  private static String hex(Object value) {
    return value == null
        ? null
        : HexFormat.of().formatHex(ByteBuffers.toByteArray((ByteBuffer) value));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("debezium.sink.iceberg.geospatial-types", "true");
    }
  }
}
