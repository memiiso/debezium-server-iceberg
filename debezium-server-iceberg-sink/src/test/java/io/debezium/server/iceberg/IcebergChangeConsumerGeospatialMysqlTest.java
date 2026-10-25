/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** End to end: MySQL spatial columns to Iceberg v3 geometry, longitude first for SRID 4326. */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerGeospatialMysqlTest.TestProfile.class)
public class IcebergChangeConsumerGeospatialMysqlTest extends BaseTest {

  // column -> {mysql type, value}, WKT in longitude-latitude order
  private static final Map<String, String[]> COLUMNS = new LinkedHashMap<>();

  static {
    // no SRID, written as OGC:CRS84 with geospatial-allow-missing-srid
    COLUMNS.put("g", new String[] {"GEOMETRY", "LINESTRING(0 0,1 1)"});
    COLUMNS.put(
        "g_linestring", new String[] {"LINESTRING SRID 4326", "LINESTRING(2.35 48.85,-0.12 51.5)"});
    COLUMNS.put(
        "g_polygon", new String[] {"POLYGON SRID 4326", "POLYGON((2 48,3 48,3 49,2 49,2 48))"});
    COLUMNS.put(
        "g_multipoint",
        new String[] {"MULTIPOINT SRID 4326", "MULTIPOINT((2.35 48.85),(4.83 45.76))"});
    COLUMNS.put(
        "g_multilinestring",
        new String[] {"MULTILINESTRING SRID 4326", "MULTILINESTRING((2 48,3 49),(4 45,5 46))"});
    COLUMNS.put(
        "g_multipolygon",
        new String[] {"MULTIPOLYGON SRID 4326", "MULTIPOLYGON(((2 48,3 48,3 49,2 48)))"});
    COLUMNS.put(
        "g_geometrycollection",
        new String[] {
          "GEOMETRYCOLLECTION SRID 4326",
          "GEOMETRYCOLLECTION(POINT(2.35 48.85),LINESTRING(2 48,3 49))"
        });
  }

  @Test
  public void testGeospatialTypes() throws Exception {
    String columnDefinitions =
        COLUMNS.entrySet().stream()
            .map(c -> c.getKey() + " " + c.getValue()[0])
            .collect(Collectors.joining(", "));
    String values =
        COLUMNS.values().stream()
            .map(
                c ->
                    c[0].contains("SRID 4326")
                        ? "ST_GeomFromText('" + c[1] + "', 4326, 'axis-order=long-lat')"
                        : "ST_GeomFromText('" + c[1] + "')")
            .collect(Collectors.joining(", "));
    SourceMysqlDB.runSQL(
        "CREATE TABLE inventory.geo_types (id INTEGER PRIMARY KEY, "
            + columnDefinitions
            + ", p POINT SRID 4326)");
    SourceMysqlDB.runSQL(
        "INSERT INTO inventory.geo_types VALUES (1, "
            + values
            + ", ST_GeomFromText('POINT(2.35 48.85)', 4326, 'axis-order=long-lat'))");
    SourceMysqlDB.runSQL("INSERT INTO inventory.geo_types (id) VALUES (2)");

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
    Map<Integer, Record> rows =
        Lists.newArrayList(getTableDataV2("testc.inventory.geo_types")).stream()
            .collect(Collectors.toMap(r -> (Integer) r.getField("id"), r -> r));

    try (Connection con = SourceMysqlDB.connection()) {
      for (String column : COLUMNS.keySet()) {
        assertEquals(Types.GeometryType.crs84(), table.schema().findType(column), column);
        assertEquals(isoWkb(con, column), hex(rows.get(1).getField(column)), column);
        assertNull(rows.get(2).getField(column), column);
      }
    }
    // LINESTRING(2.35 48.85, ...): longitude first, as OGC:CRS84 requires
    ByteBuffer line =
        ((ByteBuffer) rows.get(1).getField("g_linestring")).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(2.35, line.getDouble(9));
    assertEquals(48.85, line.getDouble(17));
    assertTrue(
        getDataFiles(table).stream()
            .anyMatch(
                f ->
                    f.lowerBounds()
                        .containsKey(table.schema().findField("g_linestring").fieldId())));
    // MySQL POINT is sent as io.debezium.data.geometry.Point, like PostgreSQL native point
    assertEquals(
        List.of("x", "y", "wkb", "srid"),
        table.schema().findType("p").asStructType().fields().stream()
            .map(Types.NestedField::name)
            .toList());
  }

  /** MySQL WKB of row 1, longitude first, as hex. */
  private static String isoWkb(Connection con, String column) throws Exception {
    String axisOrder =
        COLUMNS.get(column)[0].contains("SRID 4326") ? ", 'axis-order=long-lat'" : "";
    try (ResultSet rs =
        con.createStatement()
            .executeQuery(
                "SELECT HEX(ST_AsBinary("
                    + column
                    + axisOrder
                    + ")) FROM inventory.geo_types WHERE id=1")) {
      rs.next();
      return rs.getString(1).toLowerCase();
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
      return Map.of(
          "quarkus.profile", "mysql",
          "%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector",
          "%mysql.debezium.source.table.whitelist", "inventory.geo_types",
          "debezium.sink.iceberg.geospatial-types-enabled", "true",
          "debezium.sink.iceberg.geospatial-allow-missing-srid", "true");
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }
}
