package io.debezium.server.iceberg.converter.geospatial;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class JsonGeospatialValuesTest {

  // SRID=4326;POINT(1 2)
  private static final byte[] EWKB = hex("0101000020e6100000000000000000f03f0000000000000040");
  private static final ByteBuffer ISO =
      ByteBuffer.wrap(hex("0101000000000000000000f03f0000000000000040"));
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Type GEOMETRY = Types.GeometryType.crs84();
  private static final Type UNKNOWN_CRS_GEOMETRY = Types.GeometryType.of("srid:0");

  @Test
  void testFromJson() throws Exception {
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    assertEquals(
        ISO,
        JsonGeospatialValues.fromJson(
            MAPPER.readTree("{\"wkb\":\"" + base64 + "\"}"), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: missing wkb",
        () ->
            JsonGeospatialValues.fromJson(MAPPER.readTree("{}"), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: missing wkb",
        () ->
            JsonGeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":null}"), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: wkb is not base64",
        () ->
            JsonGeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"%%\"}"), "g", UNKNOWN_CRS_GEOMETRY, false));
  }

  @Test
  void testSridMismatchFails() {
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    String reason = "g: SRID 3857 doesn't match the column CRS OGC:CRS84, see geometry.crs";
    assertInvalid(
        reason,
        () ->
            JsonGeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":3857}"),
                "g",
                GEOMETRY,
                false));
    assertInvalid(
        "g: srid is not an integer: \"x\"",
        () ->
            JsonGeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":\"x\"}"),
                "g",
                GEOMETRY,
                false));
    String noSrid =
        "g: value without SRID doesn't match the column CRS OGC:CRS84, see geometry.crs and"
            + " geospatial.allow.missing.srid";
    assertInvalid(
        noSrid,
        () ->
            JsonGeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":null}"),
                "g",
                GEOMETRY,
                false));
  }

  @Test
  void testAllowMissingSrid() throws Exception {
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    for (Type type :
        List.of(GEOMETRY, Types.GeometryType.of("EPSG:2154"), Types.GeographyType.crs84())) {
      assertEquals(
          ISO,
          JsonGeospatialValues.fromJson(
              MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":null}"), "g", type, true));
    }
  }

  @Test
  void testWkbCheckedBeforeSrid() {
    assertInvalid(
        "g: missing wkb",
        () -> JsonGeospatialValues.fromJson(MAPPER.readTree("{}"), "g", GEOMETRY, false));
    assertInvalid(
        "g: invalid WKB: truncated or corrupt",
        () ->
            JsonGeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"AQEAACA=\"}"), "g", GEOMETRY, false));
  }

  private static void assertInvalid(String reason, org.junit.jupiter.api.function.Executable call) {
    DebeziumException e = assertThrows(DebeziumException.class, call);
    assertEquals("Invalid geospatial value, field: " + reason, e.getMessage());
  }

  private static byte[] hex(String hex) {
    return HexFormat.of().parseHex(hex);
  }
}
