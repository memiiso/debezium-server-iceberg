package io.debezium.server.iceberg.converter.geospatial;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class GeospatialValuesTest {

  // SRID=4326;POINT(1 2)
  private static final byte[] EWKB = hex("0101000020e6100000000000000000f03f0000000000000040");
  private static final ByteBuffer ISO =
      ByteBuffer.wrap(hex("0101000000000000000000f03f0000000000000040"));
  private static final Schema SCHEMA =
      SchemaBuilder.struct().field("wkb", Schema.OPTIONAL_BYTES_SCHEMA).build();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Type GEOMETRY = Types.GeometryType.crs84();
  private static final Type UNKNOWN_CRS_GEOMETRY = Types.GeometryType.of("srid:0");

  @Test
  void testFromStruct() {
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(SCHEMA).put("wkb", EWKB), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(SCHEMA).put("wkb", ByteBuffer.wrap(EWKB)),
            "g",
            UNKNOWN_CRS_GEOMETRY,
            false));
    assertInvalid(
        "g: unexpected wkb: null",
        () -> GeospatialValues.fromStruct(new Struct(SCHEMA), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: not a Struct: java.lang.String",
        () -> GeospatialValues.fromStruct("x", "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: invalid WKB: truncated or corrupt",
        () ->
            GeospatialValues.fromStruct(
                new Struct(SCHEMA).put("wkb", hex("0101000020")),
                "g",
                UNKNOWN_CRS_GEOMETRY,
                false));
  }

  @Test
  void testFromJson() throws Exception {
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    assertEquals(
        ISO,
        GeospatialValues.fromJson(
            MAPPER.readTree("{\"wkb\":\"" + base64 + "\"}"), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: missing wkb",
        () -> GeospatialValues.fromJson(MAPPER.readTree("{}"), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: missing wkb",
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":null}"), "g", UNKNOWN_CRS_GEOMETRY, false));
    assertInvalid(
        "g: wkb is not base64",
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"%%\"}"), "g", UNKNOWN_CRS_GEOMETRY, false));
  }

  @Test
  void testSridMatchesCrs() {
    assertTrue(GeospatialValues.sridMatchesCrs(4326, GEOMETRY));
    assertFalse(GeospatialValues.sridMatchesCrs(3857, GEOMETRY));
    assertTrue(GeospatialValues.sridMatchesCrs(3857, Types.GeometryType.of("srid:3857")));
    assertTrue(GeospatialValues.sridMatchesCrs(3857, Types.GeometryType.of("EPSG:3857")));
    assertFalse(GeospatialValues.sridMatchesCrs(4326, Types.GeometryType.of("srid:3857")));
    assertTrue(GeospatialValues.sridMatchesCrs(4326, Types.GeographyType.crs84()));
    // values without SRID only match the unknown CRS
    assertFalse(GeospatialValues.sridMatchesCrs(null, GEOMETRY));
    assertFalse(GeospatialValues.sridMatchesCrs(0, GEOMETRY));
    assertFalse(GeospatialValues.sridMatchesCrs(0, Types.GeographyType.crs84()));
    assertTrue(GeospatialValues.sridMatchesCrs(null, UNKNOWN_CRS_GEOMETRY));
    assertTrue(GeospatialValues.sridMatchesCrs(0, UNKNOWN_CRS_GEOMETRY));
    assertFalse(GeospatialValues.sridMatchesCrs(4326, UNKNOWN_CRS_GEOMETRY));
    // a CRS that can't be compared fails
    assertFalse(GeospatialValues.sridMatchesCrs(3857, Types.GeometryType.of("projjson:my_crs")));
  }

  @Test
  void testSridMismatchFails() throws Exception {
    Schema schema =
        SchemaBuilder.struct()
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    String reason =
        "g: SRID 3857 doesn't match the column CRS OGC:CRS84, see"
            + " debezium.sink.iceberg.geometry-crs";
    assertInvalid(
        reason,
        () ->
            GeospatialValues.fromStruct(
                new Struct(schema).put("wkb", EWKB).put("srid", 3857), "g", GEOMETRY, false));
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    assertInvalid(
        reason,
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":3857}"),
                "g",
                GEOMETRY,
                false));
    assertInvalid(
        "g: srid is not an integer: \"x\"",
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":\"x\"}"),
                "g",
                GEOMETRY,
                false));
    // SRID 4326 matches CRS84, SRID 0 means no SRID
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(schema).put("wkb", EWKB).put("srid", 4326), "g", GEOMETRY, false));
    String noSrid =
        "g: value without SRID doesn't match the column CRS OGC:CRS84, see"
            + " debezium.sink.iceberg.geometry-crs and"
            + " debezium.sink.iceberg.geospatial-allow-missing-srid";
    assertInvalid(
        noSrid,
        () ->
            GeospatialValues.fromStruct(
                new Struct(schema).put("wkb", EWKB).put("srid", 0), "g", GEOMETRY, false));
    assertInvalid(
        noSrid,
        () ->
            GeospatialValues.fromStruct(new Struct(schema).put("wkb", EWKB), "g", GEOMETRY, false));
    assertInvalid(
        noSrid,
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":null}"),
                "g",
                GEOMETRY,
                false));
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(schema).put("wkb", EWKB).put("srid", 0), "g", UNKNOWN_CRS_GEOMETRY, false));
  }

  @Test
  void testStructSridOtherTypes() {
    // srid emitted as another numeric type by an upstream transform or custom schema
    Schema int64 =
        SchemaBuilder.struct()
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(int64).put("wkb", EWKB).put("srid", 4326L), "g", GEOMETRY, false));
    Schema int16 =
        SchemaBuilder.struct()
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT16_SCHEMA)
            .build();
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(int16).put("wkb", EWKB).put("srid", (short) 4326), "g", GEOMETRY, false));
    Schema string =
        SchemaBuilder.struct()
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    assertInvalid(
        "g: srid is not an integer: x",
        () ->
            GeospatialValues.fromStruct(
                new Struct(string).put("wkb", EWKB).put("srid", "x"), "g", GEOMETRY, false));
  }

  @Test
  void testAllowMissingSrid() throws Exception {
    Schema schema =
        SchemaBuilder.struct()
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    // values without SRID are written with the column CRS
    for (Type type :
        List.of(GEOMETRY, Types.GeometryType.of("EPSG:2154"), Types.GeographyType.crs84())) {
      assertEquals(
          ISO,
          GeospatialValues.fromStruct(
              new Struct(schema).put("wkb", EWKB).put("srid", 0), "g", type, true));
      assertEquals(
          ISO, GeospatialValues.fromStruct(new Struct(schema).put("wkb", EWKB), "g", type, true));
      assertEquals(
          ISO,
          GeospatialValues.fromJson(
              MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":null}"), "g", type, true));
    }
    // a mismatching SRID still fails
    assertInvalid(
        "g: SRID 3857 doesn't match the column CRS OGC:CRS84, see"
            + " debezium.sink.iceberg.geometry-crs",
        () ->
            GeospatialValues.fromStruct(
                new Struct(schema).put("wkb", EWKB).put("srid", 3857), "g", GEOMETRY, true));
  }

  @Test
  void testWkbCheckedBeforeSrid() throws Exception {
    assertInvalid(
        "g: missing wkb",
        () -> GeospatialValues.fromJson(MAPPER.readTree("{}"), "g", GEOMETRY, false));
    assertInvalid(
        "g: unexpected wkb: null",
        () -> GeospatialValues.fromStruct(new Struct(SCHEMA), "g", GEOMETRY, false));
    // truncated WKB, no SRID
    assertInvalid(
        "g: invalid WKB: truncated or corrupt",
        () ->
            GeospatialValues.fromStruct(
                new Struct(SCHEMA).put("wkb", hex("0101000020")), "g", GEOMETRY, false));
    assertInvalid(
        "g: invalid WKB: truncated or corrupt",
        () ->
            GeospatialValues.fromJson(
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
