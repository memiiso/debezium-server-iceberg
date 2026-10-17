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

  @Test
  void testFromStruct() {
    assertEquals(
        ISO, GeospatialValues.fromStruct(new Struct(SCHEMA).put("wkb", EWKB), "g", GEOMETRY));
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(SCHEMA).put("wkb", ByteBuffer.wrap(EWKB)), "g", GEOMETRY));
    assertInvalid(
        "g: unexpected wkb: null",
        () -> GeospatialValues.fromStruct(new Struct(SCHEMA), "g", GEOMETRY));
    assertInvalid(
        "g: not a Struct: java.lang.String", () -> GeospatialValues.fromStruct("x", "g", GEOMETRY));
    assertInvalid(
        "g: invalid WKB: truncated or corrupt",
        () ->
            GeospatialValues.fromStruct(
                new Struct(SCHEMA).put("wkb", hex("0101000020")), "g", GEOMETRY));
  }

  @Test
  void testFromJson() throws Exception {
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    assertEquals(
        ISO,
        GeospatialValues.fromJson(MAPPER.readTree("{\"wkb\":\"" + base64 + "\"}"), "g", GEOMETRY));
    assertInvalid(
        "g: missing wkb", () -> GeospatialValues.fromJson(MAPPER.readTree("{}"), "g", GEOMETRY));
    assertInvalid(
        "g: missing wkb",
        () -> GeospatialValues.fromJson(MAPPER.readTree("{\"wkb\":null}"), "g", GEOMETRY));
    assertInvalid(
        "g: wkb is not base64",
        () -> GeospatialValues.fromJson(MAPPER.readTree("{\"wkb\":\"%%\"}"), "g", GEOMETRY));
  }

  @Test
  void testSridMatchesCrs() {
    assertTrue(GeospatialValues.sridMatchesCrs(4326, GEOMETRY));
    assertFalse(GeospatialValues.sridMatchesCrs(3857, GEOMETRY));
    assertTrue(GeospatialValues.sridMatchesCrs(3857, Types.GeometryType.of("srid:3857")));
    assertTrue(GeospatialValues.sridMatchesCrs(3857, Types.GeometryType.of("EPSG:3857")));
    assertFalse(GeospatialValues.sridMatchesCrs(4326, Types.GeometryType.of("srid:3857")));
    assertTrue(GeospatialValues.sridMatchesCrs(4326, Types.GeographyType.crs84()));
    // values without SRID are accepted
    assertTrue(GeospatialValues.sridMatchesCrs(null, GEOMETRY));
    assertTrue(GeospatialValues.sridMatchesCrs(0, GEOMETRY));
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
                new Struct(schema).put("wkb", EWKB).put("srid", 3857), "g", GEOMETRY));
    String base64 = Base64.getEncoder().encodeToString(EWKB);
    assertInvalid(
        reason,
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":3857}"), "g", GEOMETRY));
    assertInvalid(
        "g: srid is not an integer: \"x\"",
        () ->
            GeospatialValues.fromJson(
                MAPPER.readTree("{\"wkb\":\"" + base64 + "\",\"srid\":\"x\"}"), "g", GEOMETRY));
    // SRID 4326 matches CRS84, SRID 0 means no SRID
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(schema).put("wkb", EWKB).put("srid", 4326), "g", GEOMETRY));
    assertEquals(
        ISO,
        GeospatialValues.fromStruct(
            new Struct(schema).put("wkb", EWKB).put("srid", 0), "g", GEOMETRY));
  }

  private static void assertInvalid(String reason, org.junit.jupiter.api.function.Executable call) {
    DebeziumException e = assertThrows(DebeziumException.class, call);
    assertEquals("Invalid geospatial value, field: " + reason, e.getMessage());
  }

  private static byte[] hex(String hex) {
    return HexFormat.of().parseHex(hex);
  }
}
