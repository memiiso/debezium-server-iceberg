package io.debezium.server.iceberg.converter.geospatial;

import static io.debezium.server.iceberg.converter.geospatial.GeospatialTypes.GEOGRAPHY_LOGICAL_NAME;
import static io.debezium.server.iceberg.converter.geospatial.GeospatialTypes.GEOMETRY_LOGICAL_NAME;
import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

import io.debezium.DebeziumException;
import io.debezium.data.geometry.Point;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.EventFactory;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.converter.StructEventConverter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GeospatialEventConverterTest {

  // POINT(1 2)
  private static final byte[] ISO_POINT = hex("0101000000000000000000f03f0000000000000040");
  // SRID=4326;POINT Z(1 2 3)
  private static final byte[] EWKB_POINT_Z =
      hex("01010000a0e6100000000000000000f03f00000000000000400000000000000840");
  // POINT Z(1 2 3)
  private static final byte[] ISO_POINT_Z =
      hex("01e9030000000000000000f03f00000000000000400000000000000840");

  @Mock GlobalConfig config;
  @Mock IcebergConfig icebergConfig;
  @Mock DebeziumConfig debeziumConfig;
  @TempDir Path tempDir;

  @BeforeAll
  static void setup() {
    JsonEventConverter.initializeStaticSerdes();
  }

  @BeforeEach
  void setUp() {
    lenient().when(config.iceberg()).thenReturn(icebergConfig);
    lenient().when(config.debezium()).thenReturn(debeziumConfig);
    lenient().when(debeziumConfig.isEventFlatteningEnabled()).thenReturn(true);
    lenient().when(icebergConfig.geospatialTypes()).thenReturn(true);
    lenient().when(icebergConfig.geometryCrs()).thenReturn(Types.GeometryType.DEFAULT_CRS);
    lenient().when(icebergConfig.cdcOpField()).thenReturn("__op");
  }

  @Test
  void testDisabledKeepsStructMapping() {
    lenient().when(icebergConfig.geospatialTypes()).thenReturn(false);
    assertEquals(
        Types.StructType.of(
            Types.NestedField.optional(2, "wkb", Types.BinaryType.get()),
            Types.NestedField.optional(3, "srid", Types.IntegerType.get())),
        geoStructEvent().icebergSchema(false).findType("g"));
  }

  @Test
  void testPostgresPointKeepsStructMapping() {
    Schema pointSchema = Point.builder().optional().build();
    Schema valueSchema = SchemaBuilder.struct().field("p", pointSchema).build();
    Struct value = new Struct(valueSchema).put("p", Point.createValue(pointSchema, 350, 120));
    var pointType = structEvent(value).icebergSchema(false).findType("p").asStructType();
    assertEquals(
        List.of("x", "y", "wkb", "srid"),
        pointType.fields().stream().map(Types.NestedField::name).toList());
  }

  @Test
  void testStructEventConverter() throws IOException {
    assertConverted(geoStructEvent());
  }

  @Test
  void testJsonEventConverter() throws IOException {
    JsonEventConverter converter =
        jsonEvent(
            String.join(
                ",",
                jsonGeoSchema(GEOMETRY_LOGICAL_NAME, "g"),
                jsonGeoSchema(GEOGRAPHY_LOGICAL_NAME, "geog"),
                jsonGeoSchema(GEOMETRY_LOGICAL_NAME, "g_null")),
            """
            "g":{"wkb":"%s","srid":4326},"geog":{"wkb":"%s","srid":4326},"g_null":null"""
                .formatted(base64(EWKB_POINT_Z), base64(ISO_POINT)));
    assertConverted(converter);
  }

  @Test
  void testGeometryArray() {
    JsonEventConverter json =
        jsonEvent(
            """
            {"type":"array","optional":true,"field":"ga","items":%s}"""
                .formatted(jsonGeoSchema(GEOMETRY_LOGICAL_NAME, null)),
            """
            "ga":[{"wkb":"%s","srid":4326},null]"""
                .formatted(base64(EWKB_POINT_Z)));
    var jsonSchema = json.icebergSchema(false);
    assertEquals(Types.GeometryType.crs84(), jsonSchema.findType("ga").asListType().elementType());
    assertEquals(
        Arrays.asList(ByteBuffer.wrap(ISO_POINT_Z), null), json.convert(jsonSchema).getField("ga"));

    Schema geomSchema = geoSchema(GEOMETRY_LOGICAL_NAME);
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("ga", SchemaBuilder.array(geomSchema).build())
            .field("__op", OPTIONAL_STRING_SCHEMA)
            .build();
    StructEventConverter struct =
        structEvent(
            new Struct(valueSchema)
                .put("ga", List.of(new Struct(geomSchema).put("wkb", EWKB_POINT_Z))));
    var structSchema = struct.icebergSchema(false);
    assertEquals(
        Types.GeometryType.crs84(), structSchema.findType("ga").asListType().elementType());
    assertEquals(
        List.of(ByteBuffer.wrap(ISO_POINT_Z)), struct.convert(structSchema).getField("ga"));
  }

  @Test
  void testInvalidArrayElementNamesTheColumn() {
    byte[] invalid = hex("0101");
    JsonEventConverter json =
        jsonEvent(
            """
            {"type":"array","optional":true,"field":"ga","items":%s}"""
                .formatted(jsonGeoSchema(GEOMETRY_LOGICAL_NAME, null)),
            "\"ga\":[{\"wkb\":\"%s\"}]".formatted(base64(invalid)));
    var jsonSchema = json.icebergSchema(false);
    assertTrue(
        assertThrows(DebeziumException.class, () -> json.convert(jsonSchema))
            .getMessage()
            .startsWith("Invalid geospatial value, field: ga[]:"));

    Schema geomSchema = geoSchema(GEOMETRY_LOGICAL_NAME);
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("ga", SchemaBuilder.array(geomSchema).build())
            .field("__op", OPTIONAL_STRING_SCHEMA)
            .build();
    StructEventConverter struct =
        structEvent(
            new Struct(valueSchema).put("ga", List.of(new Struct(geomSchema).put("wkb", invalid))));
    var structSchema = struct.icebergSchema(false);
    assertTrue(
        assertThrows(DebeziumException.class, () -> struct.convert(structSchema))
            .getMessage()
            .startsWith("Invalid geospatial value, field: ga[]:"));
  }

  @Test
  void testGeometryPrimaryKeyIsNotSorted() {
    Schema geomSchema =
        SchemaBuilder.struct().name(GEOMETRY_LOGICAL_NAME).field("wkb", BYTES_SCHEMA).build();
    Schema keySchema =
        SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("g", geomSchema).build();
    Schema valueSchema =
        SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("g", geomSchema).build();
    Struct g = new Struct(geomSchema).put("wkb", ISO_POINT);
    StructEventConverter struct =
        new StructEventConverter(
            EventFactory.createMockChangeEvent(
                new Struct(keySchema).put("id", 1).put("g", g),
                new Struct(valueSchema).put("id", 1).put("g", g)),
            config);
    assertGeometryKeyNotSorted(struct);

    String fields =
        """
        [{"type":"int32","optional":false,"field":"id"},%s]"""
            .formatted(jsonGeoSchema(GEOMETRY_LOGICAL_NAME, "g"));
    String payload = "{\"id\":1,\"g\":{\"wkb\":\"%s\"}}".formatted(base64(ISO_POINT));
    String json = "{\"schema\":{\"type\":\"struct\",\"fields\":%s},\"payload\":%s}";
    assertGeometryKeyNotSorted(
        new JsonEventConverter(
            "test", json.formatted(fields, payload), json.formatted(fields, payload), config));
  }

  private static void assertGeometryKeyNotSorted(EventConverter event) {
    org.apache.iceberg.Schema schema = event.icebergSchema(true);
    assertEquals(java.util.Set.of("id", "g"), schema.identifierFieldNames());
    assertEquals(
        List.of("id"),
        event.sortOrder(schema).fields().stream()
            .map(f -> schema.findColumnName(f.sourceId()))
            .toList());
  }

  /** Checks the converted geometry/geography values and writes them to Parquet. */
  private void assertConverted(EventConverter event) throws IOException {
    org.apache.iceberg.Schema schema = event.icebergSchema(false);
    assertEquals(Types.GeometryType.crs84(), schema.findType("g"));
    assertEquals(Types.GeographyType.crs84(), schema.findType("geog"));
    Record record = event.convert(schema);
    assertEquals(ByteBuffer.wrap(ISO_POINT_Z), record.getField("g"));
    assertEquals(ByteBuffer.wrap(ISO_POINT), record.getField("geog"));
    assertNull(record.getField("g_null"));

    File file = tempDir.resolve("data-" + System.nanoTime() + ".parquet").toFile();
    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      appender.add(record);
    }
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      Record row = Lists.newArrayList(reader).get(0);
      assertEquals(record.getField("g"), row.getField("g"));
      assertEquals(record.getField("geog"), row.getField("geog"));
      assertNull(row.getField("g_null"));
    }
  }

  private StructEventConverter geoStructEvent() {
    Schema geomSchema = geoSchema(GEOMETRY_LOGICAL_NAME);
    Schema geogSchema = geoSchema(GEOGRAPHY_LOGICAL_NAME);
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("g", geomSchema)
            .field("geog", geogSchema)
            .field("g_null", geomSchema)
            .field("__op", OPTIONAL_STRING_SCHEMA)
            .build();
    return structEvent(
        new Struct(valueSchema)
            .put("g", new Struct(geomSchema).put("wkb", EWKB_POINT_Z).put("srid", 4326))
            .put("geog", new Struct(geogSchema).put("wkb", ISO_POINT).put("srid", 4326)));
  }

  private StructEventConverter structEvent(Struct value) {
    return new StructEventConverter(EventFactory.createMockChangeEvent(null, value), config);
  }

  private JsonEventConverter jsonEvent(String fieldSchemas, String payload) {
    String event =
        """
        {"schema":{"type":"struct","optional":false,"fields":[%s,
         {"type":"string","optional":true,"field":"__op"}]},"payload":{%s,"__op":"r"}}"""
            .formatted(fieldSchemas, payload);
    return new JsonEventConverter("test", event, null, config);
  }

  private static Schema geoSchema(String logicalName) {
    return SchemaBuilder.struct()
        .name(logicalName)
        .field("wkb", BYTES_SCHEMA)
        .field("srid", OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();
  }

  private static String jsonGeoSchema(String logicalName, String field) {
    return """
        {"type":"struct","name":"%s","optional":true%s,"fields":[
         {"type":"bytes","optional":false,"field":"wkb"},
         {"type":"int32","optional":true,"field":"srid"}]}"""
        .formatted(logicalName, field == null ? "" : ",\"field\":\"" + field + "\"");
  }

  private static byte[] hex(String hex) {
    return HexFormat.of().parseHex(hex);
  }

  private static String base64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }
}
