package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.DebeziumException;
import io.debezium.converters.iceberg.geospatial.GeospatialTypes;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.ZonedTimestamp;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

class StructSchemaConverterTest {

  private final IcebergConverterConfig defaultConfig = ConnectIcebergConverterConfig.defaults();

  @Test
  void testSimpleSchemaConversionWithPrimaryKey() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("SimpleRecord")
            .field("pk_id", Schema.INT32_SCHEMA)
            .field("data_field", Schema.OPTIONAL_STRING_SCHEMA)
            .field("timestamp_field", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    Schema keySchema =
        SchemaBuilder.struct().name("SimpleRecordKey").field("pk_id", Schema.INT32_SCHEMA).build();

    StructSchemaConverter converter =
        new StructSchemaConverter(valueSchema, keySchema, defaultConfig);

    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();

    assertNotNull(icebergSchema, "Iceberg schema should not be null");
    assertEquals(3, icebergSchema.columns().size(), "Should have 3 top-level fields");

    assertEquals(
        Set.of(1), icebergSchema.identifierFieldIds(), "Field 'pk_id' should be the identifier");
    Types.NestedField pkField = icebergSchema.findField("pk_id");
    assertNotNull(pkField, "Field 'pk_id' not found");
    assertEquals(1, pkField.fieldId(), "Field ID mismatch for 'pk_id'");
    assertFalse(pkField.isOptional(), "Identifier field 'pk_id' should be required");
    assertEquals(Types.IntegerType.get(), pkField.type(), "Type mismatch for 'pk_id'");

    Types.NestedField dataField = icebergSchema.findField("data_field");
    assertNotNull(dataField, "Field 'data_field' not found");
    assertEquals(2, dataField.fieldId(), "Field ID mismatch for 'data_field'");
    assertTrue(dataField.isOptional(), "Optionality mismatch for 'data_field'");
    assertEquals(Types.StringType.get(), dataField.type(), "Type mismatch for 'data_field'");

    Types.NestedField timestampField = icebergSchema.findField("timestamp_field");
    assertNotNull(timestampField, "Field 'timestamp_field' not found");
    assertEquals(3, timestampField.fieldId(), "Field ID mismatch for 'timestamp_field'");
    assertTrue(timestampField.isOptional(), "Optionality mismatch for 'timestamp_field'");
    assertEquals(
        Types.LongType.get(), timestampField.type(), "Type mismatch for 'timestamp_field'");
  }

  @Test
  void testComplexSchemaConversion() {
    Schema nestedStructSchema =
        SchemaBuilder.struct()
            .name("NestedRecord")
            .field("nested_id", Schema.INT32_SCHEMA)
            .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .name("ComplexRecord")
            .field("id", Schema.INT32_SCHEMA)
            .field("int8_field", Schema.OPTIONAL_INT8_SCHEMA)
            .field("int16_field", Schema.OPTIONAL_INT16_SCHEMA)
            .field("int32_field", Schema.OPTIONAL_INT32_SCHEMA)
            .field("int64_field", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float32_field", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("float64_field", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("boolean_field", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("string_field", Schema.OPTIONAL_STRING_SCHEMA)
            .field("bytes_field", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("date_field", Date.builder().optional().build())
            .field("time_micros_field", MicroTime.builder().optional().build())
            .field("timestamp_micros_field", MicroTimestamp.builder().optional().build())
            .field("nano_time_field", NanoTime.builder().optional().build())
            .field("nano_ts_field", NanoTimestamp.builder().optional().build())
            .field(
                "decimal_field",
                Decimal.builder(5).optional().parameter("connect.decimal.precision", "10").build())
            .field("uuid_field", Uuid.builder().optional().build())
            .field("zoned_timestamp_field", ZonedTimestamp.builder().optional().build())
            .field("iso_date_field", IsoDate.builder().optional().build())
            .field("iso_timestamp_field", IsoTimestamp.builder().optional().build())
            .field("__ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("nested_record", nestedStructSchema)
            .field("array_of_strings", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(
                "map_string_int",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .field("array_of_structs", SchemaBuilder.array(nestedStructSchema).optional().build())
            .field(
                "map_string_struct",
                SchemaBuilder.map(Schema.STRING_SCHEMA, nestedStructSchema).optional().build())
            .build();

    Schema keySchema =
        SchemaBuilder.struct().name("ComplexRecordKey").field("id", Schema.INT32_SCHEMA).build();

    StructSchemaConverter converter =
        new StructSchemaConverter(valueSchema, keySchema, defaultConfig);

    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();

    assertNotNull(icebergSchema);
    assertEquals(26, icebergSchema.columns().size(), "Should have 26 top-level fields");

    assertEquals(
        Set.of(1), icebergSchema.identifierFieldIds(), "Field 'id' should be the identifier");
    Types.NestedField idField = icebergSchema.findField("id");
    assertNotNull(idField);
    assertEquals(1, idField.fieldId());
    assertFalse(idField.isOptional(), "Identifier field 'id' should not be optional");
    assertEquals(Types.IntegerType.get(), idField.type(), "Identifier field 'id' type mismatch");

    assertField(icebergSchema, "int8_field", 2, Types.IntegerType.get());
    assertField(icebergSchema, "int16_field", 3, Types.IntegerType.get());
    assertField(icebergSchema, "int32_field", 4, Types.IntegerType.get());
    assertField(icebergSchema, "int64_field", 5, Types.LongType.get());
    assertField(icebergSchema, "float32_field", 6, Types.FloatType.get());
    assertField(icebergSchema, "float64_field", 7, Types.DoubleType.get());
    assertField(icebergSchema, "boolean_field", 8, Types.BooleanType.get());
    assertField(icebergSchema, "string_field", 9, Types.StringType.get());
    assertField(icebergSchema, "bytes_field", 10, Types.BinaryType.get());

    assertField(icebergSchema, "date_field", 11, Types.DateType.get());
    assertField(icebergSchema, "time_micros_field", 12, Types.LongType.get());
    assertField(icebergSchema, "timestamp_micros_field", 13, Types.TimestampType.withoutZone());
    assertField(icebergSchema, "nano_time_field", 14, Types.LongType.get());
    assertField(icebergSchema, "nano_ts_field", 15, Types.TimestampType.withoutZone());
    assertField(icebergSchema, "decimal_field", 16, Types.DecimalType.of(10, 5));
    assertField(icebergSchema, "uuid_field", 17, Types.UUIDType.get());
    assertField(icebergSchema, "zoned_timestamp_field", 18, Types.TimestampType.withZone());
    assertField(icebergSchema, "iso_date_field", 19, Types.DateType.get());
    assertField(icebergSchema, "iso_timestamp_field", 20, Types.TimestampType.withoutZone());
    assertField(icebergSchema, "__ts_ms", 21, Types.TimestampType.withZone());

    Types.NestedField nestedRecordField =
        assertField(
            icebergSchema,
            "nested_record",
            22,
            Types.StructType.of(
                Types.NestedField.required(23, "nested_id", Types.IntegerType.get()),
                Types.NestedField.optional(24, "nested_data", Types.StringType.get())));
    assertTrue(nestedRecordField.type().isStructType());
    Types.StructType nestedStructType = nestedRecordField.type().asStructType();
    assertEquals(2, nestedStructType.fields().size());
    assertTrue(nestedStructType.field("nested_id").isOptional());
    assertTrue(nestedStructType.field("nested_data").isOptional());

    Types.NestedField arrayStringsField =
        assertField(
            icebergSchema,
            "array_of_strings",
            25,
            Types.ListType.ofOptional(29, Types.StringType.get()));
    assertEquals(Types.StringType.get(), arrayStringsField.type().asListType().elementType());

    Types.NestedField mapStringIntField =
        assertField(
            icebergSchema,
            "map_string_int",
            28,
            Types.MapType.ofOptional(30, 31, Types.StringType.get(), Types.IntegerType.get()));
    assertEquals(Types.StringType.get(), mapStringIntField.type().asMapType().keyType());
    assertEquals(Types.IntegerType.get(), mapStringIntField.type().asMapType().valueType());

    Types.NestedField arrayStructsField =
        assertField(
            icebergSchema, "array_of_structs", 33, Types.ListType.ofOptional(32, nestedStructType));
    assertTrue(arrayStructsField.type().isListType());

    Types.NestedField mapStringStructField =
        assertField(
            icebergSchema,
            "map_string_struct",
            38,
            Types.MapType.ofOptional(33, 34, Types.StringType.get(), nestedStructType));
    assertTrue(mapStringStructField.type().isMapType());
  }

  @Test
  void testPreserveRequiredProperty() {
    IcebergConverterConfig config =
        ConnectIcebergConverterConfig.fromMap(Map.of("preserve.required.property", "true"));
    org.apache.iceberg.Schema icebergSchema = setupRequiredPropertyTest(config);

    assertTrue(icebergSchema.findField("pk_id").isRequired());
    assertTrue(icebergSchema.findField("f_required").isRequired());
    assertTrue(icebergSchema.findField("f_optional").isOptional());
    assertTrue(icebergSchema.findField("f_array").isRequired());
    assertTrue(icebergSchema.findField("f_array_opt").isOptional());
    assertTrue(icebergSchema.findField("f_map").isRequired());
    assertTrue(icebergSchema.findField("f_map_opt").isOptional());
    assertTrue(icebergSchema.findField("f_struct").isRequired());
    assertTrue(icebergSchema.findField("f_struct_opt").isOptional());
  }

  @Test
  void testDefaultRequiredPropertyConversion() {
    org.apache.iceberg.Schema icebergSchema = setupRequiredPropertyTest(defaultConfig);

    assertTrue(icebergSchema.findField("pk_id").isRequired());
    assertTrue(icebergSchema.findField("f_required").isOptional());
    assertTrue(icebergSchema.findField("f_optional").isOptional());
    assertTrue(icebergSchema.findField("f_array").isOptional());
    assertTrue(icebergSchema.findField("f_array_opt").isOptional());
    assertTrue(icebergSchema.findField("f_map").isOptional());
    assertTrue(icebergSchema.findField("f_map_opt").isOptional());
    assertTrue(icebergSchema.findField("f_struct").isOptional());
    assertTrue(icebergSchema.findField("f_struct_opt").isOptional());
  }

  private org.apache.iceberg.Schema setupRequiredPropertyTest(IcebergConverterConfig config) {
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("SimpleRecord")
            .field("pk_id", Schema.INT32_SCHEMA)
            .field("f_required", Schema.INT64_SCHEMA)
            .field("f_optional", Schema.OPTIONAL_INT64_SCHEMA)
            .field("f_array", SchemaBuilder.array(Schema.INT64_SCHEMA))
            .field("f_array_opt", SchemaBuilder.array(Schema.INT64_SCHEMA).optional())
            .field("f_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
            .field(
                "f_map_opt",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional())
            .field(
                "f_struct",
                SchemaBuilder.struct()
                    .field("nested_id", Schema.INT32_SCHEMA)
                    .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA))
            .field(
                "f_struct_opt",
                SchemaBuilder.struct()
                    .field("nested_id", Schema.INT32_SCHEMA)
                    .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional())
            .build();
    Schema keySchema =
        SchemaBuilder.struct().name("SimpleRecordKey").field("pk_id", Schema.INT32_SCHEMA).build();

    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);
    return converter.icebergSchema();
  }

  @Test
  void testConnectTimestampField() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("SimpleRecord")
            .field("id", Schema.INT32_SCHEMA)
            .field(
                "connect_timestamp_field",
                org.apache.kafka.connect.data.Timestamp.builder().optional().build())
            .build();

    Schema keySchema =
        SchemaBuilder.struct().name("SimpleRecordKey").field("id", Schema.INT32_SCHEMA).build();

    StructSchemaConverter converter =
        new StructSchemaConverter(valueSchema, keySchema, defaultConfig);
    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();

    assertEquals(2, icebergSchema.columns().size());
    assertEquals(Set.of(1), icebergSchema.identifierFieldIds());
    assertField(icebergSchema, "connect_timestamp_field", 2, Types.TimestampType.withoutZone());
  }

  @Test
  void testSortOrderSkipsKeyFieldsNotInValueSchemaAndGeospatialKeys() {
    Schema geomSchema =
        SchemaBuilder.struct()
            .name(GeospatialTypes.GEOMETRY_LOGICAL_NAME)
            .field("wkb", Schema.BYTES_SCHEMA)
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("TournamentRecord")
            .field("id", Schema.INT32_SCHEMA)
            .field("g", geomSchema)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema keySchema =
        SchemaBuilder.struct()
            .name("TournamentRecordKey")
            .field("id", Schema.INT32_SCHEMA)
            .field("g", geomSchema)
            .field("tenant_db", Schema.STRING_SCHEMA)
            .build();

    IcebergConverterConfig geoConfig =
        ConnectIcebergConverterConfig.fromMap(Map.of("geospatial.types.enabled", "true"));
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, geoConfig);
    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();

    assertNull(icebergSchema.findField("tenant_db"));
    assertEquals(Set.of("id", "g"), icebergSchema.identifierFieldNames());

    SortOrder sortOrder = converter.sortOrder(icebergSchema);
    assertEquals(1, sortOrder.fields().size());
    assertEquals("id", icebergSchema.findColumnName(sortOrder.fields().get(0).sourceId()));
  }

  @Test
  void testExcludedColumnsAndNestedAsVariant() {
    Schema nested = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).optional().build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("secret", Schema.OPTIONAL_STRING_SCHEMA)
            .field("payload", nested)
            .build();
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

    IcebergConverterConfig config =
        ConnectIcebergConverterConfig.fromMap(
            Map.of("excluded.columns", "secret", "nested.as.variant", "true"));
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);
    org.apache.iceberg.Schema schema = converter.icebergSchema(false);

    assertEquals(2, schema.columns().size());
    assertNull(schema.findField("secret"));
    assertEquals(Types.VariantType.get(), schema.findType("payload"));
  }

  @Test
  void testUnflattenedEventWithIdentifierFieldsThrowsNeutralMessage() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema nestedAfter = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    Schema keySchema = SchemaBuilder.struct().field("after", nestedAfter).build();

    IcebergConverterConfig config =
        ConnectIcebergConverterConfig.fromMap(Map.of("event.flattening.enabled", "false"));
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);

    DebeziumException ex =
        assertThrows(DebeziumException.class, () -> converter.icebergSchema(true));
    assertTrue(ex.getMessage().contains("create.identifier.fields=false"));
  }

  @Test
  void testNullOrNonStructValueSchemaThrows() {
    StructSchemaConverter nullConverter = new StructSchemaConverter(null, null, defaultConfig);
    assertThrows(DebeziumException.class, nullConverter::icebergSchema);

    StructSchemaConverter primitiveConverter =
        new StructSchemaConverter(Schema.STRING_SCHEMA, null, defaultConfig);
    assertThrows(DebeziumException.class, primitiveConverter::icebergSchema);
  }

  @Test
  void testEqualsAndHashCodeBasedOnSchemas() {
    Schema v1 = SchemaBuilder.struct().name("A").field("id", Schema.INT32_SCHEMA).build();
    Schema v2 = SchemaBuilder.struct().name("B").field("id", Schema.INT64_SCHEMA).build();
    Schema k1 = SchemaBuilder.struct().name("AK").field("id", Schema.INT32_SCHEMA).build();

    StructSchemaConverter c1 = new StructSchemaConverter(v1, k1, defaultConfig);
    StructSchemaConverter c2 = new StructSchemaConverter(v1, k1, defaultConfig);
    StructSchemaConverter c3 = new StructSchemaConverter(v2, k1, defaultConfig);

    assertEquals(c1, c2);
    assertEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c3);
  }

  private Types.NestedField assertField(
      org.apache.iceberg.Schema schema,
      String name,
      int expectedId,
      org.apache.iceberg.types.Type expectedType) {
    Types.NestedField field = schema.findField(name);
    assertNotNull(field, "Field '" + name + "' not found");
    assertEquals(expectedId, field.fieldId(), "Field ID mismatch for '" + name + "'");
    assertEquals(expectedType.typeId(), field.type().typeId(), "Type mismatch for '" + name + "'");
    if (expectedType.isStructType()) {
      assertEquals(
          expectedType.asStructType().fields().size(),
          field.type().asStructType().fields().size(),
          "Struct field count mismatch for '" + name + "'");
    } else if (expectedType.isListType()) {
      assertEquals(
          expectedType.asListType().elementType().typeId(),
          field.type().asListType().elementType().typeId(),
          "List element type mismatch for '" + name + "'");
    } else if (expectedType.isMapType()) {
      assertEquals(
          expectedType.asMapType().keyType().typeId(),
          field.type().asMapType().keyType().typeId(),
          "Map key type mismatch for '" + name + "'");
      assertEquals(
          expectedType.asMapType().valueType().typeId(),
          field.type().asMapType().valueType().typeId(),
          "Map value type mismatch for '" + name + "'");
    }
    return field;
  }
}
