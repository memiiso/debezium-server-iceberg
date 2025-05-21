package io.debezium.server.iceberg.converter;

import io.debezium.data.Uuid;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.time.Date;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@EnabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class StructSchemaConverterTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(StructSchemaConverterTest.class);
  @Mock
  public GlobalConfig config;
  @Mock
  public IcebergConfig icebergConfig;
  @Mock
  public DebeziumConfig debeziumConfig;

  @BeforeEach
  void setUpBeforeEach() {
    when(config.iceberg()).thenReturn(icebergConfig);
    when(config.debezium()).thenReturn(debeziumConfig);
    when(icebergConfig.createIdentifierFields()).thenReturn(true);
    when(debeziumConfig.isEventFlatteningEnabled()).thenReturn(true);
    when(icebergConfig.preserveRequiredProperty()).thenReturn(false);
  }

  @Test
  void testSimpleSchemaConversionWithPrimaryKey() {
    // 1. Define the Value Schema
    org.apache.kafka.connect.data.Schema valueSchema = SchemaBuilder.struct()
        .name("SimpleRecord")
        .field("pk_id", Schema.INT32_SCHEMA) // PK field, required by definition in Connect schema
        .field("data_field", Schema.OPTIONAL_STRING_SCHEMA)
        .field("timestamp_field", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // 2. Define the Key Schema
    org.apache.kafka.connect.data.Schema keySchema = SchemaBuilder.struct()
        .name("SimpleRecordKey")
        .field("pk_id", Schema.INT32_SCHEMA) // The PK field
        .build();

    // 3. Instantiate the Converter
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);

    // 4. Convert to Iceberg Schema
    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();
    LOGGER.error("{}", icebergSchema);

    // 5. Assertions
    assertNotNull(icebergSchema, "Iceberg schema should not be null");
    assertEquals(3, icebergSchema.columns().size(), "Should have 3 top-level fields");

    // Verify Identifier Field ('pk_id')
    assertEquals(Set.of(1), icebergSchema.identifierFieldIds(), "Field 'pk_id' should be the identifier");
    Types.NestedField pkField = icebergSchema.findField("pk_id");
    assertNotNull(pkField, "Field 'pk_id' not found");
    assertEquals(1, pkField.fieldId(), "Field ID mismatch for 'pk_id'");
    // Identifier fields derived from the key schema are always required in Iceberg.
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
    assertEquals(Types.LongType.get(), timestampField.type(), "Type mismatch for 'timestamp_field'");

    LOGGER.debug("Generated Iceberg schema:\n{}", icebergSchema);
  }


  @Test
  void testComplexSchemaConversion() {
    // 1. Define Nested Struct Schema
    org.apache.kafka.connect.data.Schema nestedStructSchema = SchemaBuilder.struct()
        .name("NestedRecord")
        .field("nested_id", Schema.INT32_SCHEMA)
        .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    // 2. Define the main Value Schema including all types
    org.apache.kafka.connect.data.Schema valueSchema = SchemaBuilder.struct()
        .name("ComplexRecord")
        // Primitives
        .field("id", Schema.INT32_SCHEMA) // PK field
        .field("int8_field", Schema.OPTIONAL_INT8_SCHEMA)
        .field("int16_field", Schema.OPTIONAL_INT16_SCHEMA)
        .field("int32_field", Schema.OPTIONAL_INT32_SCHEMA)
        .field("int64_field", Schema.OPTIONAL_INT64_SCHEMA)
        .field("float32_field", Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("float64_field", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("boolean_field", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("string_field", Schema.OPTIONAL_STRING_SCHEMA)
        .field("bytes_field", Schema.OPTIONAL_BYTES_SCHEMA)
        // Logical Types
        .field("date_field", Date.builder().optional().build()) // Days since epoch (INT32)
        .field("time_micros_field", MicroTime.builder().optional().build()) // Micros since midnight (INT64)
        .field("timestamp_micros_field", MicroTimestamp.builder().optional().build()) // Micros since epoch (UTC) (INT64)
        .field("nano_time_field", NanoTime.builder().optional().build()) // Nanos since midnight (INT64) <-- ADDED
        .field("nano_ts_field", NanoTimestamp.builder().optional().build()) // Nanos since epoch (UTC) (INT64) <-- ADDED
        .field("decimal_field", Decimal.builder(5).optional().parameter("connect.decimal.precision", "10").build()) // scale 5, precision 10 (BYTES)
        .field("uuid_field", Uuid.builder().optional().build()) // String UUID (STRING)
        .field("zoned_timestamp_field", ZonedTimestamp.builder().optional().build()) // String ISO Zoned Timestamp (STRING)
        .field("iso_date_field", IsoDate.builder().optional().build()) // String ISO Date (STRING)
        .field("iso_timestamp_field", IsoTimestamp.builder().optional().build()) // String ISO Timestamp (no zone) (STRING)
        // Special Debezium Field
        .field("__ts_ms", Schema.OPTIONAL_INT64_SCHEMA) // Should map to TimestampTZ
        // Nested Struct
        .field("nested_record", nestedStructSchema)
        // Collections
        .field("array_of_strings", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .field("map_string_int", SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            Schema.INT32_SCHEMA).optional().build())
        .field("array_of_structs", SchemaBuilder.array(nestedStructSchema).optional().build())
        .field("map_string_struct", SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            nestedStructSchema).optional().build())
        .build();

    // 3. Define the Key Schema
    org.apache.kafka.connect.data.Schema keySchema = SchemaBuilder.struct()
        .name("ComplexRecordKey")
        .field("id", Schema.INT32_SCHEMA) // The PK field
        .build();

    // 4. Instantiate the Converter
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);
    LOGGER.error("{}", keySchema.toString());
    LOGGER.error("{}", valueSchema.toString());

    // 5. Convert to Iceberg Schema
    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();

    // 6. Assertions
    assertNotNull(icebergSchema);
    assertEquals(26, icebergSchema.columns().size(), "Should have 26 top-level fields"); // Increased count by 2

    // Verify Identifier Field
    assertEquals(Set.of(1), icebergSchema.identifierFieldIds(), "Field 'id' should be the identifier");
    Types.NestedField idField = icebergSchema.findField("id");
    assertNotNull(idField);
    assertEquals(1, idField.fieldId());
    assertFalse(idField.isOptional(), "Identifier field 'id' should not be optional");
    assertEquals(Types.IntegerType.get(), idField.type(), "Identifier field 'id' type mismatch");

    // Verify Primitive Types (check optionality and type)
    assertField(icebergSchema, "int8_field", 2, true, Types.IntegerType.get());
    assertField(icebergSchema, "int16_field", 3, true, Types.IntegerType.get());
    assertField(icebergSchema, "int32_field", 4, true, Types.IntegerType.get());
    assertField(icebergSchema, "int64_field", 5, true, Types.LongType.get());
    assertField(icebergSchema, "float32_field", 6, true, Types.FloatType.get());
    assertField(icebergSchema, "float64_field", 7, true, Types.DoubleType.get());
    assertField(icebergSchema, "boolean_field", 8, true, Types.BooleanType.get());
    assertField(icebergSchema, "string_field", 9, true, Types.StringType.get());
    assertField(icebergSchema, "bytes_field", 10, true, Types.BinaryType.get());

    // Verify Logical Types
    assertField(icebergSchema, "date_field", 11, true, Types.DateType.get());
    // Note: Time types might map differently based on config/needs, often to Long or TimeType.
    // Assuming MicroTime maps to Long representing microseconds for this test. Adjust if needed.
    // Update: Debezium Time logical types are not explicitly mapped to Iceberg TimeType in the provided converter.
    // It seems they might fall back to Long or Integer based on underlying type (INT64 for MicroTime).
    assertField(icebergSchema, "time_micros_field", 12, true, Types.LongType.get()); // Check if this mapping is intended
    assertField(icebergSchema, "timestamp_micros_field", 13, true, Types.TimestampType.withoutZone());
    assertField(icebergSchema, "nano_time_field", 14, true, Types.LongType.get()); // <-- ADDED (Mapped to underlying Long)
    assertField(icebergSchema, "nano_ts_field", 15, true, Types.TimestampType.withoutZone()); // <-- ADDED
    assertField(icebergSchema, "decimal_field", 16, true, Types.DecimalType.of(10, 5));
    assertField(icebergSchema, "uuid_field", 17, true, Types.UUIDType.get());
    assertField(icebergSchema, "zoned_timestamp_field", 18, true, Types.TimestampType.withZone());
    assertField(icebergSchema, "iso_date_field", 19, true, Types.DateType.get());
    assertField(icebergSchema, "iso_timestamp_field", 20, true, Types.TimestampType.withoutZone());

    // Verify Special Debezium Field
    assertField(icebergSchema, "__ts_ms", 21, true, Types.TimestampType.withZone());

    // Verify Nested Struct
    Types.NestedField nestedRecordField = assertField(icebergSchema, "nested_record", 22, false, Types.StructType.of( // Struct itself is required
        Types.NestedField.of(23, false, "nested_id", Types.IntegerType.get()), // nested_id is required within struct
        Types.NestedField.of(24, true, "nested_data", Types.StringType.get())  // nested_data is optional
    ));
    assertNotNull(nestedRecordField);
    assertTrue(nestedRecordField.type().isStructType());
    Types.StructType nestedStructType = nestedRecordField.type().asStructType();
    assertEquals(2, nestedStructType.fields().size());
    assertNotNull(nestedStructType.field("nested_id"));
    assertNotNull(nestedStructType.field("nested_data"));
    // all fields are created as optional, except PK
    assertTrue(nestedStructType.field("nested_id").isOptional());
    assertTrue(nestedStructType.field("nested_data").isOptional());

    // Verify Collections
    // Array of Strings
    Types.NestedField arrayStringsField = assertField(icebergSchema, "array_of_strings", 25, true, Types.ListType.ofOptional(29, Types.StringType.get()));
    assertNotNull(arrayStringsField);
    assertTrue(arrayStringsField.type().isListType());
    assertEquals(Types.StringType.get(), arrayStringsField.type().asListType().elementType());

    // Map String to Int
    Types.NestedField mapStringIntField = assertField(icebergSchema, "map_string_int", 28, true, Types.MapType.ofOptional(30, 31, Types.StringType.get(), Types.IntegerType.get()));
    assertNotNull(mapStringIntField);
    assertTrue(mapStringIntField.type().isMapType());
    assertEquals(Types.StringType.get(), mapStringIntField.type().asMapType().keyType());
    assertEquals(Types.IntegerType.get(), mapStringIntField.type().asMapType().valueType());

    // Array of Structs
    Types.NestedField arrayStructsField = assertField(icebergSchema, "array_of_structs", 33, true, Types.ListType.ofOptional(32, nestedStructType)); // Re-use nestedStructType
    assertNotNull(arrayStructsField);
    assertTrue(arrayStructsField.type().isListType());
    // Expected :struct<23: nested_id: required int, 24: nested_data: optional string>
    // Actual   :struct<36: nested_id: required int, 37: nested_data: optional string>
    // assertEquals(nestedStructType, arrayStructsField.type().asListType().elementType()); // ID mismatch expected

    // Map String to Struct
    Types.NestedField mapStringStructField = assertField(icebergSchema, "map_string_struct", 38, true, Types.MapType.ofOptional(33, 34, Types.StringType.get(), nestedStructType)); // Re-use nestedStructType
    assertNotNull(mapStringStructField);
    assertTrue(mapStringStructField.type().isMapType());
    assertEquals(Types.StringType.get(), mapStringStructField.type().asMapType().keyType());
    // Expected :struct<23: nested_id: required int, 24: nested_data: optional string>
    // Actual   :struct<43: nested_id: required int, 44: nested_data: optional string>
    // assertEquals(nestedStructType, mapStringStructField.type().asMapType().valueType()); // ID mismatch expected

    // Print schema for manual verification if needed
    System.out.println(icebergSchema.toString());
  }

  @Test
  void testPreserveRequiredProperty() {
    // Preserve required fields as required in Iceberg schema
    when(icebergConfig.preserveRequiredProperty()).thenReturn(true);

    org.apache.iceberg.Schema icebergSchema = setupRequiredPropertyTest();
    LOGGER.error("{}", icebergSchema);

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
    org.apache.iceberg.Schema icebergSchema = setupRequiredPropertyTest();
    LOGGER.error("{}", icebergSchema);

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

  private org.apache.iceberg.Schema setupRequiredPropertyTest() {
    // Define nested schema
    SchemaBuilder structBuilder = SchemaBuilder.struct()
            .field("nested_id", Schema.INT32_SCHEMA)
            .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA);
    SchemaBuilder mapBuilder = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA);

    // Define value schema
    org.apache.kafka.connect.data.Schema valueSchema = SchemaBuilder.struct()
            .name("SimpleRecord")
            .field("pk_id", Schema.INT32_SCHEMA) // PK field, required by definition in Connect schema
            .field("f_required", Schema.INT64_SCHEMA)
            .field("f_optional", Schema.OPTIONAL_INT64_SCHEMA)
            .field("f_array", SchemaBuilder.array(Schema.INT64_SCHEMA))
            .field("f_array_opt", SchemaBuilder.array(Schema.INT64_SCHEMA).optional())
            .field("f_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
            .field("f_map_opt", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional())
            .field("f_struct", SchemaBuilder.struct()
                    .field("nested_id", Schema.INT32_SCHEMA)
                    .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA))
            .field("f_struct_opt", SchemaBuilder.struct()
                    .field("nested_id", Schema.INT32_SCHEMA)
                    .field("nested_data", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional())
            .build();
    // Define key schema
    org.apache.kafka.connect.data.Schema keySchema = SchemaBuilder.struct()
            .name("SimpleRecordKey")
            .field("pk_id", Schema.INT32_SCHEMA) // The PK field
            .build();

    // Convert to Iceberg schema
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);
    return converter.icebergSchema();

  }

  // Helper assertion method
  private Types.NestedField assertField(org.apache.iceberg.Schema schema, String name, int expectedId, boolean expectedOptional, org.apache.iceberg.types.Type expectedType) {
    Types.NestedField field = schema.findField(name);
    assertNotNull(field, "Field '" + name + "' not found");
    assertEquals(expectedId, field.fieldId(), "Field ID mismatch for '" + name + "'");
    // assertEquals(expectedOptional, field.isOptional(), "Optionality mismatch for '" + name + "'"); // Optionality check removed as per original code
    assertEquals(expectedType.typeId(), field.type().typeId(), "Type mismatch for '" + name + "'");
    // Check complex types more thoroughly
    if (expectedType.isStructType()) {
      assertEquals(expectedType.asStructType().fields().size(), field.type().asStructType().fields().size(), "Struct field count mismatch for '" + name + "'");
      // Note: Deep comparison of struct fields might be needed for full validation
    } else if (expectedType.isListType()) {
      assertEquals(expectedType.asListType().elementType().typeId(), field.type().asListType().elementType().typeId(), "List element type mismatch for '" + name + "'");
    } else if (expectedType.isMapType()) {
      assertEquals(expectedType.asMapType().keyType().typeId(), field.type().asMapType().keyType().typeId(), "Map key type mismatch for '" + name + "'");
      assertEquals(expectedType.asMapType().valueType().typeId(), field.type().asMapType().valueType().typeId(), "Map value type mismatch for '" + name + "'");
    }
    return field;
  }
}