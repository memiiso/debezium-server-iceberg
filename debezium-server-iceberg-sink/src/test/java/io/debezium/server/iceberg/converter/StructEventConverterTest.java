package io.debezium.server.iceberg.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.debezium.DebeziumException;
import io.debezium.data.Uuid;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTimestamp;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
@EnabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class StructEventConverterTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(StructEventConverterTest.class);
  private static final String TEST_DESTINATION = "test.inventory.customers";
  private static final String CDC_OP_FIELD = "__op";
  private static final String CDC_TS_MS_FIELD = "__ts_ms";
  // Test Data Values
  private static final Integer TEST_INT = 123;
  private static final Long TEST_LONG = 456L;
  private static final Float TEST_FLOAT = 12.34f;
  private static final Double TEST_DOUBLE = 56.78;
  private static final Boolean TEST_BOOLEAN = true;
  private static final String TEST_STRING = "hello world";
  private static final byte[] TEST_BYTES = new byte[] {1, 2, 3};
  private static final BigDecimal TEST_DECIMAL = new BigDecimal("1234.56");
  private static final int TEST_DECIMAL_SCALE = 2;
  private static final int TEST_DECIMAL_PRECISION = 6; // 123456
  private static final UUID TEST_UUID = UUID.randomUUID();
  private static final Integer TEST_DATE_INT = 19118; // Days since epoch for 2022-05-06
  private static final Long TEST_TIME_MICROS = 45789000000L; // Micros for 12:43:09
  private static final Long TEST_TIMESTAMP_MICROS =
      1651838589000000L; // Micros for 2022-05-06T12:43:09Z
  private static final String TEST_ZONED_TIMESTAMP_STRING =
      "2022-05-06T14:43:09+02:00"; // ISO String
  private static final List<String> TEST_LIST = List.of("item1", "item2");
  private static final Map<String, Integer> TEST_MAP = Map.of("key1", 1, "key2", 2);
  private static final String NESTED_STRUCT_STRING = "nested_string";
  private static final Long NESTED_STRUCT_LONG = 999L;
  private static final Long TEST_TS_MS = Instant.now().toEpochMilli();
  // Expected Iceberg Values
  private static final LocalDate EXPECTED_DATE = LocalDate.ofEpochDay(TEST_DATE_INT);
  private static final LocalTime EXPECTED_TIME = LocalTime.ofNanoOfDay(TEST_TIME_MICROS * 1000);
  private static final LocalDateTime EXPECTED_TIMESTAMP =
      Instant.ofEpochSecond(0, TEST_TIMESTAMP_MICROS * 1000)
          .atOffset(ZoneOffset.UTC)
          .toLocalDateTime();
  private static final OffsetDateTime EXPECTED_ZONED_TIMESTAMP =
      OffsetDateTime.parse(TEST_ZONED_TIMESTAMP_STRING);
  private static final ByteBuffer EXPECTED_BYTES = ByteBuffer.wrap(TEST_BYTES);
  private static final BigDecimal EXPECTED_DECIMAL = TEST_DECIMAL.setScale(TEST_DECIMAL_SCALE);

  @Mock public GlobalConfig config;
  @Mock public IcebergConfig icebergConfig;
  @Mock public DebeziumConfig debeziumConfig;
  // Schemas
  private org.apache.kafka.connect.data.Schema nestedConnectSchema;
  private org.apache.kafka.connect.data.Schema valueConnectSchema;
  private org.apache.kafka.connect.data.Schema keyConnectSchema;
  private org.apache.iceberg.Schema icebergSchema;
  private Types.StructType nestedIcebergStructType;

  @BeforeEach
  void setUp() {

    lenient().when(config.iceberg()).thenReturn(icebergConfig);
    lenient().when(config.debezium()).thenReturn(debeziumConfig);
    lenient().when(debeziumConfig.isEventFlatteningEnabled()).thenReturn(true);
    lenient()
        .when(debeziumConfig.temporalPrecisionMode())
        .thenReturn(TemporalPrecisionMode.ISOSTRING);
    lenient().when(icebergConfig.createIdentifierFields()).thenReturn(true);
    lenient().when(icebergConfig.cdcOpField()).thenReturn(CDC_OP_FIELD);
    lenient().when(icebergConfig.cdcSourceTsField()).thenReturn(Optional.of(CDC_TS_MS_FIELD));

    // Define Nested Connect Schema
    nestedConnectSchema =
        SchemaBuilder.struct()
            .name("Nested")
            .field("nested_str", Schema.STRING_SCHEMA)
            .field("nested_long", Schema.INT64_SCHEMA)
            .build();

    // Define Value Connect Schema
    valueConnectSchema =
        SchemaBuilder.struct()
            .name("Value")
            .field("id", Schema.INT32_SCHEMA) // Part of key
            .field("col_int", Schema.OPTIONAL_INT32_SCHEMA)
            .field("col_long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("col_float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("col_double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("col_bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("col_string", Schema.OPTIONAL_STRING_SCHEMA)
            .field("col_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("col_decimal", Decimal.builder(TEST_DECIMAL_SCALE).optional().build())
            .field("col_uuid", Uuid.builder().optional().build())
            .field("col_date", Date.builder().optional().build()) // Debezium Date (int)
            .field(
                "col_time_micros",
                MicroTime.builder().optional().build()) // Debezium MicroTime (long)
            .field(
                "col_ts_micros",
                MicroTimestamp.builder()
                    .optional()
                    .build()) // Debezium MicroTimestamp (long) - maps to LocalDateTime
            .field(
                "col_ts_zoned",
                ZonedTimestamp.builder()
                    .optional()
                    .build()) // Debezium ZonedTimestamp (string) - maps to OffsetDateTime
            .field("col_list", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(
                "col_map",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .field("col_struct", nestedConnectSchema)
            .field(CDC_OP_FIELD, Schema.STRING_SCHEMA)
            .field(CDC_TS_MS_FIELD, Schema.INT64_SCHEMA)
            .build();

    // Define Key Connect Schema
    keyConnectSchema = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();

    // Define Nested Iceberg Schema
    nestedIcebergStructType =
        Types.StructType.of(
            Types.NestedField.optional(100, "nested_str", Types.StringType.get()),
            Types.NestedField.optional(101, "nested_long", Types.LongType.get()));

    // Define Iceberg Schema (field IDs need to be unique and sequential)
    icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()), // Key field
            Types.NestedField.optional(2, "col_int", Types.IntegerType.get()),
            Types.NestedField.optional(3, "col_long", Types.LongType.get()),
            Types.NestedField.optional(4, "col_float", Types.FloatType.get()),
            Types.NestedField.optional(5, "col_double", Types.DoubleType.get()),
            Types.NestedField.optional(6, "col_bool", Types.BooleanType.get()),
            Types.NestedField.optional(7, "col_string", Types.StringType.get()),
            Types.NestedField.optional(8, "col_bytes", Types.BinaryType.get()),
            Types.NestedField.optional(
                9, "col_decimal", Types.DecimalType.of(TEST_DECIMAL_PRECISION, TEST_DECIMAL_SCALE)),
            Types.NestedField.optional(10, "col_uuid", Types.UUIDType.get()),
            Types.NestedField.optional(11, "col_date", Types.DateType.get()),
            Types.NestedField.optional(12, "col_time_micros", Types.TimeType.get()),
            Types.NestedField.optional(
                13,
                "col_ts_micros",
                Types.TimestampType.withoutZone()), // MicroTimestamp -> LocalDateTime
            Types.NestedField.optional(
                14,
                "col_ts_zoned",
                Types.TimestampType.withZone()), // ZonedTimestamp -> OffsetDateTime
            Types.NestedField.optional(
                15, "col_list", Types.ListType.ofOptional(16, Types.StringType.get())),
            Types.NestedField.optional(
                17,
                "col_map",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.IntegerType.get())),
            Types.NestedField.optional(20, "col_struct", nestedIcebergStructType),
            Types.NestedField.optional(21, CDC_OP_FIELD, Types.StringType.get()),
            Types.NestedField.optional(
                22, CDC_TS_MS_FIELD, Types.LongType.get()) // Assuming Long for ts_ms
            );
  }

  private Struct createTestValueStruct(String opValue) {
    Struct nestedStruct =
        new Struct(nestedConnectSchema)
            .put("nested_str", NESTED_STRUCT_STRING)
            .put("nested_long", NESTED_STRUCT_LONG);

    Struct valueStruct =
        new Struct(valueConnectSchema)
            .put("id", 1) // Match key
            .put("col_int", TEST_INT)
            .put("col_long", TEST_LONG)
            .put("col_float", TEST_FLOAT)
            .put("col_double", TEST_DOUBLE)
            .put("col_bool", TEST_BOOLEAN)
            .put("col_string", TEST_STRING)
            .put("col_bytes", TEST_BYTES)
            .put("col_decimal", TEST_DECIMAL)
            .put("col_uuid", TEST_UUID.toString()) // Connect Uuid logical type expects String
            .put("col_date", TEST_DATE_INT) // Debezium Date expects int
            .put("col_time_micros", TEST_TIME_MICROS) // Debezium MicroTime expects long
            .put("col_ts_micros", TEST_TIMESTAMP_MICROS) // Debezium MicroTimestamp expects long
            .put(
                "col_ts_zoned",
                TEST_ZONED_TIMESTAMP_STRING) // Debezium ZonedTimestamp expects String
            .put("col_list", TEST_LIST)
            .put("col_map", TEST_MAP)
            .put("col_struct", nestedStruct)
            .put(CDC_OP_FIELD, opValue)
            .put(CDC_TS_MS_FIELD, TEST_TS_MS);

    return valueStruct;
  }

  private Struct createTestKeyStruct() {
    return new Struct(keyConnectSchema).put("id", 1);
  }

  private EmbeddedEngineChangeEvent createMockChangeEvent(Struct key, Struct value) {
    SourceRecord mockSourceRecord = Mockito.mock(SourceRecord.class);
    when(mockSourceRecord.key()).thenReturn(key);
    when(mockSourceRecord.value()).thenReturn(value);
    when(mockSourceRecord.keySchema()).thenReturn(key != null ? key.schema() : null);
    lenient()
        .when(mockSourceRecord.valueSchema())
        .thenReturn(value != null ? value.schema() : null);
    lenient()
        .when(mockSourceRecord.topic())
        .thenReturn(TEST_DESTINATION); // Or derive from destination if needed
    // Mock other SourceRecord methods if the converter uses them (e.g., headers, timestamp)
    //
    // when(mockSourceRecord.sourcePartition()).thenReturn(Collections.singletonMap(AbstractSourceInfo.SERVER_NAME_KEY, "test_server"));
    //        when(mockSourceRecord.sourceOffset()).thenReturn(Collections.singletonMap("lsn",
    // 12345L));

    EmbeddedEngineChangeEvent mockEvent = Mockito.mock(EmbeddedEngineChangeEvent.class);
    lenient().when(mockEvent.key()).thenReturn(key);
    lenient().when(mockEvent.value()).thenReturn(value);
    when(mockEvent.sourceRecord()).thenReturn(mockSourceRecord);
    lenient().when(mockEvent.destination()).thenReturn(TEST_DESTINATION);

    return mockEvent;
  }

  @Test
  void testConvertInsertEvent() {
    Struct keyStruct = createTestKeyStruct();
    Struct valueStruct = createTestValueStruct("c");
    EmbeddedEngineChangeEvent event = createMockChangeEvent(keyStruct, valueStruct);

    StructEventConverter converter = new StructEventConverter(event, config);

    // Use the schema derived by the converter itself for conversion
    org.apache.iceberg.Schema derivedIcebergSchema = converter.icebergSchema();
    // For assertion, we use the manually defined one to ensure correctness
    // In a real scenario, you might trust the derived schema or compare them first.
    // @TODO FIX
    //    assertEquals(icebergSchema.asStruct(), derivedIcebergSchema.asStruct(), "Derived schema
    // should match expected schema");

    RecordWrapper icebergRecord = converter.convert(derivedIcebergSchema);

    assertNotNull(icebergRecord);
    assertEquals(Operation.INSERT, icebergRecord.op());

    // Assertions for each field type
    assertEquals(1, icebergRecord.getField("id")); // From key
    assertEquals(TEST_INT, icebergRecord.getField("col_int"));
    assertEquals(TEST_LONG, icebergRecord.getField("col_long"));
    assertEquals(TEST_FLOAT, icebergRecord.getField("col_float"));
    assertEquals(TEST_DOUBLE, icebergRecord.getField("col_double"));
    assertEquals(TEST_BOOLEAN, icebergRecord.getField("col_bool"));
    assertEquals(TEST_STRING, icebergRecord.getField("col_string"));
    assertEquals(EXPECTED_BYTES, icebergRecord.getField("col_bytes"));
    // Compare BigDecimals carefully
    assertEquals(0, EXPECTED_DECIMAL.compareTo((BigDecimal) icebergRecord.getField("col_decimal")));
    assertEquals(TEST_UUID, icebergRecord.getField("col_uuid"));
    assertEquals(EXPECTED_DATE, icebergRecord.getField("col_date"));
    assertEquals(
        TimeUnit.MICROSECONDS.toMillis(EXPECTED_TIME.toNanoOfDay()),
        icebergRecord.getField("col_time_micros"));
    assertEquals(EXPECTED_TIMESTAMP, icebergRecord.getField("col_ts_micros"));
    assertEquals(EXPECTED_ZONED_TIMESTAMP, icebergRecord.getField("col_ts_zoned"));
    assertEquals(TEST_LIST, icebergRecord.getField("col_list"));
    assertEquals(TEST_MAP, icebergRecord.getField("col_map"));

    // Assert Nested Struct
    GenericRecord nestedRecord = (GenericRecord) icebergRecord.getField("col_struct");
    assertNotNull(nestedRecord);
    assertEquals(NESTED_STRUCT_STRING, nestedRecord.getField("nested_str"));
    assertEquals(NESTED_STRUCT_LONG, nestedRecord.getField("nested_long"));

    // Assert CDC fields
    assertEquals("c", icebergRecord.getField(CDC_OP_FIELD));
    LocalDateTime tsMsVal =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(TEST_TS_MS), ZoneOffset.UTC);
    assertEquals(tsMsVal + "Z", icebergRecord.getField(CDC_TS_MS_FIELD) + "");
  }

  @Test
  void testConvertUpdateEvent() {
    Struct keyStruct = createTestKeyStruct();
    Struct valueStruct = createTestValueStruct("u");
    EmbeddedEngineChangeEvent event = createMockChangeEvent(keyStruct, valueStruct);

    StructEventConverter converter = new StructEventConverter(event, config);
    org.apache.iceberg.Schema derivedIcebergSchema = converter.icebergSchema();
    RecordWrapper recordWrapper = converter.convert(derivedIcebergSchema);

    assertEquals(Operation.UPDATE, recordWrapper.op());
    assertEquals("u", recordWrapper.getField(CDC_OP_FIELD));
  }

  @Test
  void testConvertDeleteEvent() {
    Struct keyStruct = createTestKeyStruct();
    Struct valueStruct = createTestValueStruct("d");
    EmbeddedEngineChangeEvent event = createMockChangeEvent(keyStruct, valueStruct);
    StructEventConverter converter = new StructEventConverter(event, config);
    org.apache.iceberg.Schema derivedIcebergSchema = converter.icebergSchema();
    RecordWrapper recordWrapper = converter.convert(derivedIcebergSchema);

    assertEquals(Operation.DELETE, recordWrapper.op());
    assertEquals("d", recordWrapper.getField(CDC_OP_FIELD));
  }

  @Test
  void testConvertAsAppend() {
    Struct keyStruct = createTestKeyStruct();
    Struct valueStruct = createTestValueStruct("i"); // Op in data is ignored
    EmbeddedEngineChangeEvent event = createMockChangeEvent(keyStruct, valueStruct);

    StructEventConverter converter = new StructEventConverter(event, config);
    org.apache.iceberg.Schema derivedIcebergSchema = converter.icebergSchema();
    RecordWrapper recordWrapper =
        converter.convertAsAppend(derivedIcebergSchema); // Use convertAsAppend

    assertEquals(Operation.INSERT, recordWrapper.op()); // Should always be INSERT
    assertEquals(TEST_INT, recordWrapper.getField("col_int"));
    assertEquals("i", recordWrapper.getField(CDC_OP_FIELD));
  }

  @Test
  void testNullValueHandling() {
    Struct keyStruct = createTestKeyStruct();
    Struct valueStruct =
        new Struct(valueConnectSchema)
            .put("id", 1)
            // All optional fields are null
            .put(CDC_OP_FIELD, "i")
            .put(CDC_TS_MS_FIELD, TEST_TS_MS);
    // Put null for the optional struct field
    valueStruct.put("col_int", null);
    valueStruct.put("col_string", null);

    EmbeddedEngineChangeEvent event = createMockChangeEvent(keyStruct, valueStruct);
    StructEventConverter converter = new StructEventConverter(event, config);
    org.apache.iceberg.Schema derivedIcebergSchema = converter.icebergSchema();
    RecordWrapper icebergRecord = converter.convert(derivedIcebergSchema);

    assertNotNull(icebergRecord);
    // Assert that optional fields are null in the Iceberg record
    assertNull(icebergRecord.getField("col_int"));
    assertNull(icebergRecord.getField("col_long"));
    assertNull(icebergRecord.getField("col_float"));
    assertNull(icebergRecord.getField("col_double"));
    assertNull(icebergRecord.getField("col_bool"));
    assertNull(icebergRecord.getField("col_string"));
    assertNull(icebergRecord.getField("col_bytes"));
    assertNull(icebergRecord.getField("col_decimal"));
    assertNull(icebergRecord.getField("col_uuid"));
    assertNull(icebergRecord.getField("col_date"));
    assertNull(icebergRecord.getField("col_time_micros"));
    assertNull(icebergRecord.getField("col_ts_micros"));
    assertNull(icebergRecord.getField("col_ts_zoned"));
    assertNull(icebergRecord.getField("col_list"));
    assertNull(icebergRecord.getField("col_map"));
    assertNull(icebergRecord.getField("col_struct")); // Check null struct

    // Required fields should still be there
    assertEquals(1, icebergRecord.getField("id"));
    assertEquals("i", icebergRecord.getField(CDC_OP_FIELD));
    LocalDateTime tsMsVal =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(TEST_TS_MS), ZoneOffset.UTC);
    assertEquals(tsMsVal + "Z", icebergRecord.getField(CDC_TS_MS_FIELD) + "");
  }

  @Test
  void testConvertWithNullValueStruct() {
    Struct keyStruct = createTestKeyStruct();
    SourceRecord mockSourceRecord = Mockito.mock(SourceRecord.class);
    lenient().when(mockSourceRecord.key()).thenReturn(keyStruct);
    lenient().when(mockSourceRecord.value()).thenReturn(null); // Null value
    lenient().when(mockSourceRecord.keySchema()).thenReturn(keyConnectSchema);
    lenient().when(mockSourceRecord.valueSchema()).thenReturn(valueConnectSchema); // Provide schema
    lenient().when(mockSourceRecord.topic()).thenReturn(TEST_DESTINATION);

    EmbeddedEngineChangeEvent mockEvent = Mockito.mock(EmbeddedEngineChangeEvent.class);
    lenient().when(mockEvent.key()).thenReturn(keyStruct);
    lenient().when(mockEvent.value()).thenReturn(null); // Null value
    lenient().when(mockEvent.sourceRecord()).thenReturn(mockSourceRecord);
    lenient().when(mockEvent.destination()).thenReturn(TEST_DESTINATION);

    StructEventConverter converter = new StructEventConverter(mockEvent, config);

    org.apache.iceberg.Schema derivedIcebergSchema =
        converter.icebergSchema(); // Schema derivation should still work

    // convert() should fail because it needs the value struct to get CDC fields etc.
    assertThrows(
        DebeziumException.class,
        () -> {
          converter.convert(derivedIcebergSchema);
        },
        "Conversion should fail with null value struct");

    // convertAsAppend() should also fail
    assertThrows(
        DebeziumException.class,
        () -> {
          converter.convertAsAppend(derivedIcebergSchema);
        },
        "convertAsAppend should fail with null value struct");
  }

  @Test
  void testDecimalPrecisionScaleMismatch() {
    // Define a decimal with different precision/scale in Connect vs Iceberg
    BigDecimal connectDecimal = new BigDecimal("12.345"); // scale 3
    int connectScale = 3;
    org.apache.kafka.connect.data.Schema decimalConnectSchema =
        Decimal.builder(connectScale).build();

    org.apache.iceberg.Schema decimalIcebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(
                1, "dec_field", Types.DecimalType.of(5, 2)) // precision 5, scale 2
            );
    Types.StructType icebergStruct = decimalIcebergSchema.asStruct();

    Struct valueStruct =
        new Struct(
                SchemaBuilder.struct()
                    .field("dec_field", decimalConnectSchema)
                    .field(CDC_OP_FIELD, Schema.STRING_SCHEMA)
                    .build())
            .put("dec_field", connectDecimal)
            .put(CDC_OP_FIELD, "c");

    // Minimal mock setup for convertValue
    StructEventConverter converter =
        new StructEventConverter(
            createMockChangeEvent(null, valueStruct), // Key not needed for this specific test part
            config);

    RecordWrapper icebergRecord = converter.convert(decimalIcebergSchema);

    BigDecimal expectedIcebergDecimal = new BigDecimal("12.35"); // Rounded to scale 2
    assertEquals(
        0, expectedIcebergDecimal.compareTo((BigDecimal) icebergRecord.getField("dec_field")));
  }

  @Test
  void testSortOrderSkipsKeyFieldsNotInValueSchema() {
    // This test verifies that sortOrder() correctly skips key fields that don't exist
    // in the value schema. This scenario occurs when using Debezium's ByLogicalTableRouter
    // transform which adds a key field (e.g., 'tenant_db') only for records matching
    // a specific pattern, but the field is not present in the value schema.

    // 1. Define the Value Schema (without 'tenant_db' field)
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .name("TournamentRecord")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("location", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    // 2. Define the Key Schema WITH an extra field 'tenant_db' that doesn't exist in value schema
    // This simulates what ByLogicalTableRouter does when it adds a key field for routing
    org.apache.kafka.connect.data.Schema keySchema =
        SchemaBuilder.struct()
            .name("TournamentRecordKey")
            .field("id", Schema.INT32_SCHEMA)
            .field("tenant_db", Schema.STRING_SCHEMA) // Extra key field not in value schema
            .build();

    // 3. Instantiate the Converter
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);

    // 4. Convert to Iceberg Schema
    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();
    LOGGER.debug("Generated Iceberg schema: {}", icebergSchema);

    // 5. Verify the Iceberg schema does NOT contain 'tenant_db'
    assertNotNull(icebergSchema, "Iceberg schema should not be null");
    assertEquals(3, icebergSchema.columns().size(), "Should have 3 fields (id, name, location)");
    assertNotNull(icebergSchema.findField("id"), "Field 'id' should exist");
    assertNotNull(icebergSchema.findField("name"), "Field 'name' should exist");
    assertNotNull(icebergSchema.findField("location"), "Field 'location' should exist");
    assertEquals(
        null, icebergSchema.findField("tenant_db"), "Field 'tenant_db' should NOT exist in schema");

    // 6. Get the sort order - this should NOT throw an exception
    SortOrder sortOrder = converter.sortOrder(icebergSchema);
    LOGGER.debug("Generated sort order: {}", sortOrder);

    // 7. Verify the sort order only contains 'id' (the key field that exists in value schema)
    assertNotNull(sortOrder, "Sort order should not be null");
    // Sort order should have 1 field (id), not 2 (id + tenant_db)
    assertEquals(1, sortOrder.fields().size(), "Sort order should have 1 field (only 'id')");
    assertEquals(
        "id",
        sortOrder.fields().get(0).sourceId() == 1 ? "id" : "unknown",
        "Sort order should be on 'id' field");
  }

  @Test
  void testSortOrderWithAllKeyFieldsInValueSchema() {
    // This test verifies that sortOrder() includes all key fields when they all exist
    // in the value schema (the normal case).

    // 1. Define the Value Schema
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .name("TournamentRecord")
            .field("id", Schema.INT32_SCHEMA)
            .field("tenant_db", Schema.STRING_SCHEMA) // This time tenant_db IS in value schema
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    // 2. Define the Key Schema with both id and tenant_db
    org.apache.kafka.connect.data.Schema keySchema =
        SchemaBuilder.struct()
            .name("TournamentRecordKey")
            .field("id", Schema.INT32_SCHEMA)
            .field("tenant_db", Schema.STRING_SCHEMA)
            .build();

    // 3. Instantiate the Converter
    StructSchemaConverter converter = new StructSchemaConverter(valueSchema, keySchema, config);

    // 4. Convert to Iceberg Schema
    org.apache.iceberg.Schema icebergSchema = converter.icebergSchema();

    // 5. Verify the Iceberg schema contains both key fields
    assertNotNull(icebergSchema.findField("id"), "Field 'id' should exist");
    assertNotNull(icebergSchema.findField("tenant_db"), "Field 'tenant_db' should exist");

    // 6. Get the sort order
    SortOrder sortOrder = converter.sortOrder(icebergSchema);

    // 7. Verify the sort order contains both key fields
    assertNotNull(sortOrder, "Sort order should not be null");
    assertEquals(
        2, sortOrder.fields().size(), "Sort order should have 2 fields (id and tenant_db)");
  }
}
