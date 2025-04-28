package io.debezium.server.iceberg.converter;

import io.debezium.data.Uuid;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
import java.util.UUID;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.lenient;

/**
 * Tests focusing on data type conversions performed by StructEventConverter.
 * Adapted from StructEventConverterTypeTest to use StructEventConverter instead of RecordConverter.
 */
@ExtendWith(MockitoExtension.class)
@EnabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class StructEventConverterTypeTest {

  private static final String TEST_DESTINATION = "test.inventory.data_types";
  private static final String CDC_OP_FIELD = "__op";
  private static final String CDC_TS_MS_FIELD = "__ts_ms";

  // Test Data Values (reusing some from StructEventConverterTest and adding specifics)
  private static final Integer TEST_INT = 1;
  private static final Long TEST_LONG = 2L;
  private static final Float TEST_FLOAT = 1.1f;
  private static final Double TEST_DOUBLE = 2.2d;
  private static final Boolean TEST_BOOLEAN = true;
  private static final String TEST_STRING = "foobar";
  private static final byte[] TEST_BYTES_VAL = new byte[]{1, 2, 3};
  private static final BigDecimal TEST_DECIMAL_VAL = new BigDecimal("12.34");
  private static final int TEST_DECIMAL_SCALE = 2;
  private static final int TEST_DECIMAL_PRECISION = 9; // Precision for 12.34
  private static final UUID TEST_UUID_VAL = UUID.randomUUID();
  private static final Integer TEST_DATE_INT_VAL = (int) LocalDate.parse("2023-05-18").toEpochDay(); // Debezium Date (int)
  private static final Long TEST_TIME_MICROS_VAL = LocalTime.parse("07:14:21").toNanoOfDay() / 1000; // Debezium MicroTime (long)
  private static final Long TEST_TIMESTAMP_MICROS_VAL = LocalDateTime.parse("2023-05-18T07:14:21").toInstant(ZoneOffset.UTC).toEpochMilli() * 1000; // Debezium MicroTimestamp (long)
  private static final String TEST_ZONED_TIMESTAMP_STR_VAL = "2023-05-18T07:14:21Z"; // Debezium ZonedTimestamp (string)
  private static final List<String> TEST_LIST_VAL = List.of("hello", "world");
  private static final Map<String, String> TEST_MAP_VAL = Map.of("one", "1", "two", "2");
  private static final String NESTED_STRUCT_STRING = "nested_string";
  private static final Long NESTED_STRUCT_LONG = 999L;
  private static final Long TEST_TS_MS = Instant.now().toEpochMilli();

  // Expected Iceberg Values
  private static final LocalDate EXPECTED_DATE_VAL = LocalDate.ofEpochDay(TEST_DATE_INT_VAL);
  private static final LocalTime EXPECTED_TIME_VAL = LocalTime.ofNanoOfDay(TEST_TIME_MICROS_VAL * 1000);
  private static final LocalDateTime EXPECTED_TIMESTAMP_VAL =
      Instant.ofEpochSecond(0, TEST_TIMESTAMP_MICROS_VAL * 1000).atOffset(ZoneOffset.UTC).toLocalDateTime();
  private static final OffsetDateTime EXPECTED_ZONED_TIMESTAMP_VAL = OffsetDateTime.parse(TEST_ZONED_TIMESTAMP_STR_VAL);
  private static final ByteBuffer EXPECTED_BYTES_VAL = ByteBuffer.wrap(TEST_BYTES_VAL);
  private static final BigDecimal EXPECTED_DECIMAL_VAL = TEST_DECIMAL_VAL.setScale(TEST_DECIMAL_SCALE);

  @Mock
  public GlobalConfig config;
  @Mock
  public IcebergConfig icebergConfig;
  @Mock
  public DebeziumConfig debeziumConfig;

  // Schemas
  private org.apache.kafka.connect.data.Schema nestedConnectSchema;
  private org.apache.kafka.connect.data.Schema valueConnectSchema;
  private org.apache.kafka.connect.data.Schema keyConnectSchema;
  private org.apache.iceberg.Schema icebergSchema;
  private Types.StructType nestedIcebergStructType;

  @BeforeEach
  void setUp() {
    // Mock configurations
    lenient().when(config.iceberg()).thenReturn(icebergConfig);
    lenient().when(config.debezium()).thenReturn(debeziumConfig);
    lenient().when(debeziumConfig.isEventFlatteningEnabled()).thenReturn(true);
    lenient().when(debeziumConfig.temporalPrecisionMode()).thenReturn(TemporalPrecisionMode.ISOSTRING);
    lenient().when(icebergConfig.createIdentifierFields()).thenReturn(true);
    lenient().when(icebergConfig.cdcOpField()).thenReturn(CDC_OP_FIELD);
    lenient().when(icebergConfig.cdcSourceTsField()).thenReturn(CDC_TS_MS_FIELD);

    // Define Nested Connect Schema
    nestedConnectSchema = SchemaBuilder.struct().name("Nested")
        .field("nested_str", Schema.OPTIONAL_STRING_SCHEMA)
        .field("nested_long", Schema.OPTIONAL_INT64_SCHEMA)
        .optional() // Make nested struct optional for some tests
        .build();

    // Define Value Connect Schema (matching types from original StructEventConverterTypeTest)
    valueConnectSchema = SchemaBuilder.struct().name("Value")
        .field("id", Schema.INT32_SCHEMA) // Key field
        .field("col_int", Schema.OPTIONAL_INT32_SCHEMA) // i
        .field("col_long", Schema.OPTIONAL_INT64_SCHEMA) // l
        .field("col_date", Date.builder().optional().build()) // d (Debezium Date)
        .field("col_time_micros", MicroTime.builder().optional().build()) // t (Debezium MicroTime)
        .field("col_ts_micros", MicroTimestamp.builder().optional().build()) // ts (Debezium MicroTimestamp)
        .field("col_ts_zoned", ZonedTimestamp.builder().optional().build()) // tsz (Debezium ZonedTimestamp)
        .field("col_float", Schema.OPTIONAL_FLOAT32_SCHEMA) // fl
        .field("col_double", Schema.OPTIONAL_FLOAT64_SCHEMA) // do
        .field("col_decimal", Decimal.builder(TEST_DECIMAL_SCALE).optional().build()) // dec
        .field("col_string", Schema.OPTIONAL_STRING_SCHEMA) // s
        .field("col_bool", Schema.OPTIONAL_BOOLEAN_SCHEMA) // b
        .field("col_uuid", Uuid.builder().optional().build()) // u (Debezium Uuid)
        // .field("f", Schema.BYTES_SCHEMA) // Fixed not directly mapped by default StructEventConverter
        .field("col_bytes", Schema.OPTIONAL_BYTES_SCHEMA) // bi
        .field("col_list", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build()) // li
        .field("col_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build()) // ma
        .field("col_struct", nestedConnectSchema) // Nested struct
        .field(CDC_OP_FIELD, Schema.STRING_SCHEMA)
        .field(CDC_TS_MS_FIELD, Schema.INT64_SCHEMA)
        .build();

    // Define Key Connect Schema
    keyConnectSchema = SchemaBuilder.struct().name("Key")
        .field("id", Schema.INT32_SCHEMA)
        .build();

    // Define Nested Iceberg Schema
    nestedIcebergStructType = Types.StructType.of(
        Types.NestedField.optional(100, "nested_str", Types.StringType.get()),
        Types.NestedField.optional(101, "nested_long", Types.LongType.get())
    );

    // Define Iceberg Schema (matching fields and types)
    // Field IDs need to be unique and should generally align with potential evolution.
    icebergSchema = new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()), // Key field
        Types.NestedField.optional(2, "col_int", Types.IntegerType.get()),
        Types.NestedField.optional(3, "col_long", Types.LongType.get()),
        Types.NestedField.optional(4, "col_date", Types.DateType.get()),
        Types.NestedField.optional(5, "col_time_micros", Types.TimeType.get()),
        Types.NestedField.optional(6, "col_ts_micros", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(7, "col_ts_zoned", Types.TimestampType.withZone()),
        Types.NestedField.optional(8, "col_float", Types.FloatType.get()),
        Types.NestedField.optional(9, "col_double", Types.DoubleType.get()),
        Types.NestedField.optional(10, "col_decimal", Types.DecimalType.of(TEST_DECIMAL_PRECISION, TEST_DECIMAL_SCALE)),
        Types.NestedField.optional(11, "col_string", Types.StringType.get()),
        Types.NestedField.optional(12, "col_bool", Types.BooleanType.get()),
        Types.NestedField.optional(13, "col_uuid", Types.UUIDType.get()),
        // Types.NestedField.optional(14, "f", Types.FixedType.ofLength(3)), // Fixed not mapped
        Types.NestedField.optional(15, "col_bytes", Types.BinaryType.get()),
        Types.NestedField.optional(16, "col_list", Types.ListType.ofOptional(17, Types.StringType.get())),
        Types.NestedField.optional(18, "col_map", Types.MapType.ofOptional(19, 20, Types.StringType.get(), Types.StringType.get())),
        Types.NestedField.optional(21, "col_struct", nestedIcebergStructType),
        Types.NestedField.optional(22, CDC_OP_FIELD, Types.StringType.get()),
        Types.NestedField.optional(23, CDC_TS_MS_FIELD, Types.LongType.get())
    );
  }

  private Struct createTestKeyStruct(int id) {
    return new Struct(keyConnectSchema).put("id", id);
  }

  private Struct createFullValueStruct(String op) {
    Struct nestedStruct = new Struct(nestedConnectSchema)
        .put("nested_str", NESTED_STRUCT_STRING)
        .put("nested_long", NESTED_STRUCT_LONG);

    return new Struct(valueConnectSchema)
        .put("id", 1) // Match key
        .put("col_int", TEST_INT)
        .put("col_long", TEST_LONG)
        .put("col_date", TEST_DATE_INT_VAL)
        .put("col_time_micros", TEST_TIME_MICROS_VAL)
        .put("col_ts_micros", TEST_TIMESTAMP_MICROS_VAL)
        .put("col_ts_zoned", TEST_ZONED_TIMESTAMP_STR_VAL)
        .put("col_float", TEST_FLOAT)
        .put("col_double", TEST_DOUBLE)
        .put("col_decimal", TEST_DECIMAL_VAL)
        .put("col_string", TEST_STRING)
        .put("col_bool", TEST_BOOLEAN)
        .put("col_uuid", TEST_UUID_VAL.toString()) // Debezium Uuid expects String
        .put("col_bytes", TEST_BYTES_VAL)
        .put("col_list", TEST_LIST_VAL)
        .put("col_map", TEST_MAP_VAL)
        .put("col_struct", nestedStruct)
        .put(CDC_OP_FIELD, op)
        .put(CDC_TS_MS_FIELD, TEST_TS_MS);
  }

  // Helper to get the converted Iceberg record
  // Changed return type to GenericRecord as RecordWrapper is not needed for assertions
  private RecordWrapper getConvertedIcebergRecord(Struct key, Struct value) {
    EmbeddedEngineChangeEvent event = EventFactory.createMockChangeEvent(key, value);
    StructEventConverter converter = new StructEventConverter(event, config);
    // Use the manually defined schema for conversion consistency in tests
    RecordWrapper wrapper = converter.convert(icebergSchema);
    assertNotNull(wrapper, "RecordWrapper should not be null");
    // Extract the GenericRecord from the wrapper for easier assertion
    return wrapper;
  }

  @Test
  void testStructConvertAllTypes() {
    Struct key = createTestKeyStruct(1);
    Struct value = createFullValueStruct("c");
    RecordWrapper record = getConvertedIcebergRecord(key, value);

    // Assertions for each field type using AssertJ and JUnit 5
    assertThat(record.getField("id")).isEqualTo(1); // From key
    assertThat(record.getField("col_int")).isEqualTo(TEST_INT);
    assertThat(record.getField("col_long")).isEqualTo(TEST_LONG);
    assertThat(record.getField("col_date")).isEqualTo(EXPECTED_DATE_VAL);
    assertThat(record.getField("col_time_micros")).isEqualTo(EXPECTED_TIME_VAL);
    assertThat(record.getField("col_ts_micros")).isEqualTo(EXPECTED_TIMESTAMP_VAL);
    assertThat(record.getField("col_ts_zoned")).isEqualTo(EXPECTED_ZONED_TIMESTAMP_VAL);
    assertThat(record.getField("col_float")).isEqualTo(TEST_FLOAT);
    assertThat(record.getField("col_double")).isEqualTo(TEST_DOUBLE);
    // Compare BigDecimals carefully using compareTo
    assertEquals(0, ((BigDecimal) record.getField("col_decimal")).compareTo(EXPECTED_DECIMAL_VAL));
    assertThat(record.getField("col_string")).isEqualTo(TEST_STRING);
    assertThat(record.getField("col_bool")).isEqualTo(TEST_BOOLEAN);
    assertThat(record.getField("col_uuid")).isEqualTo(TEST_UUID_VAL); // StructEventConverter handles String -> UUID
    assertThat(record.getField("col_bytes")).isEqualTo(EXPECTED_BYTES_VAL); // StructEventConverter handles byte[] -> ByteBuffer
    assertThat(record.getField("col_list")).isEqualTo(TEST_LIST_VAL);
    assertThat(record.getField("col_map")).isEqualTo(TEST_MAP_VAL);

    // Assert Nested Struct
    GenericRecord nestedRecord = (GenericRecord) record.getField("col_struct");
    assertNotNull(nestedRecord);
    assertThat(nestedRecord.getField("nested_str")).isEqualTo(NESTED_STRUCT_STRING);
    assertThat(nestedRecord.getField("nested_long")).isEqualTo(NESTED_STRUCT_LONG);

    // Assert CDC fields
    assertThat(record.getField(CDC_OP_FIELD)).isEqualTo("c");
    assertThat(record.getField(CDC_TS_MS_FIELD)).isEqualTo(TEST_TS_MS);
  }

  @Test
  void testNestedStructConvert() {
    Struct key = createTestKeyStruct(11);
    // Create a value struct with only the nested struct populated (and required fields)
    Struct nestedStruct = new Struct(nestedConnectSchema)
        .put("nested_str", "nested_val")
        .put("nested_long", 555L);
    Struct value = new Struct(valueConnectSchema)
        .put("id", 11)
        .put("col_struct", nestedStruct)
        .put(CDC_OP_FIELD, "c")
        .put(CDC_TS_MS_FIELD, TEST_TS_MS);

    RecordWrapper record = getConvertedIcebergRecord(key, value);

    assertThat(record.getField("id")).isEqualTo(11);
    // Other top-level fields should be null
    assertThat(record.getField("col_int")).isNull();
    assertThat(record.getField("col_string")).isNull();

    // Assert Nested Struct
    GenericRecord nestedRecord = (GenericRecord) record.getField("col_struct");
    assertNotNull(nestedRecord);
    assertThat(nestedRecord.getField("nested_str")).isEqualTo("nested_val");
    assertThat(nestedRecord.getField("nested_long")).isEqualTo(555L);
  }

  @Test
  void testStructValueInListConvert() {
    // Define schemas specifically for this test case
    org.apache.kafka.connect.data.Schema nestedInListConnectSchema = SchemaBuilder.struct().name("NestedInList")
        .field("item_id", Schema.INT32_SCHEMA)
        .build();
    org.apache.kafka.connect.data.Schema listConnectSchema = SchemaBuilder.struct().name("ListHolder")
        .field("id", Schema.INT32_SCHEMA)
        .field("struct_list", SchemaBuilder.array(nestedInListConnectSchema).optional().build())
        .field(CDC_OP_FIELD, Schema.STRING_SCHEMA)
        .field(CDC_TS_MS_FIELD, Schema.INT64_SCHEMA)
        .build();
    org.apache.kafka.connect.data.Schema keyForListSchema = SchemaBuilder.struct().name("ListKey").field("id", Schema.INT32_SCHEMA).build();

    Types.StructType nestedInListIcebergType = Types.StructType.of(
        Types.NestedField.required(101, "item_id", Types.IntegerType.get())
    );
    org.apache.iceberg.Schema listIcebergSchema = new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "struct_list", Types.ListType.ofOptional(3, nestedInListIcebergType)),
        Types.NestedField.optional(4, CDC_OP_FIELD, Types.StringType.get()),
        Types.NestedField.optional(5, CDC_TS_MS_FIELD, Types.LongType.get())
    );

    // Create data
    Struct nested1 = new Struct(nestedInListConnectSchema).put("item_id", 101);
    Struct nested2 = new Struct(nestedInListConnectSchema).put("item_id", 102);
    List<Struct> structList = List.of(nested1, nested2);

    Struct key = new Struct(keyForListSchema).put("id", 20);
    Struct value = new Struct(listConnectSchema)
        .put("id", 20)
        .put("struct_list", structList)
        .put(CDC_OP_FIELD, "c")
        .put(CDC_TS_MS_FIELD, TEST_TS_MS);

    // Convert
    EmbeddedEngineChangeEvent event = EventFactory.createMockChangeEvent(key, value);
    StructEventConverter converter = new StructEventConverter(event, config);
    RecordWrapper record = converter.convert(listIcebergSchema); // Use specific schema
    assertNotNull(record, "Converted record should not be null");

    // Assert
    assertThat(record.getField("id")).isEqualTo(20);
    List<?> icebergList = (List<?>) record.getField("struct_list");
    assertNotNull(icebergList);
    assertThat(icebergList).hasSize(2);

    // Use assertInstanceOf for type checking
    assertInstanceOf(GenericRecord.class, icebergList.get(0));
    GenericRecord icebergNested1 = (GenericRecord) icebergList.get(0);
    assertThat(icebergNested1.getField("item_id")).isEqualTo(101);

    assertInstanceOf(GenericRecord.class, icebergList.get(1));
    GenericRecord icebergNested2 = (GenericRecord) icebergList.get(1);
    assertThat(icebergNested2.getField("item_id")).isEqualTo(102);
  }

  @Test
  void testStructValueInMapConvert() {
    // Define schemas specifically for this test case
    org.apache.kafka.connect.data.Schema nestedInMapConnectSchema = SchemaBuilder.struct().name("NestedInMap")
        .field("item_val", Schema.STRING_SCHEMA)
        .build();
    org.apache.kafka.connect.data.Schema mapConnectSchema = SchemaBuilder.struct().name("MapHolder")
        .field("id", Schema.INT32_SCHEMA)
        .field("struct_map", SchemaBuilder.map(Schema.STRING_SCHEMA, nestedInMapConnectSchema).optional().build())
        .field(CDC_OP_FIELD, Schema.STRING_SCHEMA)
        .field(CDC_TS_MS_FIELD, Schema.INT64_SCHEMA)
        .build();
    org.apache.kafka.connect.data.Schema keyForMapSchema = SchemaBuilder.struct().name("MapKey").field("id", Schema.INT32_SCHEMA).build();

    Types.StructType nestedInMapIcebergType = Types.StructType.of(
        Types.NestedField.required(102, "item_val", Types.StringType.get())
    );
    org.apache.iceberg.Schema mapIcebergSchema = new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "struct_map", Types.MapType.ofOptional(3, 4, Types.StringType.get(), nestedInMapIcebergType)),
        Types.NestedField.optional(5, CDC_OP_FIELD, Types.StringType.get()),
        Types.NestedField.optional(6, CDC_TS_MS_FIELD, Types.LongType.get())
    );

    // Create data
    Struct nestedA = new Struct(nestedInMapConnectSchema).put("item_val", "valueA");
    Struct nestedB = new Struct(nestedInMapConnectSchema).put("item_val", "valueB");
    Map<String, Struct> structMap = Map.of("keyA", nestedA, "keyB", nestedB);

    Struct key = new Struct(keyForMapSchema).put("id", 30);
    Struct value = new Struct(mapConnectSchema)
        .put("id", 30)
        .put("struct_map", structMap)
        .put(CDC_OP_FIELD, "c")
        .put(CDC_TS_MS_FIELD, TEST_TS_MS);

    // Convert
    EmbeddedEngineChangeEvent event = EventFactory.createMockChangeEvent(key, value);
    StructEventConverter converter = new StructEventConverter(event, config);
    RecordWrapper record = converter.convert(mapIcebergSchema); // Use specific schema
    assertNotNull(record, "Converted record should not be null");

    // Assert
    assertThat(record.getField("id")).isEqualTo(30);
    Map<?, ?> icebergMap = (Map<?, ?>) record.getField("struct_map");
    assertNotNull(icebergMap);
    assertThat(icebergMap).hasSize(2);

    // Use assertInstanceOf for type checking
    assertInstanceOf(GenericRecord.class, icebergMap.get("keyA"));
    GenericRecord icebergNestedA = (GenericRecord) icebergMap.get("keyA");
    assertThat(icebergNestedA.getField("item_val")).isEqualTo("valueA");

    assertInstanceOf(GenericRecord.class, icebergMap.get("keyB"));
    GenericRecord icebergNestedB = (GenericRecord) icebergMap.get("keyB");
    assertThat(icebergNestedB.getField("item_val")).isEqualTo("valueB");
  }

  @Test
  void testNullValueHandling() {
    Struct key = createTestKeyStruct(1);
    // Create a value struct with only required fields and CDC fields
    Struct value = new Struct(valueConnectSchema)
        .put("id", 1)
        // All optional fields are null by default if not put
        .put(CDC_OP_FIELD, "c")
        .put(CDC_TS_MS_FIELD, TEST_TS_MS);

    // Explicitly put null for a nested struct to be sure
    value.put("col_struct", null);

    RecordWrapper record = getConvertedIcebergRecord(key, value);

    // Assert required fields
    assertThat(record.getField("id")).isEqualTo(1);
    assertThat(record.getField(CDC_OP_FIELD)).isEqualTo("c");
    assertThat(record.getField(CDC_TS_MS_FIELD)).isEqualTo(TEST_TS_MS);

    // Assert optional fields are null using assertNull
    assertNull(record.getField("col_int"));
    assertNull(record.getField("col_long"));
    assertNull(record.getField("col_date"));
    assertNull(record.getField("col_time_micros"));
    assertNull(record.getField("col_ts_micros"));
    assertNull(record.getField("col_ts_zoned"));
    assertNull(record.getField("col_float"));
    assertNull(record.getField("col_double"));
    assertNull(record.getField("col_decimal"));
    assertNull(record.getField("col_string"));
    assertNull(record.getField("col_bool"));
    assertNull(record.getField("col_uuid"));
    assertNull(record.getField("col_bytes"));
    assertNull(record.getField("col_list"));
    assertNull(record.getField("col_map"));
    assertNull(record.getField("col_struct")); // Check null struct
  }

  @Test
  void testDecimalPrecisionScaleMismatchHandling() {
    int icebergScale = 2;
    int icebergPrecision = 5; // e.g., 12.35 fits in precision 5
    BigDecimal connectDecimal = new BigDecimal("12.345"); // scale 3

    org.apache.iceberg.Schema decimalIcebergSchema = new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "dec_field", Types.DecimalType.of(icebergPrecision, icebergScale)),
        Types.NestedField.optional(3, CDC_OP_FIELD, Types.StringType.get()),
        Types.NestedField.optional(4, CDC_TS_MS_FIELD, Types.LongType.get())
    );

    Struct key = StructBuilder.create("decimalConnectKey")
        .field("id", 50)
        .build();

    EmbeddedEngineChangeEvent event = StructBuilder.create("decimalConnectSchema")
        .field("id", 50)
        .field("dec_field", connectDecimal)
        .field(CDC_OP_FIELD, "c")
        .field(CDC_TS_MS_FIELD, TEST_TS_MS)
        .buildChangeEvent(key);

    // Convert
    StructEventConverter converter = new StructEventConverter(event, config);
    RecordWrapper record = converter.convert(decimalIcebergSchema); // Use specific schema

    // Assert - Iceberg should store the value rounded/adjusted to its scale
    BigDecimal expectedIcebergDecimal = new BigDecimal("12.35"); // Rounded to scale 2
    BigDecimal actualDecimal = (BigDecimal) record.getField("dec_field");

    assertNotNull(actualDecimal);
    // Use assertEquals for compareTo result check
    assertEquals(0, actualDecimal.compareTo(expectedIcebergDecimal), "Decimal values should be equal after scaling");
    // Use assertEquals for scale check
    assertEquals(icebergScale, actualDecimal.scale(), "Decimal scale should match Iceberg schema");
  }

}