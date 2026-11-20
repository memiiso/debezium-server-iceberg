/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class StructVariantObjectTest {

  @Test
  void testStructVariantObjectAllSupportedTypes() {
    Schema childSchema = SchemaBuilder.struct().field("c", Schema.STRING_SCHEMA).optional().build();
    Schema schema =
        SchemaBuilder.struct()
            .field("s", Schema.OPTIONAL_STRING_SCHEMA)
            .field("i8", Schema.OPTIONAL_INT8_SCHEMA)
            .field("i16", Schema.OPTIONAL_INT16_SCHEMA)
            .field("i32", Schema.OPTIONAL_INT32_SCHEMA)
            .field("i64", Schema.OPTIONAL_INT64_SCHEMA)
            .field("f32", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("f64", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("dec", Decimal.builder(2).optional().build())
            .field("arr", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(
                "map",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
            .field("child", childSchema)
            .field("nullField", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    Struct struct =
        new Struct(schema)
            .put("s", "hello")
            .put("i8", (byte) 1)
            .put("i16", (short) 2)
            .put("i32", 3)
            .put("i64", 4L)
            .put("f32", 1.5f)
            .put("f64", 2.5d)
            .put("bool", true)
            .put("bytes", new byte[] {1, 2})
            .put("dec", new BigDecimal("12.34"))
            .put("arr", List.of("a", "b"))
            .put("map", Map.of("k", 7))
            .put("child", new Struct(childSchema).put("c", "nested"))
            .put("nullField", null);

    StructVariantObject variant = new StructVariantObject(struct);
    assertNotNull(variant.metadata());
    assertEquals(14, variant.numFields());
    assertEquals("hello", variant.get("s").asPrimitive().get());
    assertEquals((byte) 1, variant.get("i8").asPrimitive().get());
    assertEquals((short) 2, variant.get("i16").asPrimitive().get());
    assertEquals(3, variant.get("i32").asPrimitive().get());
    assertEquals(4L, variant.get("i64").asPrimitive().get());
    assertEquals(1.5f, variant.get("f32").asPrimitive().get());
    assertEquals(2.5d, variant.get("f64").asPrimitive().get());
    assertEquals(true, variant.get("bool").asPrimitive().get());
    assertEquals(ByteBuffer.wrap(new byte[] {1, 2}), variant.get("bytes").asPrimitive().get());
    assertEquals(new BigDecimal("12.34"), variant.get("dec").asPrimitive().get());
    assertEquals(PhysicalType.NULL, variant.get("nullField").type());

    VariantArray arr = variant.get("arr").asArray();
    assertEquals(2, arr.numElements());
    assertEquals("a", arr.get(0).asPrimitive().get());

    VariantObject mapObj = variant.get("map").asObject();
    assertEquals(7, mapObj.get("k").asPrimitive().get());

    VariantObject childObj = variant.get("child").asObject();
    assertEquals("nested", childObj.get("c").asPrimitive().get());
  }

  @Test
  void testNullStructThrowsIllegalArgumentException() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> new StructVariantObject(null));
    assertTrue(ex.getMessage().contains("Invalid Struct: null"));
  }

  @Test
  void testNonStringMapKeyThrowsIllegalArgumentException() {
    Schema schema =
        SchemaBuilder.struct()
            .field("m", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
            .build();
    Struct struct = new Struct(schema).put("m", Map.of(1, "one"));
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> new StructVariantObject(struct));
    assertTrue(ex.getMessage().contains("Only MAP with STRING key is supported"));
  }
}
