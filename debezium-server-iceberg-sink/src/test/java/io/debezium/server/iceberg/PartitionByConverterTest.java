package io.debezium.server.iceberg;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionByConverterTest {

    private final PartitionByConverter converter = new PartitionByConverter();

    @Test
    void testSimpleCommaSeparatedValues() {
        List<String> result = converter.convert("field1,field2,field3");
        assertEquals(3, result.size());
        assertEquals("field1", result.get(0));
        assertEquals("field2", result.get(1));
        assertEquals("field3", result.get(2));
    }

    @Test
    void testWithWhitespace() {
        List<String> result = converter.convert("field1, field2 , field3");
        assertEquals(3, result.size());
        assertEquals("field1", result.get(0));
        assertEquals("field2", result.get(1));
        assertEquals("field3", result.get(2));
    }

    @Test
    void testWithParentheses() {
        List<String> result = converter.convert("bucket(10, id),year(ts),month(ts)");
        assertEquals(3, result.size());
        assertEquals("bucket(10, id)", result.get(0));
        assertEquals("year(ts)", result.get(1));
        assertEquals("month(ts)", result.get(2));
    }

    @Test
    void testComplexWithParenthesesAndSpaces() {
        List<String> result = converter.convert("bucket(10, id), year(created_at), month(created_at), day(created_at)");
        assertEquals(4, result.size());
        assertEquals("bucket(10, id)", result.get(0));
        assertEquals("year(created_at)", result.get(1));
        assertEquals("month(created_at)", result.get(2));
        assertEquals("day(created_at)", result.get(3));
    }

    @Test
    void testEmptyString() {
        assertTrue(converter.convert("").isEmpty());
    }

    @Test
    void testWhitespaceOnly() {
        assertTrue(converter.convert("   ").isEmpty());
    }

    @Test
    void testNullValue() {
        assertTrue(() -> converter.convert(null).isEmpty());
    }

    @Test
    void testSingleValue() {
        List<String> result = converter.convert("field1");
        assertEquals(1, result.size());
        assertEquals("field1", result.get(0));
    }

    @Test
    void testNestedParentheses() {
        List<String> result = converter.convert("truncate(10, name),bucket(5, id)");
        assertEquals(2, result.size());
        assertEquals("truncate(10, name)", result.get(0));
        assertEquals("bucket(5, id)", result.get(1));
    }
}

