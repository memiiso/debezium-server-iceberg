package io.debezium.server.iceberg.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.server.iceberg.GlobalConfig;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JsonSchemaConverterTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void testEqualsAndHashCodeWithIdenticalSchemas() throws Exception {
    GlobalConfig config = Mockito.mock(GlobalConfig.class);
    String schemaJson1 = "{\"type\":\"struct\",\"fields\":[{\"field\":\"id\",\"type\":\"int32\"}]}";
    String schemaJson2 = "{\"type\":\"struct\",\"fields\":[{\"field\":\"id\",\"type\":\"int32\"}]}";

    JsonNode node1 = mapper.readTree(schemaJson1);
    JsonNode node2 = mapper.readTree(schemaJson2);

    JsonSchemaConverter converter1 = new JsonSchemaConverter(node1, null, config);
    JsonSchemaConverter converter2 = new JsonSchemaConverter(node2, null, config);

    Assertions.assertEquals(converter1, converter2);
    Assertions.assertEquals(converter1.hashCode(), converter2.hashCode());

    Set<JsonSchemaConverter> set = new HashSet<>();
    set.add(converter1);
    set.add(converter2);
    Assertions.assertEquals(1, set.size(), "Identical schemas should collapse in HashSet");
  }

  @Test
  void testDifferentSchemasNotEqual() throws Exception {
    GlobalConfig config = Mockito.mock(GlobalConfig.class);
    String schemaJson1 = "{\"type\":\"struct\",\"fields\":[{\"field\":\"id\",\"type\":\"int32\"}]}";
    String schemaJson2 =
        "{\"type\":\"struct\",\"fields\":[{\"field\":\"name\",\"type\":\"string\"}]}";

    JsonNode node1 = mapper.readTree(schemaJson1);
    JsonNode node2 = mapper.readTree(schemaJson2);

    JsonSchemaConverter converter1 = new JsonSchemaConverter(node1, null, config);
    JsonSchemaConverter converter2 = new JsonSchemaConverter(node2, null, config);

    Assertions.assertNotEquals(converter1, converter2);
  }

  @Test
  void testLinkedHashMapGroupingPreservesOrderAndGroupsBatches() throws Exception {
    GlobalConfig config = Mockito.mock(GlobalConfig.class);
    JsonNode schemaA =
        mapper.readTree("{\"type\":\"struct\",\"fields\":[{\"field\":\"a\",\"type\":\"int32\"}]}");
    JsonNode schemaB =
        mapper.readTree("{\"type\":\"struct\",\"fields\":[{\"field\":\"b\",\"type\":\"string\"}]}");

    JsonSchemaConverter convA1 = new JsonSchemaConverter(schemaA, null, config);
    JsonSchemaConverter convB = new JsonSchemaConverter(schemaB, null, config);
    JsonSchemaConverter convA2 = new JsonSchemaConverter(schemaA, null, config);

    List<JsonSchemaConverter> converters = List.of(convA1, convB, convA2);

    Map<JsonSchemaConverter, List<JsonSchemaConverter>> grouped =
        converters.stream()
            .collect(Collectors.groupingBy(c -> c, LinkedHashMap::new, Collectors.toList()));

    Assertions.assertEquals(2, grouped.size(), "Should have exactly 2 distinct schema groups");

    List<JsonSchemaConverter> keys = List.copyOf(grouped.keySet());
    Assertions.assertEquals(convA1, keys.get(0));
    Assertions.assertEquals(convB, keys.get(1));
    Assertions.assertEquals(
        2, grouped.get(convA1).size(), "Schema A should group 2 events together");
    Assertions.assertEquals(1, grouped.get(convB).size(), "Schema B should group 1 event");
  }
}
