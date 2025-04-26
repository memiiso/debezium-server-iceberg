package io.debezium.server.iceberg.converter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class JsonEventConverterSchemaDataTest {

  @Test
  void testIcebergSchemaConverterDataBehaviourAndCloning() {

    IcebergSchemaInfo test = new IcebergSchemaInfo(5);
    test.identifierFieldIds().add(3);
    assertEquals(6, test.nextFieldId().incrementAndGet());
    assertEquals(Set.of(3), test.identifierFieldIds());

    // test cloning and then changing nextFieldId is persisting
    IcebergSchemaInfo copy = test.copyPreservingMetadata();
    assertEquals(6, test.nextFieldId().get());
    copy.nextFieldId().incrementAndGet();
    assertEquals(7, test.nextFieldId().get());

    // test cloning and then changing identifier fields is persisting
    assertEquals(Set.of(3), copy.identifierFieldIds());
    copy.identifierFieldIds().add(7);
    assertEquals(Set.of(3, 7), test.identifierFieldIds());

  }

}