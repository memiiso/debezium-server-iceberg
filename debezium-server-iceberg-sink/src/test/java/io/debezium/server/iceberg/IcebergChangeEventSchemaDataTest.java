package io.debezium.server.iceberg;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IcebergChangeEventSchemaDataTest {

  @Test
  void testIcebergChangeEventSchemaDataBehaviourAndCloning() {

    IcebergChangeEventSchemaData test = new IcebergChangeEventSchemaData(5);
    test.identifierFieldIds().add(3);
    assertEquals(6, test.nextFieldId().incrementAndGet());
    assertEquals(Set.of(3), test.identifierFieldIds());

    // test cloning and then changing nextFieldId is persisting
    IcebergChangeEventSchemaData copy = test.copyKeepIdentifierFieldIdsAndNextFieldId();
    assertEquals(6, test.nextFieldId().get());
    copy.nextFieldId().incrementAndGet();
    assertEquals(7, test.nextFieldId().get());

    // test cloning and then changing identifier fields is persisting
    assertEquals(Set.of(3), copy.identifierFieldIds());
    copy.identifierFieldIds().add(7);
    assertEquals(Set.of(3, 7), test.identifierFieldIds());

  }

}