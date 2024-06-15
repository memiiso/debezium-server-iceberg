package io.debezium.server.iceberg;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IcebergChangeEventSchemaDataTest {

  @Test
  void nextFieldId() {

    IcebergChangeEventSchemaData test = new IcebergChangeEventSchemaData(5);
    test.identifierFieldIds().add(1);
    assertEquals(6, test.nextFieldId().incrementAndGet());

    IcebergChangeEventSchemaData testSubschemaField = test.copyKeepNextFieldId();
    testSubschemaField.nextFieldId().incrementAndGet();
    assertEquals(7, test.nextFieldId().get());
  }

}