/*
 *
 * * Copyright memiiso Authors.
 * *
 * * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Lists;
import io.debezium.server.iceberg.BaseTest;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test class to verify Iceberg schema evolution capabilities, particularly for field type changes.
 * It simulates a scenario where a legacy Debezium version (0.8.x) handles timestamps as a long
 * type, and a newer version (0.9.x) handles them as a proper timestamp type.
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergSchemaEvolutionTest.LegacyTimePrecisionProfile.class)
@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class IcebergSchemaEvolutionTest extends BaseTest {

  /**
   * Creates an Iceberg table based on the provided event.
   *
   * @param sampleEvent The JSON event used to infer the table schema.
   * @return The created Iceberg Table object.
   */
  public Table createIcebergTableFromEvent(JsonEventConverter sampleEvent) {
    TableIdentifier tableId = consumer.mapDestination(sampleEvent.destination());
    return consumer.loadIcebergTable(tableId, sampleEvent);
  }

  /**
   * Tests the scenario where a timestamp field's type evolves from long to timestamp.
   * This is a common migration scenario for Debezium users.
   */
  @Test
  public void testTimestampFieldTypeEvolution() throws InterruptedException {
    // A legacy data type handling (e.g., from Debezium 0.8.x) where the timestamp converted to long.
    String eventLegacyJson = """
          {
          "schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},
          {"type":"string","optional":false,"field":"first_name"},
          {"type":"int64","optional":true,"field":"order_created_ts_ms"},
          {"type":"string","optional":true,"field":"__op"},
          {"type":"boolean","optional":true,"field":"__deleted"}]},
          "payload":{"id":1,"first_name":"name","__op":"u","order_created_ts_ms":1735830245000,"__deleted":false}
          }
        """;

    // A new data type handling (e.g., from Debezium 0.9.x) where the timestamp converted to timestamp type.
    String eventModernJson = """
        {
        "schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},
        {"type":"string","optional":false,"field":"first_name"},
        {"type":"int64","optional":true,"field":"order_created_ts_ms","name":"io.debezium.time.Timestamp"},
        {"type":"string","optional":true,"field":"__op"},
        {"type":"boolean","optional":true,"field":"__deleted"}]},
        "payload":{"id":1,"first_name":"name","__op":"u","order_created_ts_ms":1735830245000,"__deleted":false}
        }
        """;

    JsonEventConverter legacyEvent = new JsonEventConverter("test", eventLegacyJson, null, config);
    JsonEventConverter modernEvent = new JsonEventConverter("test", eventModernJson, null, config);

    // Step 1: Create the table with the legacy schema (long for timestamp).
    Table legacyTable = createIcebergTableFromEvent(legacyEvent);
    icebergTableOperator.addToTable(legacyTable, List.of(legacyEvent));

    List<DataFile> legacyDataFiles = getDataFiles(legacyTable);
    assertEquals(1, legacyDataFiles.size(), "Expected one data file after initial write.");

    // Step 2: Attempt to write the modern event, which should fail due to the type mismatch.
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      icebergTableOperator.addToTable(legacyTable, List.of(modernEvent));
    }, "Writing the new event should fail due to a type change.");
    assertTrue(exception.getMessage().contains("Cannot change column type: order_created_ts_ms: long -> timestamp"),
        "The error message should indicate a type change is not allowed.");

    // Step 3: END USER ACTION! Manually evolve the schema by renaming the old column.
    // This is a metadata-only operation, no data rewrite occurs.
    legacyTable.updateSchema()
        .renameColumn("order_created_ts_ms", "order_created_ts_ms__legacy")
        .commit();

    // Reload the table to reflect the schema changes.
    legacyTable.refresh();

    // Step 4: Now, consume the new event. The consumer will add the timestamp field
    // with the correct 'timestamp' type, successfully evolving the schema.
    icebergTableOperator.addToTable(legacyTable, List.of(modernEvent));

    // Verify the data and schema after evolution.
    List<Record> records = Lists.newArrayList(getTableDataV2("test"));
    assertEquals(2, records.size(), "Expected two records after the second write.");

    // Verify both the old and new columns exist with their respective types.
    assertEquals("long", legacyTable.schema().findField("order_created_ts_ms__legacy").type().toString(),
        "The renamed legacy column should have a 'long' type.");
    assertEquals("timestamp", legacyTable.schema().findField("order_created_ts_ms").type().toString(),
        "The new column should have a 'timestamp' type.");

    List<DataFile> evolvedDataFiles = getDataFiles(legacyTable);
    assertEquals(2, evolvedDataFiles.size(), "Expected two data files after schema evolution and new data write.");

    // The new set of data files should contain the original data file.
    List<String> legacyFileLocations = legacyDataFiles.stream().map(DataFile::location).collect(Collectors.toList());
    List<String> evolvedFileLocations = evolvedDataFiles.stream().map(DataFile::location).collect(Collectors.toList());
    assertTrue(evolvedFileLocations.containsAll(legacyFileLocations),
        "The new data files should include the original data file, as no rewrite occurred.");
  }

  /**
   * A Quarkus test profile that configures the Debezium source to use
   * 'connect' time precision, which is crucial for this test's scenario.
   */
  public static class LegacyTimePrecisionProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.source.time.precision.mode", "connect");
      return config;
    }
  }
}