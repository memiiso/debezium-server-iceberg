/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.debezium.DebeziumException;
import io.debezium.server.iceberg.BaseTest;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class IcebergTableOperatorTest extends BaseTest {

  static String testTable = "inventory.test_table_operator";

  public Table createTable(JsonEventConverter sampleEvent) {
    TableIdentifier tableId = consumer.mapDestination(sampleEvent.destination());
    return consumer.loadIcebergTable(tableId, sampleEvent);
  }

  @Test
  public void testIcebergTableOperator() throws Exception {
    // setup
    List<EventConverter> events = new ArrayList<>();
    Table icebergTable = this.createTable(
        eventBuilder
            .destination(testTable)
            .addKeyField("id", 1)
            .addField("data", "record1")
            .addField("preferences", "feature1", true)
            .build()
    );

    events.add(eventBuilder
        .destination(testTable)
        .addKeyField("id", 1)
        .addField("data", "record1")
        .build()
    );
    events.add(eventBuilder
        .destination(testTable)
        .addKeyField("id", 2)
        .addField("data", "record2")
        .build()
    );
    events.add(eventBuilder
        .destination(testTable)
        .addKeyField("id", 3)
        .addField("user_name", "Alice")
        .addField("data", "record3_adding_field")
        .build()
    );
    icebergTableOperator.addToTable(icebergTable, events);

    Assertions.assertEquals(3, Lists.newArrayList(getTableDataV2(testTable)).size());
    events.clear();
    events.add(eventBuilder
        .destination(testTable)
        .addKeyField("id", 3)
        .addField("user_name", "Alice-Updated")
        .addField("data", "record3_updated")
        .addField("preferences", "feature2", "feature2Val2")
        .addField("__op", "u")
        .build()
    );
    icebergTableOperator.addToTable(icebergTable, events);
    Assertions.assertEquals(4, Lists.newArrayList(getTableDataV2(testTable)).size());
    Assertions.assertTrue(Lists.newArrayList(getTableDataV2(testTable)).toString().contains("Alice-Updated"));
    Assertions.assertTrue(Lists.newArrayList(getTableDataV2(testTable)).toString().contains("feature2Val2"));
  }

  @Test
  public void testDeduplicateBatch() throws Exception {
    JsonEventConverter e1 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "row1")
        .addField("__source_ts_ns", 1L)
        .build();
    JsonEventConverter e2 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "row1")
        .addField("__source_ts_ns", 3L)
        .build();

    List<EventConverter> records = List.of(e1, e2);
    List<EventConverter> dedups = icebergTableOperator.deduplicateBatch(records);
    Assertions.assertEquals(1, dedups.size());
    Assertions.assertEquals(3L, ((JsonNode)dedups.get(0).value()).get("__source_ts_ns").asLong(0L));

    EventConverter e21 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addField("__op", "r")
        .addField("__source_ts_ns", 1L)
        .build();
    EventConverter e22 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addField("__op", "u")
        .addField("__source_ts_ns", 1L)
        .build();

    List<EventConverter> records2 = List.of(e21, e22);
    List<EventConverter> dedups2 = icebergTableOperator.deduplicateBatch(records2);
    Assertions.assertEquals(1, dedups2.size());
    Assertions.assertEquals("u", ((JsonNode)dedups2.get(0).value()).get("__op").asText("x"));

    // deduplicating wth null key should fail!
    EventConverter e31 = eventBuilder
        .destination("destination")
        .addField("id", 3)
        .addField("__op", "r")
        .addField("__source_ts_ns", 1L)
        .build();
    JsonEventConverter e32 = eventBuilder
        .destination("destination")
        .addField("id", 3)
        .addField("__op", "u")
        .addField("__source_ts_ns", 1L)
        .build();

    List<EventConverter> records3 = List.of(e31, e32);
    DebeziumException thrown = assertThrows(DebeziumException.class,
        () -> {
          icebergTableOperator.deduplicateBatch(records3);
        });

    Assertions.assertTrue(thrown.getMessage().contains("Cannot deduplicate data with null key!"));
  }
}