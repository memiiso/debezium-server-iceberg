/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.Lists;
import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergChangeConsumer;
import io.debezium.server.iceberg.RecordConverter;
import io.debezium.server.iceberg.testresources.BaseTest;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
class IcebergTableOperatorTest extends BaseTest {

  static String testTable = "inventory.test_table_operator";
  @Inject
  IcebergTableOperator icebergTableOperator;

  @Inject
  IcebergChangeConsumer icebergConsumer;

  @Inject
  IcebergChangeEventBuilder eventBuilder;

  public Table createTable(RecordConverter sampleEvent) {
    TableIdentifier tableId =  icebergConsumer.mapDestination(sampleEvent.destination());
    return icebergConsumer.loadIcebergTable(tableId, sampleEvent);
  }

  @Test
  public void testIcebergTableOperator() throws Exception {
    // setup
    List<RecordConverter> events = new ArrayList<>();
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
    RecordConverter e1 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "row1")
        .addField("__source_ts_ms", 1L)
        .build();
    RecordConverter e2 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "row1")
        .addField("__source_ts_ms", 3L)
        .build();

    List<RecordConverter> records = List.of(e1, e2);
    List<RecordConverter> dedups = icebergTableOperator.deduplicateBatch(records);
    Assertions.assertEquals(1, dedups.size());
    Assertions.assertEquals(3L, dedups.get(0).value().get("__source_ts_ms").asLong(0L));

    RecordConverter e21 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addField("__op", "r")
        .addField("__source_ts_ms", 1L)
        .build();
    RecordConverter e22 = eventBuilder
        .destination("destination")
        .addKeyField("id", 1)
        .addField("__op", "u")
        .addField("__source_ts_ms", 1L)
        .build();

    List<RecordConverter> records2 = List.of(e21, e22);
    List<RecordConverter> dedups2 = icebergTableOperator.deduplicateBatch(records2);
    Assertions.assertEquals(1, dedups2.size());
    Assertions.assertEquals("u", dedups2.get(0).value().get("__op").asText("x"));

    // deduplicating wth null key should fail!
    RecordConverter e31 = eventBuilder
        .destination("destination")
        .addField("id", 3)
        .addField("__op", "r")
        .addField("__source_ts_ms", 1L)
        .build();
    RecordConverter e32 = eventBuilder
        .destination("destination")
        .addField("id", 3)
        .addField("__op", "u")
        .addField("__source_ts_ms", 1L)
        .build();

    List<RecordConverter> records3 = List.of(e31, e32);
    DebeziumException thrown = assertThrows(DebeziumException.class,
        () -> {
          icebergTableOperator.deduplicateBatch(records3);
        });

    Assertions.assertTrue(thrown.getMessage().contains("Cannot deduplicate data with null key!"));
  }
}