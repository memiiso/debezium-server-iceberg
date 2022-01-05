/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergChangeEvent;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;


/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
class IcebergTableOperatorTest extends BaseSparkTest {

  static String testTable = "inventory.test_table_operator";
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
  @ConfigProperty(name = "debezium.sink.iceberg." + DEFAULT_FILE_FORMAT, defaultValue = DEFAULT_FILE_FORMAT_DEFAULT)
  String writeFormat;
  @Inject
  IcebergTableOperator icebergTableOperator;
  IcebergChangeEventBuilder eventBuilder = new IcebergChangeEventBuilder().destination(testTable);

  public Table createTable(IcebergChangeEvent sampleEvent) {
    HadoopCatalog icebergCatalog = getIcebergCatalog();
    final TableIdentifier tableId = TableIdentifier.of(Namespace.of(namespace), tablePrefix + sampleEvent.destinationTable());
    return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(), writeFormat, !upsert);
  }

  @Test
  public void testIcebergTableOperator() {
    // setup
    List<IcebergChangeEvent> events = new ArrayList<>();

    eventBuilder.addKeyField("id", 1)
        .addField("data", "record1")
        .addField("preferences", "feature1", true)
        .addField("preferences", "feature2", true);
    Table icebergTable = this.createTable(eventBuilder.build());

    events.add(eventBuilder.build());
    events.add(eventBuilder.addKeyField("id", 2).addField("data", "record2").build());
    icebergTableOperator.addToTable(icebergTable, events);

    Assertions.assertEquals(2, getTableData(testTable).count());
    events.clear();
    events.add(eventBuilder
        .addKeyField("id", 3)
        .addField("user_name", "Alice")
        .addField("data", "record3_adding_field")
        .build());
    icebergTableOperator.addToTable(icebergTable, events);
    Dataset<Row> ds = getTableData(testTable);
    ds.show(false);
    Assertions.assertEquals(3, getTableData(testTable).count());
    Assertions.assertEquals(1, getTableData(testTable).where("user_name == 'Alice'").count());
  }
}