/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.DebeziumServer;
import io.debezium.server.testresource.BaseSparkTest;
import io.debezium.server.testresource.S3Minio;
import io.debezium.server.testresource.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(IcebergEventsChangeConsumerTestProfile.class)
public class IcebergEventsChangeConsumerTest extends BaseSparkTest {
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;
  @Inject
  DebeziumServer server;

  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergEventsChangeConsumer.class);
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.iceberg.catalog-name", defaultValue = "default")
  String catalogName;
  Configuration hadoopConf = new Configuration();
  Map<String, String> icebergProperties = new ConcurrentHashMap<>();

  @Test
  public void testIcebergEvents() throws Exception {
    Assertions.assertEquals(sinkType, "icebergevents");
    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      try {
        Dataset<Row> ds = spark.newSession().sql("SELECT * FROM default.debezium_events");
        ds.show();
        return ds.count() >= 5
            && ds.select("event_destination").distinct().count() >= 2;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testExampleAppend() throws InterruptedException {

    Schema TABLE_SCHEMA = new Schema(
        required(1, "event_destination", Types.StringType.get()),
        optional(2, "event_key", Types.StringType.get()),
        optional(3, "event_value", Types.StringType.get()),
        optional(4, "event_sink_epoch_ms", Types.LongType.get()),
        optional(5, "event_sink_timestamp", Types.TimestampType.withZone())
    );
    PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA)
        .identity("event_destination")
        .hour("event_sink_timestamp")
        .build();

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("default"), "debezium_events_test");

    Map<String, String> conf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX);
    conf.forEach(this.hadoopConf::set);
    conf.forEach(this.icebergProperties::put);

    Catalog icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);
    icebergCatalog.createTable(tableIdentifier, TABLE_SCHEMA, TABLE_PARTITION);
    Table eventTable = icebergCatalog.loadTable(tableIdentifier);

    final String fileName =
        UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET;
    // NOTE! manually setting partition directory here to destination
    OutputFile out = eventTable.io().newOutputFile(eventTable.locationProvider().newDataLocation(fileName));

    FileAppender<Record> writer;
    try {
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(eventTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        GenericRecord rec = GenericRecord.create(TABLE_SCHEMA.asStruct());
        rec.setField("event_destination", "test_destination");
        rec.setField("event_key", "{\"aaa\":\"event_key_data\"}");
        rec.setField("event_value", "event_value_data");
        rec.setField("event_sink_epoch_ms", Instant.now().toEpochMilli());
        rec.setField("event_sink_timestamp", OffsetDateTime.now(ZoneOffset.UTC));
        ArrayList<Record> a = new ArrayList<>();
        a.add(rec);
        writer.addAll(a);
      }

    } catch (IOException e) {
      LOGGER.error("Failed committing events to iceberg table!", e);
      throw new InterruptedException(e.getMessage());
    }

    PartitionKey pk = new PartitionKey(TABLE_PARTITION, TABLE_SCHEMA);
    Record pr = GenericRecord.create(TABLE_SCHEMA);
    pr.setField("event_destination", "test_destination");
    pr.setField("event_sink_timestamp", DateTimeUtil.microsFromTimestamptz(OffsetDateTime.now(ZoneOffset.UTC)));
    pk.partition(pr);
    System.out.println(pk.toPath());
    System.out.println(pk.toPath());

    DataFile dataFile = DataFiles.builder(eventTable.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .withPartition(pk)
        .build();

    LOGGER.debug("Appending new file '{}' !", dataFile.path());
    eventTable.newAppend()
        .appendFile(dataFile)
        .commit();
    LOGGER.info("Committed events to table! {}", eventTable.location());

    S3Minio.listFiles();
    Dataset<Row> ds = spark.newSession().sql("SELECT * FROM default.debezium_events_test");
    ds.show();
  }

}
