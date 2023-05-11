/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.spi.ConfigSource;

public class TestConfigSource implements ConfigSource {

  public static final String S3_REGION = "us-east-1";
  public static final String S3_BUCKET = "test-bucket";
  protected Map<String, String> config = new HashMap<>();


  public TestConfigSource() {
    config.put("quarkus.profile", "postgresql");
    // common sink conf
    config.put("debezium.sink.type", "iceberg");
    config.put("debezium.sink.iceberg.upsert", "false");
    config.put("debezium.sink.iceberg.upsert-keep-deletes", "true");
    config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");

    // ==== configure batch behaviour/size ====
    // Positive integer value that specifies the maximum size of each batch of events that should be processed during
    // each iteration of this connector. Defaults to 2048.
    config.put("debezium.source.max.batch.size", "1255");
    // Positive integer value that specifies the number of milliseconds the connector should wait for new change
    // events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.
    config.put("debezium.source.poll.interval.ms", "10000"); // 5 seconds!
    // iceberg
    config.put("debezium.sink.iceberg.table-prefix", "debeziumcdc_");
    config.put("debezium.sink.iceberg.table-namespace", "debeziumevents");
    config.put("debezium.sink.iceberg.fs.defaultFS", "s3a://" + S3_BUCKET);
    config.put("debezium.sink.iceberg.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    config.put("debezium.sink.iceberg.type", "hadoop");
    config.put("debezium.sink.iceberg.catalog-name", "mycatalog");
    //config.put("debezium.sink.iceberg.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");

    // enable disable schema
    config.put("debezium.format.value.schemas.enable", "true");

    // debezium unwrap message
    config.put("debezium.transforms", "unwrap");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,ts_ms");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
    config.put("debezium.transforms.unwrap.drop.tombstones", "true");

    // DEBEZIUM SOURCE conf
    config.put("debezium.source.offset.storage", "io.debezium.server.iceberg.offset.IcebergOffsetBackingStore");
    config.put("debezium.source.offset.storage.iceberg.table-name", "debezium_offset_storage_custom_table");
    config.put("debezium.source.schema.history.internal", "io.debezium.server.iceberg.history.IcebergSchemaHistory");
    config.put("debezium.source.schema.history.internal.iceberg.table-name", "debezium_database_history_storage_test");
    config.put("debezium.source.offset.flush.interval.ms", "60000");
    config.put("debezium.source.database.server.name", "testc");
    config.put("debezium.source.database.server.id", "1234");
    config.put("debezium.source.topic.prefix", "testc");
    config.put("%postgresql.debezium.source.schema.whitelist", "inventory");
    config.put("%postgresql.debezium.source.database.whitelist", "inventory");
    config.put("debezium.source.table.whitelist", "inventory.*");
    config.put("debezium.source.include.schema.changes", "false");

    config.put("quarkus.log.level", "INFO");
    config.put("quarkus.log.category.\"org.apache.spark\".level", "WARN");
    config.put("quarkus.log.category.\"org.apache.hadoop\".level", "ERROR");
    config.put("quarkus.log.category.\"org.apache.parquet\".level", "WARN");
    config.put("quarkus.log.category.\"org.eclipse.jetty\".level", "WARN");
    config.put("quarkus.log.category.\"org.apache.iceberg\".level", "ERROR");

  }

  @Override
  public Map<String, String> getProperties() {
    return config;
  }

  @Override
  public String getName() {
    return "test";
  }

  @Override
  public Set<String> getPropertyNames() {
    return config.keySet();
  }

  @Override
  public String getValue(String propertyName) {
    return config.get(propertyName);
  }
}
