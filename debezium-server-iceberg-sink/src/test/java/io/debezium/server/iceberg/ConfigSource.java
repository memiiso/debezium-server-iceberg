/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.TestConfigSource;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

public class ConfigSource extends TestConfigSource {

  public static final String S3_REGION = "us-east-1";
  public static final String S3_BUCKET = "test-bucket";

  final Map<String, String> s3Test = new HashMap<>();

  public ConfigSource() {
    // common sink conf
    s3Test.put("debezium.sink.type", "iceberg");
    s3Test.put("debezium.sink.iceberg.upsert", "false");
    s3Test.put("debezium.sink.iceberg.upsert-keep-deletes", "true");

    // ==== configure batch behaviour/size ====
    // Positive integer value that specifies the maximum size of each batch of events that should be processed during
    // each iteration of this connector. Defaults to 2048.
    s3Test.put("debezium.source.max.batch.size", "2");
    // Positive integer value that specifies the number of milliseconds the connector should wait for new change
    // events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.
    s3Test.put("debezium.source.poll.interval.ms", "10000"); // 5 seconds!
    // iceberg
    s3Test.put("debezium.sink.iceberg.table-prefix", "debeziumcdc_");
    s3Test.put("debezium.sink.iceberg.fs.defaultFS", "s3a://" + S3_BUCKET);
    s3Test.put("debezium.sink.iceberg.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    s3Test.put("debezium.sink.iceberg.type", "hadoop");
    s3Test.put("debezium.sink.iceberg.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");

    // enable disable schema
    s3Test.put("debezium.format.value.schemas.enable", "true");

    // debezium unwrap message
    s3Test.put("debezium.transforms", "unwrap");
    s3Test.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    s3Test.put("debezium.transforms.unwrap.add.fields", "op,table,lsn,source.ts_ms");
    s3Test.put("debezium.transforms.unwrap.add.headers", "db");
    s3Test.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");

    // DEBEZIUM SOURCE conf
    s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
    s3Test.put("debezium.source.offset.flush.interval.ms", "60000");
    s3Test.put("debezium.source.database.server.name", "testc");
    s3Test.put("debezium.source.schema.whitelist", "inventory");
    s3Test.put("debezium.source.table.whitelist", "inventory.customers,inventory.orders,inventory.products," +
        "inventory.geom,inventory.table_datatypes,inventory.test_date_table");

    config.put("quarkus.log.level", "INFO");
    s3Test.put("quarkus.log.category.\"org.apache.spark\".level", "WARN");
    s3Test.put("quarkus.log.category.\"org.apache.hadoop\".level", "ERROR");
    s3Test.put("quarkus.log.category.\"org.apache.parquet\".level", "WARN");
    s3Test.put("quarkus.log.category.\"org.eclipse.jetty\".level", "WARN");
    s3Test.put("quarkus.log.category.\"org.apache.iceberg\".level", "ERROR");

    config = s3Test;
  }
}
