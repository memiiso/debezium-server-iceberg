/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.TestConfigSource;
import io.debezium.server.TestDatabase;
import io.debezium.server.testresource.TestS3Minio;

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

    // ==== configure batch behavious/size ====
    // Positive integer value that specifies the maximum size of each batch of events that should be processed during
    // each iteration of this connector. Defaults to 2048.
    s3Test.put("debezium.source.max.batch.size", "2");
    // Positive integer value that specifies the number of milliseconds the connector should wait for new change
    // events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.
    s3Test.put("debezium.source.poll.interval.ms", "10000"); // 5 seconds!
    // iceberg
    s3Test.put("debezium.sink.iceberg.table-prefix", "debezium-cdc-");
    s3Test.put("debezium.sink.iceberg.fs.defaultFS", "s3a://" + S3_BUCKET);
    s3Test.put("debezium.sink.iceberg.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    s3Test.put("debezium.sink.iceberg.user.timezone", "UTC");
    s3Test.put("debezium.sink.iceberg.com.amazonaws.services.s3.enableV4", "true");
    s3Test.put("debezium.sink.iceberg.com.amazonaws.services.s3a.enableV4", "true");
    s3Test.put("debezium.sink.iceberg.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    s3Test.put("debezium.sink.iceberg.fs.s3a.access.key", TestS3Minio.MINIO_ACCESS_KEY);
    s3Test.put("debezium.sink.iceberg.fs.s3a.secret.key", TestS3Minio.MINIO_SECRET_KEY);
    s3Test.put("debezium.sink.iceberg.fs.s3a.path.style.access", "true");
    s3Test.put("debezium.sink.iceberg.fs.s3a.endpoint", "http://localhost:9000"); // minio specific setting
    s3Test.put("debezium.sink.iceberg.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // enable disable schema
    s3Test.put("debezium.format.value.schemas.enable", "true");

    // debezium unwrap message
    s3Test.put("debezium.transforms", "unwrap");
    s3Test.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    s3Test.put("debezium.transforms.unwrap.add.fields", "op,table,lsn,source.ts_ms");
    s3Test.put("debezium.transforms.unwrap.add.headers", "db");
    s3Test.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");

    // DEBEZIUM SOURCE conf
    s3Test.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
    s3Test.put("debezium.source.offset.flush.interval.ms", "0");
    s3Test.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
    // this set by TestDatabase
    s3Test.put("debezium.source.database.port", Integer.toString(5432));
    s3Test.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
    s3Test.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
    s3Test.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
    s3Test.put("debezium.source.database.server.name", "testc");
    s3Test.put("debezium.source.schema.whitelist", "inventory");
    s3Test.put("debezium.source.table.whitelist", "inventory.customers,inventory.orders,inventory.products," +
        "inventory.geom,inventory.table_datatypes");

    config = s3Test;
  }
}
