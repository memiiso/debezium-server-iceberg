/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.testresource.TestS3Minio;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseSparkIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSparkIT.class);
  SparkConf sparkconf = new SparkConf();
  SparkSession spark;
  String warehousePath = "s3a://" + ConfigSource.S3_BUCKET + "/iceberg_warehouse";
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  Map<String, String> icebergOptions = new ConcurrentHashMap<>();

  @BeforeAll
  public void setup() {

    sparkconf
        .setAppName("CDC-S3-Batch-Spark-Sink")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.eventLog.enabled", "false")
        .set("spark.hadoop.fs.s3a.access.key", TestS3Minio.MINIO_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", TestS3Minio.MINIO_SECRET_KEY)
        // minio specific setting using minio as S3
        .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + TestS3Minio.MINIO_MAPPED_PORT)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        // enable iceberg SQL Extensions
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
        .set("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
        .set("spark.sql.warehouse.dir", warehousePath)
    ;

    icebergOptions.put("warehouse", warehousePath);
    icebergOptions.put("type", "hadoop");
    icebergOptions.put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");

    spark = SparkSession
        .builder()
        .config(sparkconf)
        .getOrCreate();
    //spark.sparkContext().setLogLevel("INFO");
    // mini test
    List<String> jsonData = Arrays
        .asList("{'name':'User-1', 'age':1122}\n{'name':'User-2', 'age':1130}\n{'name':'User-3', 'age':1119}"
            .split(IOUtils.LINE_SEPARATOR));
    Dataset<String> ds = this.spark.createDataset(jsonData, Encoders.STRING());
    spark.read().json(ds).toDF().show();

    LOGGER.warn("Spark Version:{}", spark.version());
  }

  public Dataset<Row> getTableData(String table) {

    table = tablePrefix + table.replace(".", "-");
    LOGGER.debug("Reading table {}", table);

    try {
      spark.sql("select * from default.`" + table + "`").show();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      spark.sql("select * from `" + table + "`").show();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      spark.table("`" + table + "`").show();
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.exit(1);
    return null;
  }

}
