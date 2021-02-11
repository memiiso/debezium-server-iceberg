/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.testresource.TestS3Minio;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
public class BaseSparkIT {
  protected static SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local");
  protected static SparkSession spark;

  @BeforeAll
  static void setup() {

    BaseSparkIT.sparkconf.set("spark.ui.enabled", "false");
    BaseSparkIT.sparkconf.set("spark.eventLog.enabled", "false");
//    BaseSparkIT.sparkconf.set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
//    //BaseSparkIT.sparkconf.set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkSessionCatalog");
//    BaseSparkIT.sparkconf.set("spark.sql.catalog.mycat.type", "hadoop");
//    BaseSparkIT.sparkconf.set("spark.sql.catalog.mycat.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");
//    BaseSparkIT.sparkconf.set("spark.sql.catalog.mycat.warehouse", "s3a://" + ConfigSource.S3_BUCKET +
//        "/iceberg_warehouse");
    BaseSparkIT.sparkconf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
    BaseSparkIT.sparkconf.set("spark.sql.catalog.hadoop_prod.type", "hadoop");
    BaseSparkIT.sparkconf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
    BaseSparkIT.sparkconf.set("spark.sql.catalog.hadoop_prod.warehouse", "s3a://" + ConfigSource.S3_BUCKET +
        "/iceberg_warehouse");

    BaseSparkIT.sparkconf.set("spark.sql.session.timeZone", "UTC");
    BaseSparkIT.sparkconf.set("user.timezone", "UTC");
    BaseSparkIT.sparkconf.set("com.amazonaws.services.s3.enableV4", "true");
    BaseSparkIT.sparkconf.set("com.amazonaws.services.s3a.enableV4", "true");
    BaseSparkIT.sparkconf.set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true");
    BaseSparkIT.sparkconf.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true");
    BaseSparkIT.sparkconf.set("spark.io.compression.codec", "snappy");
    BaseSparkIT.sparkconf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    BaseSparkIT.sparkconf.set("spark.hadoop.fs.s3a.access.key", TestS3Minio.MINIO_ACCESS_KEY);
    BaseSparkIT.sparkconf.set("spark.hadoop.fs.s3a.secret.key", TestS3Minio.MINIO_SECRET_KEY);
    BaseSparkIT.sparkconf.set("spark.hadoop.fs.s3a.path.style.access", "true");
    BaseSparkIT.sparkconf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + TestS3Minio.MINIO_MAPPED_PORT); // minio specific
    BaseSparkIT.sparkconf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");


    BaseSparkIT.spark = SparkSession
        .builder()
        .config(BaseSparkIT.sparkconf)
        .getOrCreate();
    BaseSparkIT.spark.sparkContext().setLogLevel("WARN");
  }

}
