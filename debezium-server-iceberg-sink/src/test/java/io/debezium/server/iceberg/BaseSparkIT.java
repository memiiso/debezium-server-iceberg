/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
public class BaseSparkIT {
  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local");
  protected static SparkSession spark;

  @BeforeAll
  static void setup() {

    BaseSparkIT.sparkconf.set("spark.ui.enabled", "false");
    BaseSparkIT.sparkconf.set("spark.eventLog.enabled", "false");
    BaseSparkIT.spark = SparkSession
        .builder()
        .config(BaseSparkIT.sparkconf)
        .getOrCreate();
    BaseSparkIT.spark.sparkContext().setLogLevel("WARN");
  }

}
