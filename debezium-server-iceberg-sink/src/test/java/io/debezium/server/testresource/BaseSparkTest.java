/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.testresource;

import io.debezium.server.iceberg.ConfigSource;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.util.Testing;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeAll;
import static io.debezium.server.iceberg.ConfigSource.S3_BUCKET;
import static io.debezium.server.testresource.TestUtil.randomInt;
import static io.debezium.server.testresource.TestUtil.randomString;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
public class BaseSparkTest {
  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local[2]");
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static SparkSession spark;

  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  // @ConfigProperty(name = "debezium.sink.iceberg.bucket-name", defaultValue = "")
  String bucket = S3_BUCKET;

  @BeforeAll
  static void setup() {
    Map<String, String> appSparkConf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(BaseSparkTest.sparkconf::set);
    sparkconf
        .set("spark.ui.enabled", "false")
        .set("spark.eventLog.enabled", "false")
        .set("spark.hadoop.fs.s3a.access.key", S3Minio.MINIO_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", S3Minio.MINIO_SECRET_KEY)
        // minio specific setting using minio as S3
        .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + S3Minio.getMappedPort())
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        // enable iceberg SQL Extensions
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
        .set("spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse")
        .set("spark.sql.warehouse.dir", "s3a://" + S3_BUCKET + "/iceberg_warehouse");

    BaseSparkTest.spark = SparkSession
        .builder()
        .config(BaseSparkTest.sparkconf)
        .getOrCreate();

    BaseSparkTest.spark.sparkContext().getConf().toDebugString();

  }

  public static void PGCreateTestDataTable() throws Exception {
    // create test table
    String sql = "" +
        "        CREATE TABLE IF NOT EXISTS inventory.test_date_table (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar VARCHAR" +
        "          );";
    SourcePostgresqlDB.runSQL(sql);
  }

  public static int PGLoadTestDataTable(int numRows) throws Exception {
    return PGLoadTestDataTable(numRows, false);
  }

  public static int PGLoadTestDataTable(int numRows, boolean addRandomDelay) throws Exception {
    int numInsert = 0;
    do {

      new Thread(() -> {
        try {
          if (addRandomDelay) {
            Thread.sleep(randomInt(20000, 100000));
          }
          String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) " +
              "VALUES ";
          StringBuilder values = new StringBuilder("\n(" + randomInt(15, 32) + ", '" + randomString(524) + "', '" + randomString(524) + "')");
          for (int i = 0; i < 100; i++) {
            values.append("\n,(").append(randomInt(15, 32)).append(", '").append(randomString(524)).append("', '").append(randomString(524)).append("')");
          }
          SourcePostgresqlDB.runSQL(sql + values);
          SourcePostgresqlDB.runSQL("COMMIT;");
        } catch (Exception e) {
          Thread.currentThread().interrupt();
        }
      }).start();

      numInsert += 100;
    } while (numInsert <= numRows);
    return numInsert;
  }

  public static void mysqlCreateTestDataTable() throws Exception {
    // create test table
    String sql = "\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.test_date_table (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar TEXT\n" +
        "          );";
    SourceMysqlDB.runSQL(sql);
  }

  public static int mysqlLoadTestDataTable(int numRows) throws Exception {
    int numInsert = 0;
    do {
      String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) " +
          "VALUES ";
      StringBuilder values = new StringBuilder("\n(" + randomInt(15, 32) + ", '" + randomString(524) + "', '" + randomString(524) + "')");
      for (int i = 0; i < 10; i++) {
        values.append("\n,(").append(randomInt(15, 32)).append(", '").append(randomString(524)).append("', '").append(randomString(524)).append("')");
      }
      SourceMysqlDB.runSQL(sql + values);
      numInsert += 10;
    } while (numInsert <= numRows);
    return numInsert;
  }

  public Dataset<Row> getTableData(String table) {
    return spark.newSession().sql("SELECT * FROM default.debeziumcdc_" + table.replace(".", "_"));
  }

}
