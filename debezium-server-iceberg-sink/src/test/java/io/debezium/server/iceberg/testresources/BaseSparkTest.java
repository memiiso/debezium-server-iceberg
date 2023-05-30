/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.IcebergUtil;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeAll;
import static io.debezium.server.iceberg.TestConfigSource.*;

/**
 * Integration test that uses spark to consumer data is consumed.
 *
 * @author Ismail Simsek
 */
public class BaseSparkTest extends BaseTest {

  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local[2]");
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static SparkSession spark;

  @BeforeAll
  static void setup() {
    Map<String, String> appSparkConf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(BaseSparkTest.sparkconf::set);
    sparkconf
        .set("spark.ui.enabled", "false")
        .set("spark.eventLog.enabled", "false")
        // enable iceberg SQL Extensions and Catalog
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.defaultCatalog", ICEBERG_CATALOG_NAME)
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".warehouse", S3_BUCKET)
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".cache-enabled", "false")
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".catalog-impl", JdbcCatalog.class.getName())
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".default-namespaces", CATALOG_TABLE_NAMESPACE)
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".uri", JdbcCatalogDB.container.getJdbcUrl())
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".jdbc.user", JdbcCatalogDB.container.getUsername())
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".jdbc.password", JdbcCatalogDB.container.getPassword())
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".s3.endpoint", "http://localhost:" + S3Minio.getMappedPort().toString())
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".s3.path-style-access", "true")
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".s3.access-key-id", S3Minio.MINIO_ACCESS_KEY)
        .set("spark.sql.catalog." + ICEBERG_CATALOG_NAME + ".s3.secret-access-key", S3Minio.MINIO_SECRET_KEY)
    ;

    BaseSparkTest.spark = SparkSession
        .builder()
        .config(BaseSparkTest.sparkconf)
        .getOrCreate();

    BaseSparkTest.spark.sparkContext().getConf().toDebugString();

  }

  public static void PGCreateTestDataTable() throws Exception {
    // create test table
    String sql = "" +
                 "        CREATE TABLE IF NOT EXISTS inventory.test_data (\n" +
                 "            c_id INTEGER ,\n" +
                 "            c_text TEXT,\n" +
                 "            c_varchar VARCHAR" +
                 "          );";
    SourcePostgresqlDB.runSQL(sql);
  }

  public static int PGLoadTestDataTable(int numRows, boolean addRandomDelay) {
    int numInsert = 0;
    do {

      new Thread(() -> {
        try {
          if (addRandomDelay) {
            Thread.sleep(TestUtil.randomInt(20000, 100000));
          }
          String sql = "INSERT INTO inventory.test_data (c_id, c_text, c_varchar ) " +
                       "VALUES ";
          StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
          for (int i = 0; i < 100; i++) {
            values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
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

  protected HadoopCatalog getIcebergCatalog() {
    // loop and set hadoopConf
    Configuration hadoopConf = new Configuration();
    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith("debezium.sink.iceberg.")) {
        hadoopConf.set(name.substring("debezium.sink.iceberg.".length()),
            ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    HadoopCatalog icebergCatalog = new HadoopCatalog();
    icebergCatalog.setConf(hadoopConf);

    Map<String, String> configMap = new HashMap<>();
    hadoopConf.forEach(e -> configMap.put(e.getKey(), e.getValue()));
    icebergCatalog.initialize("iceberg", configMap);
    return icebergCatalog;
  }

  public Dataset<Row> getTableData(String table) {
    table = CATALOG_TABLE_NAMESPACE + ".debeziumcdc_" + table.replace(".", "_");
    return spark.newSession().sql("SELECT *, input_file_name() as input_file FROM " + table);
  }

}
