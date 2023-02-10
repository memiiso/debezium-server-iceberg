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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;
import static io.debezium.server.iceberg.TestConfigSource.S3_BUCKET;

/**
 * Integration test that uses spark to consumer data is consumed.
 *
 * @author Ismail Simsek
 */
public class BaseSparkTest {

  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local[2]");
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static SparkSession spark;
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.iceberg.warehouse")
  String warehouseLocation;
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;

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
        //.set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
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
                 "        CREATE TABLE IF NOT EXISTS inventory.test_data (\n" +
                 "            c_id INTEGER ,\n" +
                 "            c_text TEXT,\n" +
                 "            c_varchar VARCHAR" +
                 "          );";
    SourcePostgresqlDB.runSQL(sql);
  }

  public static int PGLoadTestDataTable(int numRows) throws Exception {
    return PGLoadTestDataTable(numRows, false);
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

  public static void mysqlCreateTestDataTable() throws Exception {
    // create test table
    String sql = "\n" +
                 "        CREATE TABLE IF NOT EXISTS inventory.test_data (\n" +
                 "            c_id INTEGER ,\n" +
                 "            c_text TEXT,\n" +
                 "            c_varchar TEXT\n" +
                 "          );";
    SourceMysqlDB.runSQL(sql);
  }

  public static int mysqlLoadTestDataTable(int numRows) throws Exception {
    int numInsert = 0;
    do {
      String sql = "INSERT INTO inventory.test_data (c_id, c_text, c_varchar ) " +
                   "VALUES ";
      StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
      for (int i = 0; i < 10; i++) {
        values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
      }
      SourceMysqlDB.runSQL(sql + values);
      numInsert += 10;
    } while (numInsert <= numRows);
    return numInsert;
  }

  protected org.apache.iceberg.Table getTable(String table) {
    HadoopCatalog catalog = getIcebergCatalog();
    return catalog.loadTable(TableIdentifier.of(Namespace.of(namespace), tablePrefix + table.replace(".", "_")));
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
    table = "debeziumevents.debeziumcdc_" + table.replace(".", "_");
    //System.out.println("--loading-->" + table);
    return spark.newSession().sql("SELECT *, input_file_name() as input_file FROM " + table);
  }

}
