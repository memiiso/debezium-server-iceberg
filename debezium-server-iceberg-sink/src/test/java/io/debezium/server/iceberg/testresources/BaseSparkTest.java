/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.IcebergUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeAll;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;

/**
 * Integration test that uses spark to consumer data is consumed.
 *
 * @author Ismail Simsek
 */
public class BaseSparkTest extends BaseTest {

  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local[2]");
  protected static SparkSession spark;

  @BeforeAll
  static void setup() {
    Awaitility.setDefaultTimeout(Duration.ofMinutes(3));
    Awaitility.setDefaultPollInterval(Duration.ofSeconds(10));
    sparkconf
        .set("spark.ui.enabled", "false")
        .set("spark.eventLog.enabled", "false")
        // enable iceberg SQL Extensions and Catalog
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        // catalog
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.defaultCatalog", "spark_catalog")
    ;

    // take current settings and use them for sparkconf
    Map<String, String> catalogConf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), "debezium.sink.iceberg.");
    catalogConf.forEach((key, value) -> {
//      System.out.println("spark.sql.catalog.spark_catalog."+key+"="+value);
      sparkconf.set("spark.sql.catalog.spark_catalog."+key, value);
    });

    BaseSparkTest.spark = SparkSession
        .builder()
        .config(BaseSparkTest.sparkconf)
        .getOrCreate();

    BaseSparkTest.spark.sparkContext().getConf().toDebugString();
  }

  public static String dataTypeString(Dataset<Row> dataset, String colName) {
    StructField[] fields = dataset.schema().fields();
    String dataType = null;
    for(StructField field: fields) {
      if(field.name().equals(colName)) {
        dataType =  field.dataType().typeName();
        break;
      }
    }
    return dataType;
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

  public int PGLoadTestDataTable(int numRows, boolean addRandomDelay) {
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

  public Dataset<Row> getTableData(String table) throws InterruptedException {
    return getTableData(ICEBERG_CATALOG_TABLE_NAMESPACE, "debeziumcdc_" + table);
  }

  public Dataset<Row> getTableData(String schema, String table) throws InterruptedException {
    table = schema + "." + table.replace(".", "_");
    return spark.newSession().sql("SELECT *, input_file_name() as input_file FROM " + table);
  }

}
