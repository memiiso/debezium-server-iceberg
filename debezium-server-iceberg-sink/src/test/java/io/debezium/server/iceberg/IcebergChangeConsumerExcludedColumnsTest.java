/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test that verifies columns can be excluded from the written iceberg table
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerExcludedColumnsTest.TestProfile.class)
public class IcebergChangeConsumerExcludedColumnsTest extends BaseSparkTest {

  @Test
  public void testSupportExcludedColumns() throws Exception {
    String sql =
            "DROP TABLE IF EXISTS inventory.table_with_excluded_column;\n" +
            "CREATE TABLE IF NOT EXISTS inventory.table_with_excluded_column (\n" +
            "    c_id INTEGER ,\n" +
            "    c_text TEXT ,\n" +
            "    c_exclude_me TEXT\n" +
            ");";
    SourcePostgresqlDB.runSQL(sql);
    sql = "INSERT INTO inventory.table_with_excluded_column \n" +
          "(c_id, c_text, c_exclude_me) \n" +
          "VALUES \n" +
          "(1, 'one' , 'should_not_write_to_iceberg' ) \n" +
          ",(1, 'two' , 'should_not_write_to_iceberg' )";

    SourcePostgresqlDB.runSQL(sql);
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.table_with_excluded_column");
        df.show(false);
        df.schema().printTreeString();

        List<String> columns = Arrays.asList(df.columns());

        Assertions.assertTrue(columns.contains("c_id"));
        Assertions.assertTrue(columns.contains("c_text"));
        Assertions.assertFalse(columns.contains("c_exclude_me"));
        Assertions.assertFalse(columns.contains("__table"));
        Assertions.assertFalse(columns.contains("__db"));
        return true;
      } catch (Exception e) {
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.excluded-columns", "c_exclude_me,__table,__db");
      return config;
    }
  }
}
