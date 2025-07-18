/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.history;

import io.debezium.server.iceberg.BaseSparkTest;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_TABLE_NAMESPACE;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergSchemaHistoryTest.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
public class IcebergSchemaHistoryTest extends BaseSparkTest {
  @Test
  public void testSimpleUpload() throws SQLException, ClassNotFoundException {
    String sqlCreate = "CREATE TABLE IF NOT EXISTS inventory.test_schema_history_ddl (" +
        " c_id INTEGER ," +
        " c_data TEXT," +
        "  PRIMARY KEY (c_id)" +
        " );";
    SourceMysqlDB.runSQL(sqlCreate);
    String sqlInsert =
        "INSERT INTO inventory.test_schema_history_ddl (c_id, c_data ) " +
            "VALUES  (1,'data-1'),(2,'data-2'),(3,'data-3');";
    SourceMysqlDB.runSQL(sqlInsert);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.test_schema_history_ddl");
        return ds.count() >= 2;
      } catch (Exception e) {
        return false;
      }
    });

    // test nested data(struct) consumed
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> ds = getTableData(ICEBERG_CATALOG_TABLE_NAMESPACE, "debezium_database_history_storage_table");
        ds.show(10, false);
        return ds.count() > 1
            && ds.where("history_data ILIKE '%CREATE%TABLE%test_schema_history_ddl%'").count() == 1
            ;
      } catch (Exception e) {
        return false;
      }
    });
  }


  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("quarkus.profile", "mysql");
      config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
//      config.put("%mysql.debezium.source.table.whitelist", "inventory.*");
      config.put("debezium.source.schema.history.internal", "io.debezium.server.iceberg.history.IcebergSchemaHistory");
      config.put("debezium.source.schema.history.internal.iceberg.table-name", "debezium_database_history_storage_table");
      config.put("debezium.source.table.whitelist", "inventory.test_schema_history_ddl");
      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }
}