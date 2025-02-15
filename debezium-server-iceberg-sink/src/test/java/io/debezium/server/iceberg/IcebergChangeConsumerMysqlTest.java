/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.collect.Lists;
import io.debezium.server.iceberg.testresources.BaseTest;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerMysqlTest.TestProfile.class)
public class IcebergChangeConsumerMysqlTest extends BaseTest {

  @Test
  public void testSimpleUpload() throws Exception {
    String sqlCreate = "CREATE TABLE IF NOT EXISTS inventory.test_delete_table (" +
                       " c_id INTEGER ," +
                       " c_id2 INTEGER ," +
                       " c_data TEXT," +
                       "  PRIMARY KEY (c_id, c_id2)" +
                       " );";
    String sqlInsert =
        "INSERT INTO inventory.test_delete_table (c_id, c_id2, c_data ) " +
        "VALUES  (1,1,'data'),(1,2,'data'),(1,3,'data'),(1,4,'data') ;";
    String sqlDelete = "DELETE FROM inventory.test_delete_table where c_id = 1 ;";

    SourceMysqlDB.runSQL(sqlCreate);
    SourceMysqlDB.runSQL(sqlInsert);
    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {

        CloseableIterable<Record> result = getTableDataV2("testc.inventory.test_delete_table");
        return Lists.newArrayList(result).size() == 4;
      } catch (Exception e) {
        return false;
      }
    });

    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        CloseableIterable<Record> result = getTableDataV2("testc.inventory.test_delete_table");
        //result.forEach(System.out::println);
        //System.out.println("======================");
        return Lists.newArrayList(result).size() >= 20;
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
      config.put("%mysql.debezium.source.table.whitelist", "inventory.customers,inventory.test_delete_table");
      //config.put("%mysql.debezium.source.include.schema.changes", "false");
      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mysql";
    }
  }

}
