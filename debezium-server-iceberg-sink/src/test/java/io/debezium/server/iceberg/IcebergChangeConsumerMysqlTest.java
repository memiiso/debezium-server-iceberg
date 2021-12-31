/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourceMysqlDB.class)
@TestProfile(IcebergChangeConsumerMysqlTestProfile.class)
public class IcebergChangeConsumerMysqlTest extends BaseSparkTest {

  @Test
  public void testSimpleUpload() throws Exception {

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.customers");
        return df.filter("id is not null").count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
    // S3Minio.listFiles();

    // create test table
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
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_delete_table");
        df.show();
        return df.count() == 20; // 4X3 insert 4X2 delete!
      } catch (Exception e) {
        return false;
      }
    });

  }

  @Test
  public void testFieldAddition() throws Exception {

    String sqlCreate = "CREATE TABLE IF NOT EXISTS inventory.test_field_addition (" +
                       " field1 INTEGER ," +
                       " field4 INTEGER ," +
                       " field5 TEXT," +
                       " PRIMARY KEY (field1)" +
                       " );";
    SourceMysqlDB.runSQL(sqlCreate);
    SourceMysqlDB.runSQL("INSERT INTO inventory.test_field_addition (field1, field4, field5) " +
                         "VALUES  (1,1,'data1');");
    SourceMysqlDB.runSQL("ALTER TABLE inventory.test_field_addition ADD COLUMN field2 TEXT AFTER field1;");
    SourceMysqlDB.runSQL("INSERT INTO inventory.test_field_addition (field1, field2, field4, field5) " +
                         "VALUES  (2,'data2',2,'data2');");
    SourceMysqlDB.runSQL("ALTER TABLE inventory.test_field_addition ADD COLUMN field6 TEXT;");
    SourceMysqlDB.runSQL("INSERT INTO inventory.test_field_addition (field1, field2, field4, field5, field6) " +
                         "VALUES  (3,'data3',3,'data3', 'data3');");


//    AtomicReference<Table> table = null;
//    Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
//      try {
//        table.set(getTable("testc.inventory.test_field_addition"));
//        return true;
//      } catch (Exception e) {
//        return false;
//      }
//    });
//    table.get().history().forEach(System.out::println);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_field_addition");
        df.show(false);
        return df.count() == 3;
      } catch (Exception e) {
        return false;
      }
    });
  }

}
