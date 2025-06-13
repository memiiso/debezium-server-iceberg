/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.sql.SQLException;
import java.time.Duration;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to iceberg destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class IcebergChangeConsumerNessieCatalogTest extends BaseSparkTest {

  @Test
  public void testSimpleUpload() throws InterruptedException, SQLException, ClassNotFoundException {
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.customers");
        df.show(false);
        return df.count() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    int startId = 1005;
    int numberOfRecords = 5; // You can change this value
    for (int i = 0; i < numberOfRecords; i++) {
      int currentId = startId + i;
      String firstName = "FirstName" + currentId;
      String lastName = "LastName" + currentId;
      String email = "user" + currentId + "@example.com";
      String insertStatement = String.format(
          "INSERT INTO inventory.customers (id, first_name, last_name, email) VALUES (%d, '%s', '%s', '%s');",
          currentId,
          firstName,
          lastName,
          email
      );
      SourcePostgresqlDB.runSQL(insertStatement);
      Thread.sleep(3000);
      Dataset<Row> df = getTableData("testc.inventory.customers");
      df.show(false);
    }
  }


}
