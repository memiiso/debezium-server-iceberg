/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.JdbcCatalogDB;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourceMangoDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@Disabled // @TODO fix
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourceMangoDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = JdbcCatalogDB.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerMangodbTest.TestProfile.class)
public class IcebergChangeConsumerMangodbTest extends BaseSparkTest {

  @Test
  public void testSimpleUpload() {

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.products");
        df.show();
        return df.filter("_id is not null").count() >= 4;
      } catch (Exception e) {
        //e.printStackTrace();
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("quarkus.profile", "mongodb");
      config.put("%mongodb.debezium.source.connector.class", "io.debezium.connector.mongodb.MongoDbConnector");
      config.put("%mongodb.debezium.transforms.unwrap.type", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState");
      config.put("%mongodb.debezium.transforms.unwrap.add.fields", "op,source.ts_ms,db");
      config.put("%mongodb.debezium.sink.iceberg.allow-field-addition", "false");
      config.put("%mongodb.debezium.source.topic.prefix", "testc");
      config.put("%mongodb.debezium.source.database.include.list", "inventory"); // ok
      config.put("%mongodb.debezium.source.collection.include.list", "inventory.products");
      // IMPORTANT !!! FIX MongoDbConnector KEY FIELD NAME "id"=>"_id" !!!
      config.put("%mongodb.debezium.transforms", "unwrap,renamekeyfield");
      config.put("%mongodb.debezium.transforms.renamekeyfield.type",
          "org.apache.kafka.connect.transforms.ReplaceField$Key");
      config.put("%mongodb.debezium.transforms.renamekeyfield.renames", "id:_id");

      return config;
    }

    @Override
    public String getConfigProfile() {
      return "mongodb";
    }
  }

}
