package io.debezium.server.iceberg;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.debezium.server.iceberg.testresources.TestUtil;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerUpsertV3Test.TestProfile.class)
public class IcebergChangeConsumerUpsertV3Test extends BaseSparkTest {

  @Test
  public void testSimpleUpsertV3() throws Exception {
    String dest = "testc.inventory.customers_upsert_v3";

    // test simple inserts
    List<EmbeddedEngineChangeEvent> records = new ArrayList<>();
    records.add(eventFactory.of(dest, 1, "c"));
    records.add(eventFactory.of(dest, 2, "c"));
    records.add(eventFactory.of(dest, 3, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    Dataset<Row> ds = getTableData("testc.inventory.customers_upsert_v3");
    Assertions.assertEquals(ds.count(), 3);

    // Verify created table version is 3
    TableIdentifier tableId = consumer.mapDestination("testc.inventory.customers_upsert_v3");
    Table table = consumer.loadIcebergTable(tableId, null);
    Assertions.assertEquals(3, TableUtil.formatVersion(table));

    // Perform update/delete
    records.clear();
    records.add(eventFactory.of(dest, 1, "r"));
    records.add(eventFactory.of(dest, 2, "d"));
    records.add(eventFactory.of(dest, 3, "u", "UpdatednameV1"));
    records.add(eventFactory.of(dest, 4, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());

    ds = getTableData("testc.inventory.customers_upsert_v3");
    Assertions.assertEquals(4, ds.count());
    Assertions.assertEquals(ds.where("id = 2 AND __op = 'd'").count(), 1);
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.upsert", "true");
      config.put("debezium.sink.iceberg.upsert-keep-deletes", "true");
      config.put("debezium.sink.iceberg.format-version", "3");
      return config;
    }
  }
}
