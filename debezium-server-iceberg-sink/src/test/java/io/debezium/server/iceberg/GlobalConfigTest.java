package io.debezium.server.iceberg;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_NAME;
import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_WAREHOUSE_S3A;

@QuarkusTest
public class GlobalConfigTest {

  @Inject
  GlobalConfig conf;

  @Test
  void configLoadsCorrectly() {
    Assertions.assertEquals(ICEBERG_CATALOG_NAME, conf.iceberg().catalogName());
    // tests are running with false
    Assertions.assertEquals(false, conf.iceberg().upsert());
    Assertions.assertEquals(ICEBERG_WAREHOUSE_S3A, conf.iceberg().warehouseLocation());

    Assertions.assertTrue(conf.iceberg().icebergConfigs().containsKey("warehouse"));
    Assertions.assertTrue(conf.iceberg().icebergConfigs().containsValue(ICEBERG_WAREHOUSE_S3A));
    Assertions.assertTrue(conf.iceberg().icebergConfigs().containsKey("table-namespace"));
    Assertions.assertTrue(conf.iceberg().icebergConfigs().containsKey("catalog-name"));
    Assertions.assertTrue(conf.iceberg().icebergConfigs().containsValue(ICEBERG_CATALOG_NAME));
  }

}