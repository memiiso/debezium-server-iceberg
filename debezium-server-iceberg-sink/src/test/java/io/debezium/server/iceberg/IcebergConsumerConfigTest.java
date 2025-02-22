package io.debezium.server.iceberg;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_NAME;
import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_WAREHOUSE_S3A;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class IcebergConsumerConfigTest {

  @Inject
  IcebergConsumerConfig conf;

  @Test
  void configLoadsCorrectly() {
    Assertions.assertEquals(ICEBERG_CATALOG_NAME, conf.catalogName());
    // tests are running with false
    Assertions.assertEquals(false, conf.upsert());
    Assertions.assertEquals(ICEBERG_WAREHOUSE_S3A, conf.warehouseLocation());

    Assertions.assertTrue(conf.icebergConfigs().containsKey("warehouse"));
    Assertions.assertTrue(conf.icebergConfigs().containsValue(ICEBERG_WAREHOUSE_S3A));
    Assertions.assertTrue(conf.icebergConfigs().containsKey("table-namespace"));
    Assertions.assertTrue(conf.icebergConfigs().containsKey("catalog-name"));
    Assertions.assertTrue(conf.icebergConfigs().containsValue(ICEBERG_CATALOG_NAME));
  }

}