package io.debezium.server.iceberg;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_CATALOG_NAME;
import static io.debezium.server.iceberg.TestConfigSource.ICEBERG_WAREHOUSE_S3A;

@QuarkusTest
@TestProfile(GlobalConfigTest.TestProfile.class)
public class GlobalConfigTest extends BaseTest {

  @Test
  void configLoadsCorrectly() {
    Assertions.assertEquals(ICEBERG_CATALOG_NAME, config.iceberg().catalogName());
    // tests are running with false
    Assertions.assertEquals(false, config.iceberg().upsert());
    Assertions.assertEquals(ICEBERG_WAREHOUSE_S3A, config.iceberg().warehouseLocation());

    Assertions.assertTrue(config.iceberg().icebergConfigs().containsKey("warehouse"));
    Assertions.assertTrue(config.iceberg().icebergConfigs().containsValue(ICEBERG_WAREHOUSE_S3A));
    Assertions.assertTrue(config.iceberg().icebergConfigs().containsKey("table-namespace"));
    Assertions.assertTrue(config.iceberg().icebergConfigs().containsKey("catalog-name"));
    Assertions.assertTrue(config.iceberg().icebergConfigs().containsValue(ICEBERG_CATALOG_NAME));
    Assertions.assertEquals(Logger.Level.ERROR, config.quarkusLogLevel());
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("quarkus.log.level", "ERROR");
      return config;
    }
  }

}