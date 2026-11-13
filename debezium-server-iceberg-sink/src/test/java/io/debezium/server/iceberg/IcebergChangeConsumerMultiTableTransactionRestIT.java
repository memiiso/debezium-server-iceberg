/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.CatalogRest;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

/**
 * Runs {@link IcebergChangeConsumerRestCatalogIT} with the multi-table transaction enabled, the
 * parent class keeps testing the default configuration.
 */
@QuarkusTest
@QuarkusTestResource(value = CatalogRest.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergChangeConsumerMultiTableTransactionRestIT.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class IcebergChangeConsumerMultiTableTransactionRestIT
    extends IcebergChangeConsumerRestCatalogIT {

  @Test
  public void testMultiTableTransactionEnabled() {
    // the inherited tests would silently run the default configuration if the profile was not
    // applied
    Assertions.assertTrue(consumer.config.iceberg().multiTableTransaction());
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("debezium.sink.iceberg.multi-table-transaction", "true");
    }
  }
}
