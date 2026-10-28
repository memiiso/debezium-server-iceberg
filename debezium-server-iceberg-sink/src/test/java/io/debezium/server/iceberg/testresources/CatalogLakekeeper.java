/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.TestConfigSource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Lakekeeper REST catalog, with its Postgres database, storing the tables in the {@link S3Minio}
 * bucket. Must be started after {@link S3Minio}.
 */
public class CatalogLakekeeper implements QuarkusTestResourceLifecycleManager {
  public static final String LAKEKEEPER_IMAGE = "quay.io/lakekeeper/catalog:v0.14.2";
  public static final String WAREHOUSE = "lakekeeper_warehouse";
  private static final int PORT = 8181;

  private final Network network = Network.newNetwork();
  private final GenericContainer<?> db =
      new GenericContainer<>(DockerImageName.parse("postgres:17"))
          .withNetwork(network)
          .withNetworkAliases("db")
          .withEnv("POSTGRES_PASSWORD", "postgres")
          .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2));
  private GenericContainer<?> lakekeeper;

  private GenericContainer<?> lakekeeperContainer() {
    return new GenericContainer<>(DockerImageName.parse(LAKEKEEPER_IMAGE))
        .withNetwork(network)
        .withEnv("LAKEKEEPER__PG_ENCRYPTION_KEY", "This-is-NOT-Secure!")
        .withEnv(
            "LAKEKEEPER__PG_DATABASE_URL_READ", "postgresql://postgres:postgres@db:5432/postgres")
        .withEnv(
            "LAKEKEEPER__PG_DATABASE_URL_WRITE", "postgresql://postgres:postgres@db:5432/postgres");
  }

  @Override
  public Map<String, String> start() {
    db.start();
    // create the catalog schema then start the server
    try (GenericContainer<?> migrate =
        lakekeeperContainer()
            .withCommand("migrate")
            .withStartupCheckStrategy(
                new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(2)))) {
      migrate.start();
    }
    lakekeeper =
        lakekeeperContainer()
            .withCommand("serve")
            .withExposedPorts(PORT)
            .waitingFor(Wait.forHttp("/health").forPort(PORT));
    lakekeeper.start();

    String url =
        String.format("http://%s:%s", lakekeeper.getHost(), lakekeeper.getMappedPort(PORT));
    CatalogSetup.postJson(
        url + "/management/v1/bootstrap", "{\"accept-terms-of-use\": true}", null);
    CatalogSetup.postJson(
        url + "/management/v1/warehouse",
        """
        {
          "warehouse-name": "%s",
          "storage-profile": {
            "type": "s3",
            "bucket": "%s",
            "key-prefix": "lakekeeper",
            "endpoint": "%s",
            "region": "%s",
            "path-style-access": true,
            "flavor": "s3-compat",
            "sts-enabled": false
          },
          "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": "%s",
            "secret-access-key": "%s"
          }
        }
        """
            .formatted(
                WAREHOUSE,
                TestConfigSource.S3_BUCKET_NAME,
                S3Minio.getS3UrlFromContainers(),
                TestConfigSource.S3_REGION,
                TestConfigSource.S3_MINIO_ACCESS_KEY,
                TestConfigSource.S3_MINIO_SECRET_KEY),
        null);

    Map<String, String> config = new ConcurrentHashMap<>();
    config.put("debezium.sink.iceberg.type", "rest");
    config.put("debezium.sink.iceberg.uri", url + "/catalog");
    config.put("debezium.sink.iceberg.warehouse", WAREHOUSE);
    return config;
  }

  @Override
  public int order() {
    // the warehouse is created in the S3Minio bucket
    return 1;
  }

  @Override
  public void stop() {
    if (lakekeeper != null) {
      lakekeeper.stop();
    }
    db.stop();
    network.close();
  }
}
