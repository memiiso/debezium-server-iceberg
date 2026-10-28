/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.server.iceberg.TestConfigSource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Apache Polaris REST catalog, storing the tables in the {@link S3Minio} bucket. Must be started
 * after {@link S3Minio}.
 */
public class CatalogPolaris implements QuarkusTestResourceLifecycleManager {
  public static final String POLARIS_IMAGE = "apache/polaris:1.8.0";
  public static final String CATALOG = "polaris_catalog";
  private static final String CREDENTIAL = "root:s3cr3t";
  private static final int PORT = 8181;
  private static final int MANAGEMENT_PORT = 8182;

  private final GenericContainer<?> polaris =
      new GenericContainer<>(DockerImageName.parse(POLARIS_IMAGE))
          .withExposedPorts(PORT, MANAGEMENT_PORT)
          .withEnv("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS," + CREDENTIAL.replace(':', ','))
          .withEnv("polaris.realm-context.realms", "POLARIS")
          .withEnv("quarkus.otel.sdk.disabled", "true")
          // used by Polaris to write the table metadata
          .withEnv("AWS_REGION", TestConfigSource.S3_REGION)
          .withEnv("AWS_ACCESS_KEY_ID", TestConfigSource.S3_MINIO_ACCESS_KEY)
          .withEnv("AWS_SECRET_ACCESS_KEY", TestConfigSource.S3_MINIO_SECRET_KEY)
          .waitingFor(Wait.forHttp("/q/health").forPort(MANAGEMENT_PORT));

  @Override
  public Map<String, String> start() {
    polaris.start();
    String url = String.format("http://%s:%s", polaris.getHost(), polaris.getMappedPort(PORT));
    String token = token(url);

    String location = "s3://" + TestConfigSource.S3_BUCKET_NAME + "/polaris";
    CatalogSetup.postJson(
        url + "/api/management/v1/catalogs",
        """
        {
          "catalog": {
            "name": "%s",
            "type": "INTERNAL",
            "readOnly": false,
            "properties": {"default-base-location": "%s"},
            "storageConfigInfo": {
              "storageType": "S3",
              "allowedLocations": ["%s"],
              "endpoint": "%s",
              "endpointInternal": "%s",
              "stsUnavailable": true,
              "pathStyleAccess": true,
              "region": "%s"
            }
          }
        }
        """
            .formatted(
                CATALOG,
                location,
                location,
                S3Minio.getS3UrlFromContainers(),
                S3Minio.getS3UrlFromContainers(),
                TestConfigSource.S3_REGION),
        token);
    CatalogSetup.send(
        "PUT",
        url + "/api/management/v1/catalogs/" + CATALOG + "/catalog-roles/catalog_admin/grants",
        "application/json",
        "{\"type\":\"catalog\", \"privilege\":\"CATALOG_MANAGE_CONTENT\"}",
        token);

    Map<String, String> config = new ConcurrentHashMap<>();
    config.put("debezium.sink.iceberg.type", "rest");
    config.put("debezium.sink.iceberg.uri", url + "/api/catalog");
    config.put("debezium.sink.iceberg.oauth2-server-uri", url + "/api/catalog/v1/oauth/tokens");
    config.put("debezium.sink.iceberg.credential", CREDENTIAL);
    config.put("debezium.sink.iceberg.scope", "PRINCIPAL_ROLE:ALL");
    config.put("debezium.sink.iceberg.warehouse", CATALOG);
    return config;
  }

  private static String token(String url) {
    String[] credential = CREDENTIAL.split(":");
    String response =
        CatalogSetup.send(
            "POST",
            url + "/api/catalog/v1/oauth/tokens",
            "application/x-www-form-urlencoded",
            "grant_type=client_credentials&client_id="
                + credential[0]
                + "&client_secret="
                + credential[1]
                + "&scope=PRINCIPAL_ROLE:ALL",
            null);
    try {
      return new ObjectMapper().readTree(response).get("access_token").asText();
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to read Polaris token", e);
    }
  }

  @Override
  public int order() {
    // the catalog is created in the S3Minio bucket
    return 1;
  }

  @Override
  public void stop() {
    polaris.stop();
  }
}
