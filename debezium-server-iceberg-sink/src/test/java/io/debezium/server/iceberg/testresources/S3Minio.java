/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.server.iceberg.TestConfigSource;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

public class S3Minio implements QuarkusTestResourceLifecycleManager {

  protected static final Logger LOGGER = LoggerFactory.getLogger(S3Minio.class);
  static final String DEFAULT_IMAGE = "pgsty/silo:RELEASE.2026-09-16T00-00-00Z";
  public static MinioClient client;

  public static final MinIOContainer container =
      new MinIOContainer(
              DockerImageName.parse(DEFAULT_IMAGE).asCompatibleSubstituteFor("minio/minio"))
          .withUserName(TestConfigSource.S3_MINIO_ACCESS_KEY)
          .withPassword(TestConfigSource.S3_MINIO_SECRET_KEY);

  public static void listFiles() {
    LOGGER.info("-----------------------------------------------------------------");
    try {
      List<Bucket> bucketList = client.listBuckets();
      for (Bucket bucket : bucketList) {
        System.out.printf("Bucket:%s ROOT\n", bucket.name());
        Iterable<Result<Item>> results =
            client.listObjects(
                ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
        for (Result<Item> result : results) {
          Item item = result.get();
          System.out.printf(
              "Bucket:%s Item:%s Size:%s\n", bucket.name(), item.objectName(), item.size());
        }
      }
    } catch (Exception e) {
      LOGGER.info("Failed listing bucket");
    }
    LOGGER.info("-----------------------------------------------------------------");
  }

  @Override
  public void stop() {
    container.stop();
  }

  /**
   * S3 url reachable from the other containers and from the test JVM. Used by the catalogs, which
   * write to the bucket themselves and return this url to the clients.
   *
   * <p>The containers reach the exposed host port through {@code host.testcontainers.internal}, the
   * test JVM resolves the same name to the loopback address with {@link
   * TestcontainersHostResolverProvider}. Must be called before starting the containers using it.
   */
  public static String getS3UrlFromContainers() {
    int port = container.getMappedPort(9000);
    Testcontainers.exposeHostPorts(port);
    return String.format("http://%s:%s", GenericContainer.INTERNAL_HOST_HOSTNAME, port);
  }

  public static String getS3WebURL() {
    return String.format("http://%s:%s", container.getHost(), container.getMappedPort(9001));
  }

  @Override
  public Map<String, String> start() {
    container.start();
    client =
        MinioClient.builder()
            .endpoint(container.getS3URL())
            .credentials(container.getUserName(), container.getPassword())
            .build();

    try {
      client.ignoreCertCheck();
      client.makeBucket(
          MakeBucketArgs.builder()
              .region(TestConfigSource.S3_REGION)
              .bucket(TestConfigSource.S3_BUCKET_NAME)
              .build());
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOGGER.info(
        "Minio Started\nMinio UI: {}\nMinio S3 URL: {}", getS3WebURL(), container.getS3URL());
    Map<String, String> config = new ConcurrentHashMap<>();
    // FOR JDBC CATALOG
    config.put("debezium.sink.iceberg.s3.endpoint", container.getS3URL());
    config.put("debezium.sink.iceberg.s3.path-style-access", "true");
    config.put("debezium.sink.iceberg.s3.access-key-id", TestConfigSource.S3_MINIO_ACCESS_KEY);
    config.put("debezium.sink.iceberg.s3.secret-access-key", TestConfigSource.S3_MINIO_SECRET_KEY);
    config.put("debezium.sink.iceberg.client.region", TestConfigSource.S3_REGION);
    config.put("debezium.sink.iceberg.io-impl", TestConfigSource.ICEBERG_FILEIO);

    return config;
  }
}
