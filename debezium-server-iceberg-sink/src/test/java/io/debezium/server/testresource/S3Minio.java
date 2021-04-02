/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.testresource;

import io.debezium.server.iceberg.ConfigSource;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class S3Minio implements QuarkusTestResourceLifecycleManager {

  public static final String MINIO_ACCESS_KEY = "admin";
  public static final String MINIO_SECRET_KEY = "12345678";
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3Minio.class);
  static final int MINIO_DEFAULT_PORT = 9000;
  static final String DEFAULT_IMAGE = "minio/minio:latest";
  static final String DEFAULT_STORAGE_DIRECTORY = "/data";
  static final String HEALTH_ENDPOINT = "/minio/health/ready";
  public static MinioClient client;


  static private final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(DEFAULT_IMAGE))
      .waitingFor(new HttpWaitStrategy()
          .forPath(HEALTH_ENDPOINT)
          .forPort(MINIO_DEFAULT_PORT)
          .withStartupTimeout(Duration.ofSeconds(30)))
      .withEnv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
      .withEnv("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
      .withEnv("MINIO_REGION_NAME", ConfigSource.S3_REGION)
      .withCommand("server " + DEFAULT_STORAGE_DIRECTORY);

  public static List<Item> getObjectList(String bucketName) {
    List<Item> objects = new ArrayList<>();

    try {
      Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder().bucket(bucketName).recursive(true).build());
      for (Result<Item> result : results) {
        Item item = result.get();
        objects.add(item);
      }
    } catch (Exception e) {
      LOGGER.info("Failed listing bucket");
    }
    return objects;
  }

  public static void listFiles() {
    LOGGER.info("-----------------------------------------------------------------");
    try {
      List<Bucket> bucketList = client.listBuckets();
      for (Bucket bucket : bucketList) {
        LOGGER.info("Bucket:{} ROOT", bucket.name());
        Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
        for (Result<Item> result : results) {
          Item item = result.get();
          LOGGER.info("Bucket:{} Item:{} Size:{}", bucket.name(), item.objectName(), item.size());
        }
      }
    } catch (Exception e) {
      LOGGER.info("Failed listing bucket");
    }
    LOGGER.info("-----------------------------------------------------------------");

  }

  public static List<Item> getIcebergDataFiles(String bucketName) {
    List<Item> objects = new ArrayList<>();
    try {
      List<Item> results = getObjectList(bucketName);
      for (Item result : results) {
        if (result.objectName().contains("/data/") && result.objectName().endsWith("parquet")) {
          objects.add(result);
        }
      }
    } catch (Exception e) {
      LOGGER.info("Failed listing bucket");
    }
    return objects;
  }

  public static Integer getMappedPort() {
    return container.getMappedPort(MINIO_DEFAULT_PORT);
  }

  @Override
  public void stop() {
    container.stop();
  }

  @Override
  public Map<String, String> start() {
    container.start();

    client = MinioClient.builder()
        .endpoint("http://" + container.getHost() + ":" + container.getMappedPort(MINIO_DEFAULT_PORT))
        .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        .build();
    try {
      client.ignoreCertCheck();
      client.makeBucket(MakeBucketArgs.builder()
          .region(ConfigSource.S3_REGION)
          .bucket(ConfigSource.S3_BUCKET)
          .build());
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOGGER.info("Minio Started!");
    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("debezium.sink.iceberg.fs.s3a.endpoint", "http://localhost:" + container.getMappedPort(MINIO_DEFAULT_PORT).toString());
    params.put("debezium.sink.iceberg.fs.s3a.access.key", S3Minio.MINIO_ACCESS_KEY);
    params.put("debezium.sink.iceberg.fs.s3a.secret.key", S3Minio.MINIO_SECRET_KEY);
    params.put("debezium.sink.iceberg.fs.s3a.path.style.access", "true");

    return params;
  }


}
