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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.debezium.server.iceberg.TestConfigSource.S3_BUCKET;
import static io.debezium.server.iceberg.TestConfigSource.S3_BUCKET_NAME;

public class S3Minio implements QuarkusTestResourceLifecycleManager {

  public static final String MINIO_ACCESS_KEY = "admin";
  public static final String MINIO_SECRET_KEY = "12345678";
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3Minio.class);
  static final String DEFAULT_IMAGE = "minio/minio:latest";
  public static MinioClient client;

  static public final MinIOContainer container = new MinIOContainer(DockerImageName.parse(DEFAULT_IMAGE))
      .withUserName(MINIO_ACCESS_KEY)
      .withPassword(MINIO_SECRET_KEY);

  public static void listFiles() {
    LOGGER.info("-----------------------------------------------------------------");
    try {
      List<Bucket> bucketList = client.listBuckets();
      for (Bucket bucket : bucketList) {
        System.out.printf("Bucket:%s ROOT\n", bucket.name());
        Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
        for (Result<Item> result : results) {
          Item item = result.get();
          System.out.printf("Bucket:%s Item:%s Size:%s\n", bucket.name(), item.objectName(), item.size());
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

  @Override
  public Map<String, String> start() {
    container.start();
    client = MinioClient
        .builder()
        .endpoint(container.getS3URL())
        .credentials(container.getUserName(), container.getPassword())
        .build();

    try {
      client.ignoreCertCheck();
      client.makeBucket(MakeBucketArgs.builder()
          .region(TestConfigSource.S3_REGION)
          .bucket(S3_BUCKET_NAME)
          .build());
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOGGER.info("Minio Started!");
    Map<String, String> config = new ConcurrentHashMap<>();
    // FOR JDBC CATALOG
    config.put("debezium.sink.iceberg.s3.endpoint", container.getS3URL());
    config.put("debezium.sink.iceberg.s3.path-style-access", "true");
    config.put("debezium.sink.iceberg.s3.access-key-id", S3Minio.MINIO_ACCESS_KEY);
    config.put("debezium.sink.iceberg.s3.secret-access-key", S3Minio.MINIO_SECRET_KEY);
    config.put("debezium.sink.iceberg.client.region", TestConfigSource.S3_REGION);
    config.put("debezium.sink.iceberg.io-impl", TestConfigSource.ICEBERG_CATALOG_FILEIO);
    config.put("debezium.sink.iceberg.warehouse", S3_BUCKET);
    // FOR HADOOP CATALOG
    config.put("debezium.sink.iceberg.fs.s3a.endpoint", container.getS3URL());
    config.put("debezium.sink.iceberg.fs.s3a.access.key", S3Minio.MINIO_ACCESS_KEY);
    config.put("debezium.sink.iceberg.fs.s3a.secret.key", S3Minio.MINIO_SECRET_KEY);
    config.put("debezium.sink.iceberg.fs.s3a.path.style.access", "true");
    config.put("debezium.sink.iceberg.fs.defaultFS", "s3a://" + S3_BUCKET_NAME);

    return config;
  }


}
