package io.debezium.server.iceberg;

import io.debezium.config.CommonConnectorConfig;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigRoot
@ConfigMapping
public interface BatchConfig {
  @WithName("debezium.source.max.queue.size")
  @WithDefault(CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE + "")
  int sourceMaxQueueSize();

  @WithName("debezium.source.max.batch.size")
  @WithDefault(CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE + "")
  int sourceMaxBatchSize();

  @WithName("debezium.sink.batch.batch-size-wait.max-wait-ms")
  @WithDefault("300000")
  int batchSizeWaitMaxWaitMs();

  @WithName("debezium.sink.batch.batch-size-wait.wait-interval-ms")
  @WithDefault("10000")
  int batchSizeWaitWaitIntervalMs();

  @WithName("debezium.sink.batch.batch-size-wait")
  @WithDefault("NoBatchSizeWait")
  String batchSizeWaitName();

  @WithName("debezium.sink.batch.concurrent-uploads")
  @WithDefault("1")
  int concurrentUploads();

  @WithName("debezium.sink.batch.concurrent-uploads.timeout-minutes")
  @WithDefault("60")
  int concurrentUploadsTimeoutMinutes();


}