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

  @WithName("debezium.sink.batch.buffer-per-table.enabled")
  @WithDefault("false")
  Boolean bufferPerTableEnabled();

  @WithName("debezium.sink.batch.buffer-per-table.flush-interval-ms")
  @WithDefault("30000")
  Long bufferPerTableFlushIntervalMs();

  @WithName("debezium.sink.batch.buffer-per-table.threshold")
  @WithDefault("1000")
  Integer bufferPerTableThreshold();

  @WithName("debezium.sink.batch.buffer-per-table.max-tables")
  @WithDefault("100")
  Integer bufferPerTableMaxTables();

  @WithName("debezium.sink.batch.buffer-per-table.retry.max-attempts")
  @WithDefault("3")
  Integer bufferPerTableRetryMaxAttempts();

  @WithName("debezium.sink.batch.buffer-per-table.retry.initial-delay-ms")
  @WithDefault("1000")
  Long bufferPerTableRetryInitialDelayMs();

  @WithName("debezium.sink.batch.buffer-per-table.retry.max-delay-ms")
  @WithDefault("60000")
  Long bufferPerTableRetryMaxDelayMs();

}