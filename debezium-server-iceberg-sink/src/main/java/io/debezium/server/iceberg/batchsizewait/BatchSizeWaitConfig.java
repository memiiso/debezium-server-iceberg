package io.debezium.server.iceberg.batchsizewait;

import io.debezium.config.CommonConnectorConfig;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigRoot
@ConfigMapping
public interface BatchSizeWaitConfig {
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

}