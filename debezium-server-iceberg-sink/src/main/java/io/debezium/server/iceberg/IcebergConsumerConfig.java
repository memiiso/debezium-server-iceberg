package io.debezium.server.iceberg;

import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithParentName;

@ConfigRoot
@ConfigMapping
public interface IcebergConsumerConfig {

  @WithParentName
  IcebergConfig iceberg();

  @WithParentName
  DebeziumConfig debezium();

}