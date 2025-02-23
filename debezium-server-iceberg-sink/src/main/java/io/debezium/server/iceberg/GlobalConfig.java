package io.debezium.server.iceberg;

import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import org.jboss.logging.Logger;

@ConfigRoot
@ConfigMapping
public interface GlobalConfig {

  @WithParentName
  IcebergConfig iceberg();

  @WithParentName
  DebeziumConfig debezium();

  @WithName("quarkus.log.level")
  @WithDefault("INFO")
  Logger.Level quarkusLogLevel();

}