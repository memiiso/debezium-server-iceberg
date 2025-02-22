package io.debezium.server.iceberg;

import io.smallrye.config.SmallRyeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.mockito.Mockito;

public class TestConfigProducer {
  @Inject
  Config config;

  @Produces
  @ApplicationScoped
  @io.quarkus.test.Mock
  IcebergConsumerConfig appConfig() {
    IcebergConsumerConfig appConfig = config.unwrap(SmallRyeConfig.class).getConfigMapping(IcebergConsumerConfig.class);
    IcebergConsumerConfig appConfigSpy = Mockito.spy(appConfig);
    return appConfigSpy;
  }

}