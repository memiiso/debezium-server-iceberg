package io.debezium.server.iceberg;

import io.smallrye.config.SmallRyeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.mockito.Mockito;

/**
 * This class provides a mocked instance of IcebergConsumerConfig for testing purposes,
 * allowing selective overriding of configuration values while preserving the original
 * configuration.
 */
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