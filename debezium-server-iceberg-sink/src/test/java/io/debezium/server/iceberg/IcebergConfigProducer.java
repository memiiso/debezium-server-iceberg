package io.debezium.server.iceberg;

import io.smallrye.config.SmallRyeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.mockito.Mockito;

/**
 * This class provides a mocked instance of IcebergConfig for testing purposes,
 * allowing selective overriding of configuration values while preserving the original
 * configuration.
 */
public class IcebergConfigProducer {
  @Inject
  Config config;

  @Produces
  @ApplicationScoped
  @io.quarkus.test.Mock
  IcebergConfig appConfig() {
    IcebergConfig appConfig = config.unwrap(SmallRyeConfig.class).getConfigMapping(IcebergConfig.class);
    IcebergConfig appConfigSpy = Mockito.spy(appConfig);
    return appConfigSpy;
  }

}