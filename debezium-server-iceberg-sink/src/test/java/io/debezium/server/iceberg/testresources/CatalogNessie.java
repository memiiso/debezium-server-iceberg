package io.debezium.server.iceberg.testresources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogNessie implements QuarkusTestResourceLifecycleManager {
  private static final String NESSIE_IMAGE = "projectnessie/nessie:latest";
  private static final int NESSIE_PORT = 19120;
  private GenericContainer<?> nessieContainer = new GenericContainer<>(DockerImageName.parse(NESSIE_IMAGE))
      .withNetworkAliases("nessie")
      .withEnv("QUARKUS_PROFILE", "prod")
      .withEnv("QUARKUS_HTTP_PORT", String.valueOf(NESSIE_PORT))
      .withEnv("QUARKUS_LOG_LEVEL", "INFO")
      .withExposedPorts(NESSIE_PORT)
      .waitingFor(new HttpWaitStrategy()
          .forPort(NESSIE_PORT)
          .forPath("/q/health")
          .withStartupTimeout(Duration.ofSeconds(120)));

  public static void main(String[] args) {
    CatalogNessie environment = new CatalogNessie();
    try {
      environment.start();
      System.out.println("Nessie URI: " + environment.getNessieUri());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      environment.stop();
    }
  }

  @Override
  public Map<String, String> start() {
    nessieContainer.start();
    System.out.println("Nessie started: " + getNessieUri());

    Map<String, String> config = new ConcurrentHashMap<>();

    config.put("debezium.sink.iceberg.type", "nessie");
    config.put("debezium.sink.iceberg.uri", getNessieUri() + "/api/v2");
    config.put("debezium.sink.iceberg.ref", "main");
    return config;
  }

  @Override
  public void stop() {
    if (nessieContainer != null) {
      nessieContainer.stop();
    }
  }

  public String getNessieUri() {
    if (nessieContainer != null && nessieContainer.isRunning()) {
      return "http://" + nessieContainer.getHost() + ":" + nessieContainer.getMappedPort(NESSIE_PORT);
    }
    return null;
  }
}