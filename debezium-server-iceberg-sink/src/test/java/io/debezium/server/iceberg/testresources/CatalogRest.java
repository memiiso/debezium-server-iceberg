/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogRest implements QuarkusTestResourceLifecycleManager {
  public static final int REST_CATALOG_PORT = 8181;
  public static final String REST_CATALOG_IMAGE = "apache/iceberg-rest-fixture";

  public static final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(REST_CATALOG_IMAGE))
      .withExposedPorts(REST_CATALOG_PORT)
      .waitingFor(Wait.forLogMessage(".*Started Server.*", 1));

  public static String getHostUrl() {
    return String.format("http://%s:%s", container.getHost(), container.getMappedPort(REST_CATALOG_PORT));
  }

  @Override
  public Map<String, String> start() {
    container.start();

    Map<String, String> config = new ConcurrentHashMap<>();

    config.put("debezium.sink.iceberg.type", "rest");
    config.put("debezium.sink.iceberg.uri", CatalogRest.getHostUrl());

    return config;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

}
