/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.MySQLContainer;

public class CatalogJdbc implements QuarkusTestResourceLifecycleManager {
  public static final MySQLContainer<?> container = new MySQLContainer<>("mysql:8");

  @Override
  public Map<String, String> start() {
    container.start();

    Map<String, String> config = new ConcurrentHashMap<>();

    config.put("debezium.sink.iceberg.type", "jdbc");
    config.put("debezium.sink.iceberg.uri", container.getJdbcUrl());
    config.put("debezium.sink.iceberg.jdbc.user", container.getUsername());
    config.put("debezium.sink.iceberg.jdbc.password", container.getPassword());
    config.put("debezium.sink.iceberg.jdbc.schema-version", "V1");

    return config;
  }

  @Override
  public void stop() {
    container.stop();
  }

}
