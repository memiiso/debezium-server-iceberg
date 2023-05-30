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

import org.apache.iceberg.jdbc.JdbcCatalog;
import org.testcontainers.containers.MySQLContainer;

public class JdbcCatalogDB implements QuarkusTestResourceLifecycleManager {
  public static MySQLContainer<?> container = new MySQLContainer<>();

  @Override
  public Map<String, String> start() {
    container.start();

    Map<String, String> config = new ConcurrentHashMap<>();

    config.put("debezium.sink.iceberg.catalog-impl", JdbcCatalog.class.getName());
    config.put("debezium.sink.iceberg.uri", container.getJdbcUrl());
    config.put("debezium.sink.iceberg.jdbc.user", container.getUsername());
    config.put("debezium.sink.iceberg.jdbc.password", container.getPassword());

    return config;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

}
