/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class SourceMangoDB implements QuarkusTestResourceLifecycleManager {

  public static final int MONGODB_PORT = 27017;
  public static final GenericContainer<?> container = new GenericContainer(
      new ImageFromDockerfile("debezium_mongodb", false)
          .withFileFromClasspath("Dockerfile", "mongodb/Dockerfile")
          .withFileFromClasspath("start-mongodb.sh", "mongodb/start-mongodb.sh"))

      .waitingFor(Wait.forLogMessage(".*Successfully initialized inventory database.*", 1))
      .withStartupTimeout(Duration.ofSeconds(120L));

  @Override
  public Map<String, String> start() {
    container.setPortBindings(List.of(MONGODB_PORT+":"+MONGODB_PORT));

    container.withExposedPorts(MONGODB_PORT).start();

    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("%mongodb.debezium.source.mongodb.connection.string",
        "mongodb://" + container.getHost() + ":" + container.getMappedPort(MONGODB_PORT) + "/?replicaSet=rs0"
    );
    params.put("%mongodb.debezium.source.mongodb.authsource", "admin");
    params.put("%mongodb.debezium.source.mongodb.user", "debezium");
    params.put("%mongodb.debezium.source.mongodb.password", "dbz");
    //params.put("%mongodb.debezium.source.mongodb.ssl.enabled", "false");
    return params;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

}
