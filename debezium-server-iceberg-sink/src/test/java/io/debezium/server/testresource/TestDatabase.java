/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.testresource;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class TestDatabase implements QuarkusTestResourceLifecycleManager {

  public static final String POSTGRES_USER = "postgres";
  public static final String POSTGRES_PASSWORD = "postgres";
  public static final String POSTGRES_DBNAME = "postgres";
  public static final String POSTGRES_IMAGE = "debezium/example-postgres";
  public static final String POSTGRES_HOST = "localhost";
  public static final Integer POSTGRES_PORT_DEFAULT = 5432;
  private static final Logger LOGGER = LoggerFactory.getLogger(TestDatabase.class);
  public static Integer POSTGRES_PORT_MAPPED;

  private GenericContainer<?> container;

  public static void runSQL(String query) throws SQLException {
    try {

      String url = "jdbc:postgresql://" + POSTGRES_HOST + ":" + POSTGRES_PORT_MAPPED + "/" + POSTGRES_DBNAME;
      Connection con = DriverManager.getConnection(url, POSTGRES_USER, POSTGRES_PASSWORD);
      Statement st = con.createStatement();
      st.execute(query);
      con.close();
    } catch (Exception e) {
      LOGGER.error(query);
      throw e;
    }

  }

  @Override
  public void stop() {
    container.stop();
  }

  public Map<String, String> start() {
    container = new GenericContainer<>(POSTGRES_IMAGE)
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
        .withEnv("POSTGRES_USER", POSTGRES_USER)
        .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .withEnv("POSTGRES_DB", POSTGRES_DBNAME)
        .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
        .withEnv("LANG", "en_US.utf8")
        .withStartupTimeout(Duration.ofSeconds(30));
    container.start();
    POSTGRES_PORT_MAPPED = container.getMappedPort(POSTGRES_PORT_DEFAULT);

    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("debezium.source.database.hostname", POSTGRES_HOST);
    params.put("debezium.source.database.port", container.getMappedPort(POSTGRES_PORT_DEFAULT).toString());
    params.put("debezium.source.database.user", POSTGRES_USER);
    params.put("debezium.source.database.password", POSTGRES_PASSWORD);
    params.put("debezium.source.database.dbname", POSTGRES_DBNAME);
    return params;
  }

}
