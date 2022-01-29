/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

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

public class SourceMysqlDB implements QuarkusTestResourceLifecycleManager {

  public static final String MYSQL_ROOT_PASSWORD = "debezium";
  public static final String MYSQL_USER = "mysqluser";
  public static final String MYSQL_PASSWORD = "mysqlpw";
  public static final String MYSQL_DEBEZIUM_USER = "debezium";
  public static final String MYSQL_DEBEZIUM_PASSWORD = "dbz";
  public static final String MYSQL_IMAGE = "debezium/example-mysql:1.7.0.Final";
  public static final String MYSQL_HOST = "127.0.0.1";
  public static final String MYSQL_DATABASE = "inventory";
  public static final Integer MYSQL_PORT_DEFAULT = 3306;
  private static final Logger LOGGER = LoggerFactory.getLogger(SourceMysqlDB.class);

  static private final GenericContainer<?> container = new GenericContainer<>(MYSQL_IMAGE)
      .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
      .withEnv("MYSQL_USER", MYSQL_USER)
      .withEnv("MYSQL_PASSWORD", MYSQL_PASSWORD)
      .withEnv("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
      .withExposedPorts(MYSQL_PORT_DEFAULT)
      .withStartupTimeout(Duration.ofSeconds(30));

  public static void runSQL(String query) throws SQLException, ClassNotFoundException {
    try {
      String url = "jdbc:mysql://" + MYSQL_HOST + ":" + container.getMappedPort(MYSQL_PORT_DEFAULT) + "/" + MYSQL_DATABASE + "?useSSL=false";
      Class.forName("com.mysql.cj.jdbc.Driver");
      Connection con = DriverManager.getConnection(url, MYSQL_USER, MYSQL_PASSWORD);
      Statement st = con.createStatement();
      st.execute(query);
      con.close();
    } catch (Exception e) {
      LOGGER.error(query);
      throw e;
    }
  }

  @Override
  public Map<String, String> start() {
    container.start();

    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("%mysql.debezium.source.database.hostname", MYSQL_HOST);
    params.put("%mysql.debezium.source.database.port", container.getMappedPort(MYSQL_PORT_DEFAULT).toString());
    params.put("%mysql.debezium.source.database.user", MYSQL_DEBEZIUM_USER);
    params.put("%mysql.debezium.source.database.password", MYSQL_DEBEZIUM_PASSWORD);
    params.put("%mysql.debezium.source.database.dbname", MYSQL_DATABASE);
    return params;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

}
