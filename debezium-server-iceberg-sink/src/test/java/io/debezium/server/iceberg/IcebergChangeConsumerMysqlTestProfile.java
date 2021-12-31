/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class IcebergChangeConsumerMysqlTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.profile", "mysql");
    config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("%mysql.debezium.source.table.whitelist",
        "inventory.customers,inventory.test_delete_table,inventory.test_field_addition");
    return config;
  }

  @Override
  public String getConfigProfile() {
    return "mysql";
  }

}
