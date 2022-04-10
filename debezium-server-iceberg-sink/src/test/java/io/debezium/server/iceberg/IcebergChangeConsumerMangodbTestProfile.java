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

public class IcebergChangeConsumerMangodbTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.profile", "mongodb");
    config.put("%mongodb.debezium.source.connector.class", "io.debezium.connector.mongodb.MongoDbConnector");
    config.put("%mongodb.debezium.transforms.unwrap.type", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState");
    config.put("%mongodb.debezium.transforms.unwrap.add.fields", "op,source.ts_ms,db");
    config.put("%mongodb.debezium.sink.iceberg.allow-field-addition", "false");
    config.put("%mongodb.debezium.source.mongodb.name", "testc");
    config.put("%mongodb.debezium.source.database.include.list", "inventory"); // ok
    config.put("%mongodb.debezium.source.collection.include.list", "inventory.products");

    // IMPORTANT !!! FIX MongoDbConnector KEY FIELD NAME "id"=>"_id" !!!
    config.put("%mongodb.debezium.transforms", "unwrap,renamekeyfield");
    config.put("%mongodb.debezium.transforms.renamekeyfield.type",
        "org.apache.kafka.connect.transforms.ReplaceField$Key");
    config.put("%mongodb.debezium.transforms.renamekeyfield.renames", "id:_id");

    return config;
  }

  @Override
  public String getConfigProfile() {
    return "mongodb";
  }

}
