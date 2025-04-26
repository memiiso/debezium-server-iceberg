/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;


import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.testresources.TestUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.mockito.Mockito;

import java.time.Instant;

import static org.mockito.Mockito.when;

/**
 * helper class used to generate test debezium change events
 *
 * @author Ismail Simsek
 */
@ApplicationScoped
public class TestChangeEventFactory {

  @Inject
  IcebergChangeEventBuilder builder;

  public static EmbeddedEngineChangeEvent getEECE(Object key, Object value, String destination){
    EmbeddedEngineChangeEvent mockRecord = Mockito.mock(EmbeddedEngineChangeEvent.class);
    when(mockRecord.key()).thenReturn(key);
    when(mockRecord.value()).thenReturn(value);
    when(mockRecord.destination()).thenReturn(destination); // Assuming destination maps
    return mockRecord;
  }

  public static RecordConverter toIcebergChangeEvent(EmbeddedEngineChangeEvent e, GlobalConfig config) {
    return new RecordConverter(e.destination(), e.value().toString(), e.key().toString(), config);
  }

  public EmbeddedEngineChangeEvent of(String destination, Integer id, String operation, String name,
                                              Long epoch) {
    final RecordConverter t = builder
        .destination(destination)
        .addKeyField("id", id)
        .addField("first_name", name)
        .addField("__op", operation)
        .addField("__source_ts_ns", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    final String key = "{" +
        "\"schema\":" + t.schemaConverter().keySchema() + "," +
        "\"payload\":" + t.key() +
        "} ";
    final String val = "{" +
        "\"schema\":" + t.schemaConverter().valueSchema() + "," +
        "\"payload\":" + t.value() +
        "} ";
    return getEECE(key, val, destination);
  }

  public EmbeddedEngineChangeEvent ofCompositeKey(String destination, Integer id, String operation, String name,
                                                          Long epoch) {
    final RecordConverter t = builder
        .destination(destination)
        .addKeyField("id", id)
        .addKeyField("first_name", name)
        .addField("__op", operation)
        .addField("__source_ts_ns", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    final String key = "{" +
        "\"schema\":" + t.schemaConverter().keySchema() + "," +
        "\"payload\":" + t.key() +
        "} ";
    final String val = "{" +
        "\"schema\":" + t.schemaConverter().valueSchema() + "," +
        "\"payload\":" + t.value() +
        "} ";

    return getEECE(key, val, destination);
  }

  public EmbeddedEngineChangeEvent of(String destination, Integer id, String operation) {
    return of(destination, id, operation, TestUtil.randomString(12), Instant.now().toEpochMilli());
  }

  public EmbeddedEngineChangeEvent of(String destination, Integer id, String operation, String name) {
    return of(destination, id, operation, name, Instant.now().toEpochMilli());
  }

  public EmbeddedEngineChangeEvent of(String destination, Integer id, String operation, Long epoch) {
    return of(destination, id, operation, TestUtil.randomString(12), epoch);
  }

  public EmbeddedEngineChangeEvent ofNoKey(String destination, Integer id, String operation, String name,
                                                   Long epoch) {
    final RecordConverter t = builder
        .destination(destination)
        .addField("id", id)
        .addField("first_name", name)
        .addField("__op", operation)
        .addField("__source_ts_ns", epoch)
        .addField("__deleted", operation.equals("d"))
        .build();

    final String val = "{" +
        "\"schema\":" + t.schemaConverter().valueSchema() + "," +
        "\"payload\":" + t.value() +
        "} ";
    return getEECE(null, val, destination);
  }

  public EmbeddedEngineChangeEvent of(String key, String val) {
    return getEECE(key, val, "test");
  }
}
