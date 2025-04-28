/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;


import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.iceberg.DebeziumConfig;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.testresources.TestUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Objects;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * helper class used to generate test debezium change events
 *
 * @author Ismail Simsek
 */
@ApplicationScoped
public class EventFactory {

  @Inject
  JsonBuilder builder;
  @Inject
  public DebeziumConfig debeziumConfig;

  public static EmbeddedEngineChangeEvent createMockChangeEvent(Object key, Object value) {
    return createMockChangeEvent(key, value, "test-destination");
  }


  public static EmbeddedEngineChangeEvent createMockChangeEvent(Object key, Object value, String destination) {

    EmbeddedEngineChangeEvent mockEvent = Mockito.mock(EmbeddedEngineChangeEvent.class);
    lenient().when(mockEvent.key()).thenReturn(key);
    lenient().when(mockEvent.value()).thenReturn(value);
    lenient().when(mockEvent.destination()).thenReturn(destination); // Assuming destination maps

    if (key instanceof Struct || value instanceof Struct) {
      SourceRecord mockSourceRecord = Mockito.mock(SourceRecord.class);
      when(mockSourceRecord.key()).thenReturn(key);
      when(mockSourceRecord.value()).thenReturn(value);
      when(mockSourceRecord.keySchema()).thenReturn(key != null ? ((Struct) key).schema() : null);
      lenient().when(mockSourceRecord.valueSchema()).thenReturn(value != null ? ((Struct) value).schema() : null);
      lenient().when(mockSourceRecord.topic()).thenReturn(destination);
      lenient().when(mockEvent.sourceRecord()).thenReturn(mockSourceRecord);
    }

    return mockEvent;
  }

  public static JsonEventConverter toIcebergChangeEvent(EmbeddedEngineChangeEvent e, GlobalConfig config) {
    return new JsonEventConverter(e.destination(), e.value().toString(), e.key().toString(), config);
  }

  public EmbeddedEngineChangeEvent of(String destination, Integer id, String operation, String name,
                                      Long epoch) {
    Object key = null;
    Object val = null;

    if (Objects.equals(debeziumConfig.keyValueChangeEventFormat(), "json")) {
      final JsonEventConverter t = builder
          .destination(destination)
          .addKeyField("id", id)
          .addField("first_name", name)
          .addField("__op", operation)
          .addField("__source_ts_ns", epoch)
          .addField("__deleted", operation.equals("d"))
          .build();

      key = "{" +
          "\"schema\":" + t.schemaConverter().keySchema() + "," +
          "\"payload\":" + t.key() +
          "} ";
      val = "{" +
          "\"schema\":" + t.schemaConverter().valueSchema() + "," +
          "\"payload\":" + t.value() +
          "} ";
    } else if (Objects.equals(debeziumConfig.keyValueChangeEventFormat(), "connect")) {
      key = StructBuilder.create("keySchema")
          .field("id", id)
          .build();

      val = StructBuilder.create("valSchema")
          .field("id", id)
          .field("first_name", name)
          .field("__op", operation)
          .field("__source_ts_ns", epoch)
          .field("__deleted", operation.equals("d"))
          .build();
    }
    return createMockChangeEvent(key, val, destination);
  }

  public EmbeddedEngineChangeEvent ofCompositeKey(String destination, Integer id, String operation, String name,
                                                  Long epoch) {
    Object key = null;
    Object val = null;

    if (Objects.equals(debeziumConfig.keyValueChangeEventFormat(), "json")) {
      final JsonEventConverter t = builder
          .destination(destination)
          .addKeyField("id", id)
          .addKeyField("first_name", name)
          .addField("__op", operation)
          .addField("__source_ts_ns", epoch)
          .addField("__deleted", operation.equals("d"))
          .build();

      key = "{" +
          "\"schema\":" + t.schemaConverter().keySchema() + "," +
          "\"payload\":" + t.key() +
          "} ";
      val = "{" +
          "\"schema\":" + t.schemaConverter().valueSchema() + "," +
          "\"payload\":" + t.value() +
          "} ";
    } else if (Objects.equals(debeziumConfig.keyValueChangeEventFormat(), "connect")) {

      key = StructBuilder.create("keySchema")
          .field("id", id)
          .field("first_name", name)
          .build();

      val = StructBuilder.create("valSchema")
          .field("id", id)
          .field("first_name", name)
          .field("__op", operation)
          .field("__source_ts_ns", epoch)
          .field("__deleted", operation.equals("d"))
          .build();
    }

    return createMockChangeEvent(key, val, destination);
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
    Object val = null;
    if (Objects.equals(debeziumConfig.keyValueChangeEventFormat(), "json")) {
      final JsonEventConverter t = new JsonBuilder()
          .destination(destination)
          .addField("id", id)
          .addField("first_name", name)
          .addField("__op", operation)
          .addField("__source_ts_ns", epoch)
          .addField("__deleted", operation.equals("d"))
          .build();
      val = "{" +
          "\"schema\":" + t.schemaConverter().valueSchema() + "," +
          "\"payload\":" + t.value() +
          "} ";
    } else if (Objects.equals(debeziumConfig.keyValueChangeEventFormat(), "connect")) {
      val = StructBuilder.create("decimalConnectSchema")
          .field("id", id)
          .field("first_name", name)
          .field("__op", operation)
          .field("__source_ts_ns", epoch)
          .field("__deleted", operation.equals("d"))
          .build();
    }
    return createMockChangeEvent(null, val, destination);
  }

  public EmbeddedEngineChangeEvent of(String key, String val) {
    return createMockChangeEvent(key, val, "test");
  }
}
