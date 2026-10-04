/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import java.security.SecureRandom;
import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;

public class TestUtil {
  static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static final SecureRandom rnd = new SecureRandom();

  public static BatchEvent toBatchEvent(EmbeddedEngineChangeEvent event) {
    return new BatchEvent() {
      @Override
      public Object key() {
        return event.key();
      }

      @Override
      public Object value() {
        return event.value();
      }

      @Override
      public Integer partition() {
        return event.partition();
      }

      @Override
      public SourceRecord record() {
        return event.sourceRecord();
      }

      @Override
      public String destination() {
        return event.destination();
      }

      @Override
      public void commit() {}
    };
  }

  public static CapturingEvents<BatchEvent> toCapturingEvents(
      List<EmbeddedEngineChangeEvent> events) {
    List<BatchEvent> batch = events.stream().map(TestUtil::toBatchEvent).toList();
    return new CapturingEvents<>() {
      @Override
      public List<BatchEvent> records() {
        return batch;
      }

      @Override
      public String destination() {
        return null;
      }

      @Override
      public String source() {
        return "test";
      }

      @Override
      public String engine() {
        return "default";
      }
    };
  }

  public static int randomInt(int low, int high) {
    return rnd.nextInt(high - low) + low;
  }

  public static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) sb.append(AB.charAt(rnd.nextInt(AB.length())));
    return sb.toString();
  }

  public static DebeziumEngine.RecordCommitter<EmbeddedEngineChangeEvent> getCommitter() {
    return new DebeziumEngine.RecordCommitter() {
      public synchronized void markProcessed(SourceRecord record) {}

      @Override
      public void markProcessed(Object record) {}

      public synchronized void markBatchFinished() {}

      @Override
      public void markProcessed(Object record, DebeziumEngine.Offsets sourceOffsets) {}

      @Override
      public DebeziumEngine.Offsets buildOffsets() {
        return null;
      }
    };
  }
}
