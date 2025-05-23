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

import java.security.SecureRandom;

import org.apache.kafka.connect.source.SourceRecord;

public class TestUtil {
  static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static final SecureRandom rnd = new SecureRandom();


  public static int randomInt(int low, int high) {
    return rnd.nextInt(high - low) + low;
  }

  public static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++)
      sb.append(AB.charAt(rnd.nextInt(AB.length())));
    return sb.toString();
  }

  public static DebeziumEngine.RecordCommitter<EmbeddedEngineChangeEvent> getCommitter() {
    return new DebeziumEngine.RecordCommitter() {
      public synchronized void markProcessed(SourceRecord record) {
      }

      @Override
      public void markProcessed(Object record) {
      }

      public synchronized void markBatchFinished() {
      }

      @Override
      public void markProcessed(Object record, DebeziumEngine.Offsets sourceOffsets) {
      }

      @Override
      public DebeziumEngine.Offsets buildOffsets() {
        return null;
      }
    };
  }

}
