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
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
    return new DebeziumEngine.RecordCommitter<EmbeddedEngineChangeEvent>() {
      @Override
      public synchronized void markProcessed(EmbeddedEngineChangeEvent record) {}

      @Override
      public synchronized void markBatchFinished() {}

      @Override
      public void markProcessed(
          EmbeddedEngineChangeEvent record, DebeziumEngine.Offsets sourceOffsets) {}

      @Override
      public DebeziumEngine.Offsets buildOffsets() {
        return null;
      }
    };
  }

  public static String postJson(String url, String body, String bearerToken) {
    return send("POST", url, "application/json", body, bearerToken);
  }

  /**
   * Sends an HTTP request, used to set up the catalogs.
   *
   * @return the response body, fails on a non 2xx status
   */
  public static String send(
      String method, String url, String contentType, String body, String bearerToken) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", contentType)
            .method(method, HttpRequest.BodyPublishers.ofString(body));
    if (bearerToken != null) {
      request.header("Authorization", "Bearer " + bearerToken);
    }
    try {
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2) {
        throw new IllegalStateException(
            method + " " + url + " failed: " + response.statusCode() + " " + response.body());
      }
      return response.body();
    } catch (IOException e) {
      throw new IllegalStateException(method + " " + url + " failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(method + " " + url + " interrupted", e);
    }
  }
}
