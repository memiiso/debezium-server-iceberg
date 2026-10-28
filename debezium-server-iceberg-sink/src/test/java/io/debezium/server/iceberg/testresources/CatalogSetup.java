/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.testresources;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** HTTP calls used to set up the catalogs started by the test resources. */
public final class CatalogSetup {

  private CatalogSetup() {}

  public static String postJson(String url, String body, String bearerToken) {
    return send("POST", url, "application/json", body, bearerToken);
  }

  /**
   * Sends an HTTP request.
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
