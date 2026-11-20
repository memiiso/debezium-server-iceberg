/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Information about the {@code debezium-iceberg-converter} module. */
public final class Module {

  private static final String VERSION = loadVersion();

  private Module() {}

  public static String version() {
    return VERSION;
  }

  private static String loadVersion() {
    Properties props = new Properties();
    try (InputStream stream =
        Module.class.getResourceAsStream("/io/debezium/converters/iceberg/build.version")) {
      if (stream != null) {
        props.load(stream);
      }
    } catch (IOException ignored) {
      // fall through to default
    }
    return props.getProperty("version", "unknown");
  }
}
