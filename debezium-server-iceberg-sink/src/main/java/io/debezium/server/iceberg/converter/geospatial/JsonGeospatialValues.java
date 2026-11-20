/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter.geospatial;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.converters.iceberg.geospatial.GeospatialValues;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.types.Type;

/** Converts Debezium JSON geospatial payloads {@code {wkb, srid}} to Iceberg values. */
public final class JsonGeospatialValues {

  private static final String WKB_FIELD = "wkb";
  private static final String SRID_FIELD = "srid";

  private JsonGeospatialValues() {}

  /**
   * @param allowMissingSrid write values without SRID with the column CRS instead of failing
   */
  public static ByteBuffer fromJson(
      JsonNode node, String fieldName, Type type, boolean allowMissingSrid) {
    JsonNode wkb = node.get(WKB_FIELD);
    byte[] bytes;
    try {
      bytes = wkb == null ? null : wkb.binaryValue();
    } catch (IOException e) {
      throw GeospatialValues.invalid(fieldName, "wkb is not base64", e);
    }
    if (bytes == null) {
      throw GeospatialValues.invalid(fieldName, "missing wkb", null);
    }
    ByteBuffer isoWkb = GeospatialValues.toIsoWkb(bytes, fieldName);
    // after the WKB, so an invalid value isn't reported as an SRID mismatch
    JsonNode srid = node.get(SRID_FIELD);
    if (srid != null && !srid.isNull() && !srid.isInt()) {
      throw GeospatialValues.invalid(fieldName, "srid is not an integer: " + srid, null);
    }
    GeospatialValues.checkSrid(
        srid == null || srid.isNull() ? null : srid.intValue(), fieldName, type, allowMissingSrid);
    return isoWkb;
  }
}
