/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter.geospatial;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.kafka.connect.data.Struct;

/** Converts Debezium geospatial structs {@code {wkb, srid}} to Iceberg values. */
public final class GeospatialValues {

  private static final String WKB_FIELD = "wkb";
  private static final String SRID_FIELD = "srid";
  private static final Map<String, Optional<Integer>> CRS_SRIDS = new ConcurrentHashMap<>();

  private GeospatialValues() {}

  public static ByteBuffer fromStruct(Object value, String fieldName, Type type) {
    if (!(value instanceof Struct struct)) {
      throw invalid(fieldName, "not a Struct: " + value.getClass().getName(), null);
    }
    if (struct.schema().field(SRID_FIELD) != null) {
      checkSrid((Integer) struct.get(SRID_FIELD), fieldName, type);
    }
    Object wkb = struct.get(WKB_FIELD);
    if (wkb instanceof byte[] bytes) {
      return toIsoWkb(bytes, fieldName);
    }
    if (wkb instanceof ByteBuffer buffer) {
      return toIsoWkb(ByteBuffers.toByteArray(buffer), fieldName);
    }
    throw invalid(fieldName, "unexpected wkb: " + wkb, null);
  }

  public static ByteBuffer fromJson(JsonNode node, String fieldName, Type type) {
    JsonNode srid = node.get(SRID_FIELD);
    if (srid != null && !srid.isNull() && !srid.isInt()) {
      throw invalid(fieldName, "srid is not an integer: " + srid, null);
    }
    checkSrid(srid == null || srid.isNull() ? null : srid.intValue(), fieldName, type);
    JsonNode wkb = node.get(WKB_FIELD);
    byte[] bytes;
    try {
      bytes = wkb == null ? null : wkb.binaryValue();
    } catch (IOException e) {
      throw invalid(fieldName, "wkb is not base64", e);
    }
    if (bytes == null) {
      throw invalid(fieldName, "missing wkb", null);
    }
    return toIsoWkb(bytes, fieldName);
  }

  /** The per-value SRID is not stored, fail when it contradicts the column CRS. */
  private static void checkSrid(Integer srid, String fieldName, Type type) {
    if (!sridMatchesCrs(srid, type)) {
      throw invalid(
          fieldName,
          "SRID "
              + srid
              + " doesn't match the column CRS "
              + crs(type)
              + ", see debezium.sink.iceberg.geometry-crs",
          null);
    }
  }

  /** Values without SRID (null or 0) are accepted. */
  static boolean sridMatchesCrs(Integer srid, Type type) {
    if (srid == null || srid == 0) {
      return true;
    }
    return CRS_SRIDS
        .computeIfAbsent(crs(type), crs -> Optional.ofNullable(GeospatialTypes.srid(crs)))
        .filter(srid::equals)
        .isPresent();
  }

  private static String crs(Type type) {
    if (type instanceof Types.GeometryType geometry) {
      return geometry.crs();
    }
    if (type instanceof Types.GeographyType geography) {
      return geography.crs();
    }
    throw new IllegalArgumentException("Not a geospatial type: " + type);
  }

  private static ByteBuffer toIsoWkb(byte[] wkb, String fieldName) {
    try {
      return ByteBuffer.wrap(WkbConverter.toIsoWkb(wkb));
    } catch (IllegalArgumentException e) {
      throw invalid(fieldName, e.getMessage(), e);
    }
  }

  private static DebeziumException invalid(String fieldName, String reason, Throwable cause) {
    return new DebeziumException(
        "Invalid geospatial value, field: " + fieldName + ": " + reason, cause);
  }
}
