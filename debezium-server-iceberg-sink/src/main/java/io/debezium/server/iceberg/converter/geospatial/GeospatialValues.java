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

  /**
   * @param allowMissingSrid write values without SRID with the column CRS instead of failing
   */
  public static ByteBuffer fromStruct(
      Object value, String fieldName, Type type, boolean allowMissingSrid) {
    if (!(value instanceof Struct struct)) {
      throw invalid(fieldName, "not a Struct: " + value.getClass().getName(), null);
    }
    Object wkb = struct.get(WKB_FIELD);
    ByteBuffer isoWkb;
    if (wkb instanceof byte[] bytes) {
      isoWkb = toIsoWkb(bytes, fieldName);
    } else if (wkb instanceof ByteBuffer buffer) {
      isoWkb = toIsoWkb(ByteBuffers.toByteArray(buffer), fieldName);
    } else {
      throw invalid(fieldName, "unexpected wkb: " + wkb, null);
    }
    // after the WKB, so an invalid value isn't reported as an SRID mismatch
    Object sridVal = struct.schema().field(SRID_FIELD) == null ? null : struct.get(SRID_FIELD);
    Integer srid = null;
    if (sridVal instanceof Number n) {
      srid = n.intValue();
    } else if (sridVal != null) {
      throw invalid(fieldName, "srid is not an integer: " + sridVal, null);
    }
    checkSrid(srid, fieldName, type, allowMissingSrid);
    return isoWkb;
  }

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
      throw invalid(fieldName, "wkb is not base64", e);
    }
    if (bytes == null) {
      throw invalid(fieldName, "missing wkb", null);
    }
    ByteBuffer isoWkb = toIsoWkb(bytes, fieldName);
    // after the WKB, so an invalid value isn't reported as an SRID mismatch
    JsonNode srid = node.get(SRID_FIELD);
    if (srid != null && !srid.isNull() && !srid.isInt()) {
      throw invalid(fieldName, "srid is not an integer: " + srid, null);
    }
    checkSrid(
        srid == null || srid.isNull() ? null : srid.intValue(), fieldName, type, allowMissingSrid);
    return isoWkb;
  }

  /** The per-value SRID is not stored, fail when it contradicts the column CRS. */
  private static void checkSrid(
      Integer srid, String fieldName, Type type, boolean allowMissingSrid) {
    boolean missingSrid = srid == null || srid == 0;
    if ((missingSrid && allowMissingSrid) || sridMatchesCrs(srid, type)) {
      return;
    }
    throw invalid(
        fieldName,
        missingSrid
            ? "value without SRID doesn't match the column CRS "
                + crs(type)
                + ", see debezium.sink.iceberg.geometry-crs and"
                + " debezium.sink.iceberg.geospatial-allow-missing-srid"
            : "SRID "
                + srid
                + " doesn't match the column CRS "
                + crs(type)
                + ", see debezium.sink.iceberg.geometry-crs",
        null);
  }

  /** Values without SRID (null or 0) only match the unknown CRS {@code srid:0}. */
  static boolean sridMatchesCrs(Integer srid, Type type) {
    int valueSrid = srid == null ? 0 : srid;
    return CRS_SRIDS
        .computeIfAbsent(crs(type), crs -> Optional.ofNullable(GeospatialTypes.srid(crs)))
        .filter(crsSrid -> crsSrid == valueSrid)
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
