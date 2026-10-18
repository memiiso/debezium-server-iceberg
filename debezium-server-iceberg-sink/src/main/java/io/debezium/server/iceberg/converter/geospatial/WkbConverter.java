/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter.geospatial;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Converts Debezium WKB to ISO WKB. PostGIS sends EWKB, which Iceberg accepts but then skips the
 * column bounds.
 */
public final class WkbConverter {

  private static final int EWKB_Z_FLAG = 0x80000000;
  private static final int EWKB_M_FLAG = 0x40000000;
  private static final int EWKB_SRID_FLAG = 0x20000000;
  private static final int EWKB_FLAGS = EWKB_Z_FLAG | EWKB_M_FLAG | EWKB_SRID_FLAG;

  private WkbConverter() {}

  /**
   * Rewrites the geometry headers only (SRID dropped, Z/M flags to ISO codes), ISO WKB input is
   * validated and copied. Not using JTS: it writes EWKB Z/M flags.
   */
  public static byte[] toIsoWkb(byte[] wkb) {
    ByteBuffer in = ByteBuffer.wrap(wkb);
    ByteArrayOutputStream out = new ByteArrayOutputStream(wkb.length);
    try {
      convertGeometry(in, out);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("invalid WKB: " + e.getMessage(), e);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid WKB: truncated or corrupt", e);
    }
    if (in.hasRemaining()) {
      throw new IllegalArgumentException("invalid WKB: " + in.remaining() + " trailing bytes");
    }
    return out.toByteArray();
  }

  private static void convertGeometry(ByteBuffer in, ByteArrayOutputStream out) {
    byte byteOrder = in.get();
    if (byteOrder != 0 && byteOrder != 1) {
      throw new IllegalArgumentException("invalid byte order: " + byteOrder);
    }
    in.order(byteOrder == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
    int type = in.getInt();
    if ((type & EWKB_SRID_FLAG) != 0) {
      in.getInt(); // SRID
    }
    int isoType = type & ~EWKB_FLAGS;
    int geometryType = isoType % 1000;
    int isoDims = isoType / 1000;
    if (isoDims > 3) {
      throw new IllegalArgumentException("invalid geometry type: " + isoType);
    }
    boolean hasZ = (type & EWKB_Z_FLAG) != 0 || isoDims == 1 || isoDims == 3;
    boolean hasM = (type & EWKB_M_FLAG) != 0 || isoDims == 2 || isoDims == 3;
    int dimensions = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);

    out.write(byteOrder);
    writeInt(out, geometryType + (hasZ ? 1000 : 0) + (hasM ? 2000 : 0), in.order());

    // GeoParquet 2.0 only allows the 7 simple feature types: no curves, PolyhedralSurface, TIN,
    // Triangle
    switch (geometryType) {
      case 1 -> copy(in, out, 8 * dimensions); // Point
      case 2 -> copyPoints(in, out, dimensions); // LineString
      case 3 -> copyRings(in, out, dimensions); // Polygon
      case 4, 5, 6, 7 -> copyGeometries(in, out); // Multi*, GeometryCollection
      default ->
          throw new IllegalArgumentException(
              "unsupported geometry type: "
                  + geometryType
                  + ", only Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon"
                  + " and GeometryCollection are supported");
    }
  }

  private static void copyRings(ByteBuffer in, ByteArrayOutputStream out, int dimensions) {
    int rings = copyCount(in, out);
    for (int i = 0; i < rings; i++) {
      copyPoints(in, out, dimensions);
    }
  }

  private static void copyGeometries(ByteBuffer in, ByteArrayOutputStream out) {
    int geometries = copyCount(in, out);
    for (int i = 0; i < geometries; i++) {
      convertGeometry(in, out);
    }
  }

  private static void copyPoints(ByteBuffer in, ByteArrayOutputStream out, int dimensions) {
    int points = copyCount(in, out);
    copy(in, out, Math.multiplyExact(8 * dimensions, points));
  }

  private static int copyCount(ByteBuffer in, ByteArrayOutputStream out) {
    int count = in.getInt();
    if (count < 0) {
      throw new IllegalArgumentException("negative element count: " + count);
    }
    writeInt(out, count, in.order());
    return count;
  }

  private static void writeInt(ByteArrayOutputStream out, int value, ByteOrder order) {
    out.writeBytes(ByteBuffer.allocate(4).order(order).putInt(value).array());
  }

  private static void copy(ByteBuffer in, ByteArrayOutputStream out, int length) {
    out.write(in.array(), in.arrayOffset() + in.position(), length);
    in.position(in.position() + length);
  }
}
