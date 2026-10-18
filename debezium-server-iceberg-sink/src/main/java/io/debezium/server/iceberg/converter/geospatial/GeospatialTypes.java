/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter.geospatial;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.converter.IcebergSchemaInfo;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Maps Debezium geometry/geography types to Iceberg v3 geospatial types. {@code
 * io.debezium.data.geometry.Point} is not mapped: it is used for PostgreSQL native point (no CRS)
 * and for MySQL POINT, which can't be told apart.
 */
public final class GeospatialTypes {

  public static final String GEOMETRY_LOGICAL_NAME = "io.debezium.data.geometry.Geometry";
  public static final String GEOGRAPHY_LOGICAL_NAME = "io.debezium.data.geometry.Geography";
  // GeoParquet 2.0 readers must resolve <authority>:<code>, not srid:<N>: srid:0 is only used for
  // the unknown CRS
  private static final Pattern GEOMETRY_CRS =
      Pattern.compile("OGC:CRS84|EPSG:[1-9]\\d{0,8}|srid:0", Pattern.CASE_INSENSITIVE);

  private GeospatialTypes() {}

  /** Returns null when disabled or not a geospatial type. */
  public static Type.PrimitiveType icebergType(String logicalTypeName, IcebergConfig config) {
    if (logicalTypeName == null || !config.geospatialTypes()) {
      return null;
    }
    return switch (logicalTypeName) {
      case GEOMETRY_LOGICAL_NAME -> Types.GeometryType.of(config.geometryCrs());
      case GEOGRAPHY_LOGICAL_NAME -> Types.GeographyType.crs84();
      default -> null;
    };
  }

  /** Returns false when the field is not a geospatial field. */
  public static boolean addIcebergField(
      String logicalTypeName,
      String fieldName,
      boolean isOptional,
      boolean isPkField,
      String doc,
      IcebergSchemaInfo schemaData,
      IcebergConfig config) {
    Type.PrimitiveType type = icebergType(logicalTypeName, config);
    if (type == null) {
      return false;
    }
    Types.NestedField field =
        Types.NestedField.of(
            schemaData.nextFieldId().getAndIncrement(), isOptional, fieldName, type, doc);
    schemaData.fields().add(field);
    if (isPkField) {
      schemaData.identifierFieldIds().add(field.fieldId());
    }
    return true;
  }

  /** SRID of a CRS ({@code OGC:CRS84}, {@code srid:N}, {@code EPSG:N}), null for other formats. */
  public static Integer srid(String crs) {
    String normalized = crs.toLowerCase(Locale.ROOT);
    if (normalized.equals("ogc:crs84")) {
      return 4326;
    }
    for (String prefix : new String[] {"srid:", "epsg:"}) {
      if (normalized.startsWith(prefix)) {
        try {
          return Integer.parseInt(normalized.substring(prefix.length()));
        } catch (NumberFormatException e) {
          return null;
        }
      }
    }
    return null;
  }

  public static boolean isGeospatial(Type type) {
    return type.typeId() == Type.TypeID.GEOMETRY || type.typeId() == Type.TypeID.GEOGRAPHY;
  }

  public static void validateConfig(IcebergConfig config) {
    if (!config.geospatialTypes()) {
      return;
    }
    // Iceberg ORC writers don't support geospatial types
    if (FileFormat.ORC.name().equalsIgnoreCase(config.writeFormat())) {
      throw new DebeziumException(
          "debezium.sink.iceberg.geospatial-types is not supported with the orc write format,"
              + " use parquet");
    }
    if (!GEOMETRY_CRS.matcher(config.geometryCrs()).matches()) {
      throw new DebeziumException(
          "debezium.sink.iceberg.geometry-crs must be OGC:CRS84, EPSG:<N> (N > 0) or srid:0"
              + " (unknown CRS), got: "
              + config.geometryCrs());
    }
    int formatVersion;
    try {
      formatVersion = Integer.parseInt(config.formatVersion());
    } catch (NumberFormatException e) {
      throw new DebeziumException(
          "invalid debezium.sink.iceberg.format-version: " + config.formatVersion(), e);
    }
    if (formatVersion < 3) {
      throw new DebeziumException(
          "debezium.sink.iceberg.geospatial-types requires debezium.sink.iceberg.format-version"
              + " 3 or higher");
    }
  }
}
