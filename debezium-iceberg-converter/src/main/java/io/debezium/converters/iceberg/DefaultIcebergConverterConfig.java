/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg;

import io.debezium.converters.iceberg.geospatial.GeospatialTypes;
import io.debezium.jdbc.TemporalPrecisionMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.types.Types;

/**
 * Pure-Java immutable {@link IcebergConverterConfig} for Layer 1 {@link IcebergStructConverter}
 * users who do not need Kafka Connect {@code ConverterConfig} or wire serialization settings.
 */
public final class DefaultIcebergConverterConfig implements IcebergConverterConfig {

  private static final DefaultIcebergConverterConfig DEFAULTS = builder().build();

  private final TemporalPrecisionMode temporalPrecisionMode;
  private final boolean eventFlatteningEnabled;
  private final boolean createIdentifierFields;
  private final boolean preserveRequiredProperty;
  private final boolean replaceNullWithDefault;
  private final boolean nestedAsVariant;
  private final List<String> excludedColumns;
  private final boolean geospatialTypesEnabled;
  private final String geometryCrs;
  private final boolean geospatialAllowMissingSrid;

  private DefaultIcebergConverterConfig(Builder builder) {
    this.temporalPrecisionMode = builder.temporalPrecisionMode;
    this.eventFlatteningEnabled = builder.eventFlatteningEnabled;
    this.createIdentifierFields = builder.createIdentifierFields;
    this.preserveRequiredProperty = builder.preserveRequiredProperty;
    this.replaceNullWithDefault = builder.replaceNullWithDefault;
    this.nestedAsVariant = builder.nestedAsVariant;
    this.excludedColumns = builder.excludedColumns;
    this.geospatialTypesEnabled = builder.geospatialTypesEnabled;
    this.geometryCrs = builder.geometryCrs;
    this.geospatialAllowMissingSrid = builder.geospatialAllowMissingSrid;
    GeospatialTypes.validateConfig(this);
  }

  public static DefaultIcebergConverterConfig defaults() {
    return DEFAULTS;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public TemporalPrecisionMode temporalPrecisionMode() {
    return temporalPrecisionMode;
  }

  @Override
  public boolean eventFlatteningEnabled() {
    return eventFlatteningEnabled;
  }

  @Override
  public boolean createIdentifierFields() {
    return createIdentifierFields;
  }

  @Override
  public boolean preserveRequiredProperty() {
    return preserveRequiredProperty;
  }

  @Override
  public boolean replaceNullWithDefault() {
    return replaceNullWithDefault;
  }

  @Override
  public boolean nestedAsVariant() {
    return nestedAsVariant;
  }

  @Override
  public List<String> excludedColumns() {
    return excludedColumns;
  }

  @Override
  public boolean geospatialTypesEnabled() {
    return geospatialTypesEnabled;
  }

  @Override
  public String geometryCrs() {
    return geometryCrs;
  }

  @Override
  public boolean geospatialAllowMissingSrid() {
    return geospatialAllowMissingSrid;
  }

  public static final class Builder {

    private TemporalPrecisionMode temporalPrecisionMode = TemporalPrecisionMode.ISOSTRING;
    private boolean eventFlatteningEnabled = true;
    private boolean createIdentifierFields = true;
    private boolean preserveRequiredProperty = false;
    private boolean replaceNullWithDefault = false;
    private boolean nestedAsVariant = false;
    private List<String> excludedColumns = Collections.emptyList();
    private boolean geospatialTypesEnabled = false;
    private String geometryCrs = Types.GeometryType.DEFAULT_CRS;
    private boolean geospatialAllowMissingSrid = false;

    private Builder() {}

    public Builder temporalPrecisionMode(TemporalPrecisionMode temporalPrecisionMode) {
      this.temporalPrecisionMode =
          Objects.requireNonNull(temporalPrecisionMode, "temporalPrecisionMode must not be null");
      return this;
    }

    public Builder eventFlatteningEnabled(boolean eventFlatteningEnabled) {
      this.eventFlatteningEnabled = eventFlatteningEnabled;
      return this;
    }

    public Builder createIdentifierFields(boolean createIdentifierFields) {
      this.createIdentifierFields = createIdentifierFields;
      return this;
    }

    public Builder preserveRequiredProperty(boolean preserveRequiredProperty) {
      this.preserveRequiredProperty = preserveRequiredProperty;
      return this;
    }

    public Builder replaceNullWithDefault(boolean replaceNullWithDefault) {
      this.replaceNullWithDefault = replaceNullWithDefault;
      return this;
    }

    public Builder nestedAsVariant(boolean nestedAsVariant) {
      this.nestedAsVariant = nestedAsVariant;
      return this;
    }

    public Builder excludedColumns(List<String> excludedColumns) {
      if (excludedColumns == null) {
        this.excludedColumns = Collections.emptyList();
      } else {
        this.excludedColumns =
            excludedColumns.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
      }
      return this;
    }

    public Builder excludedColumns(String... excludedColumns) {
      return excludedColumns(excludedColumns == null ? null : Arrays.asList(excludedColumns));
    }

    public Builder geospatialTypesEnabled(boolean geospatialTypesEnabled) {
      this.geospatialTypesEnabled = geospatialTypesEnabled;
      return this;
    }

    public Builder geometryCrs(String geometryCrs) {
      this.geometryCrs = geometryCrs;
      return this;
    }

    public Builder geospatialAllowMissingSrid(boolean geospatialAllowMissingSrid) {
      this.geospatialAllowMissingSrid = geospatialAllowMissingSrid;
      return this;
    }

    public DefaultIcebergConverterConfig build() {
      return new DefaultIcebergConverterConfig(this);
    }
  }
}
