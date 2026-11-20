/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.converter;

import io.debezium.converters.iceberg.IcebergConverterConfig;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.server.iceberg.GlobalConfig;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Adapts Quarkus SmallRye {@link GlobalConfig} to the pure-Java {@link IcebergConverterConfig}
 * contract.
 */
public final class SinkIcebergConverterConfig implements IcebergConverterConfig {

  private final GlobalConfig config;

  public SinkIcebergConverterConfig(GlobalConfig config) {
    this.config = Objects.requireNonNull(config, "config must not be null");
  }

  public static SinkIcebergConverterConfig from(GlobalConfig config) {
    return new SinkIcebergConverterConfig(config);
  }

  @Override
  public TemporalPrecisionMode temporalPrecisionMode() {
    return config.debezium().temporalPrecisionMode();
  }

  @Override
  public boolean eventFlatteningEnabled() {
    return config.debezium().isEventFlatteningEnabled();
  }

  @Override
  public boolean createIdentifierFields() {
    return config.iceberg().createIdentifierFields();
  }

  @Override
  public boolean preserveRequiredProperty() {
    return config.iceberg().preserveRequiredProperty();
  }

  @Override
  public boolean replaceNullWithDefault() {
    return config.iceberg().replaceNullWithDefault();
  }

  @Override
  public boolean nestedAsVariant() {
    return config.iceberg().nestedAsVariant();
  }

  @Override
  public List<String> excludedColumns() {
    return config.iceberg().excludedColumns().orElse(Collections.emptyList());
  }

  @Override
  public boolean geospatialTypesEnabled() {
    return config.iceberg().geospatialTypesEnabled();
  }

  @Override
  public String geometryCrs() {
    return config.iceberg().geometryCrs();
  }

  @Override
  public boolean geospatialAllowMissingSrid() {
    return config.iceberg().geospatialAllowMissingSrid();
  }

  @Override
  public int schemaCacheSize() {
    return 0;
  }
}
