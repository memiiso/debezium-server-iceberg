package io.debezium.server.iceberg;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigRoot
@ConfigMapping
public interface DebeziumConfig {

  @WithName("debezium.source.time.precision.mode")
  @WithDefault("isostring")
  TemporalPrecisionMode temporalPrecisionMode();

  @WithName("debezium.source.decimal.handling.mode")
  @WithDefault("double")
  RelationalDatabaseConnectorConfig.DecimalHandlingMode decimalHandlingMode();

  // Event format
  @WithName("debezium.format.value")
  @WithDefault("json")
  String valueFormat();

  @WithName("debezium.format.key")
  @WithDefault("json")
  String keyFormat();

  @WithName("debezium.format.value.schemas.enable")
  @WithDefault("true")
  boolean eventSchemaEnabled();

  @WithName("debezium.format.key.schemas.enable")
  @WithDefault("true")
  boolean eventKeySchemaEnabled();

  // SET RECOMMENDED DEFAULT VALUES FOR DEBEZIUM CONFIGS
  //# Save debezium offset state to destination, iceberg table
  @WithName("debezium.source.offset.storage")
  @WithDefault("io.debezium.server.iceberg.offset.IcebergOffsetBackingStore")
  String offsetStorage();

  @WithName("debezium.source.offset.storage.iceberg.table-name")
  @WithDefault("_debezium_offset_storage")
  String offsetStorageTable();

  // Save schema history to iceberg table
  @WithName("debezium.source.schema.history.internal")
  @WithDefault("io.debezium.server.iceberg.history.IcebergSchemaHistory")
  String schemaHistoryStorage();

  @WithName("debezium.source.schema.history.internal.iceberg.table-name")
  @WithDefault("_debezium_database_history_storage")
  String schemaHistoryStorageTable();

  //  Event flattening. unwrap message!
  @WithName("debezium.transforms")
  @WithDefault("unwrap")
  String transforms();

  @WithName("debezium.transforms.unwrap.type")
  @WithDefault("io.debezium.transforms.ExtractNewRecordState")
  String unwrapType();

  @WithName("debezium.transforms.unwrap.add.fields")
  @WithDefault("op,table,source.ts_ms,source.ts_ns,db,ts_ms")
  String unwrapAddFields();

  @WithName("debezium.transforms.unwrap.delete.handling.mode")
  @WithDefault("rewrite")
  String unwrapDeleteHandlingMode();

  @WithName("debezium.transforms.unwrap.drop.tombstones")
  @WithDefault("true")
  String unwrapDeleteTombstoneHandlingMode();

  default boolean isIsoStringTemporalMode() {
    return temporalPrecisionMode() == TemporalPrecisionMode.ISOSTRING;
  }

  default boolean isAdaptiveTemporalMode() {
    return temporalPrecisionMode() == TemporalPrecisionMode.ADAPTIVE ||
        temporalPrecisionMode() == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS;
  }
}