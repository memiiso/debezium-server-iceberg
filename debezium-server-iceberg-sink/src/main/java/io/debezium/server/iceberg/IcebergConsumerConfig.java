package io.debezium.server.iceberg;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.apache.iceberg.CatalogProperties;

import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

@ConfigRoot
@ConfigMapping
public interface IcebergConsumerConfig {
  String PROP_PREFIX = "debezium.sink.iceberg";

  @WithName(PROP_PREFIX)
  Map<String, String> icebergConfigs();

  @WithName("debezium.sink.iceberg.upsert-op-field")
  @WithDefault("__op")
  String cdcOpField();

  @WithName("debezium.sink.iceberg.upsert-dedup-column")
  @WithDefault("__source_ts_ms")
  String cdcSourceTsMsField();

  @WithName("debezium.source.time.precision.mode")
  @WithDefault("isostring")
  TemporalPrecisionMode temporalPrecisionMode();

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

  //
  @WithName("debezium.sink.iceberg.upsert")
  @WithDefault("true")
  boolean upsert();

  @WithName("debezium.sink.iceberg.upsert-keep-deletes")
  @WithDefault("true")
  boolean keepDeletes();

  @WithName("debezium.sink.iceberg." + CatalogProperties.WAREHOUSE_LOCATION)
  String warehouseLocation();

  @WithName("debezium.sink.iceberg.destination-regexp")
//    @WithDefault("")
  Optional<String> destinationRegexp();

  @WithName("debezium.sink.iceberg.destination-regexp-replace")
//    @WithDefault("")
  Optional<String> destinationRegexpReplace();

  @WithName("debezium.sink.iceberg.destination-uppercase-table-names")
  @WithDefault("false")
  boolean destinationUppercaseTableNames();

  @WithName("debezium.sink.iceberg.destination-lowercase-table-names")
  @WithDefault("false")
  boolean destinationLowercaseTableNames();

  @WithName("debezium.sink.iceberg.table-prefix")
//    @WithDefault("")
  Optional<String> tablePrefix();

  @WithName("debezium.sink.iceberg.table-namespace")
  @WithDefault("default")
  String namespace();

  @WithName("debezium.sink.iceberg.catalog-name")
  @WithDefault("default")
  String catalogName();

  @WithName("debezium.sink.iceberg.create-identifier-fields")
  @WithDefault("true")
  boolean createIdentifierFields();

  @WithName("debezium.sink.batch.batch-size-wait")
  @WithDefault("NoBatchSizeWait")
  String batchSizeWaitName();

  @WithName("debezium.sink.iceberg." + DEFAULT_FILE_FORMAT)
  @WithDefault(DEFAULT_FILE_FORMAT_DEFAULT)
  String writeFormat();

  @WithName("debezium.sink.iceberg.allow-field-addition")
  @WithDefault("true")
  boolean allowFieldAddition();

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
  @WithDefault("op,table,source.ts_ms,db,ts_ms")
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