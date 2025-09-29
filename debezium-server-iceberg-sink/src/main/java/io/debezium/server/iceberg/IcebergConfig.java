package io.debezium.server.iceberg;

import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.util.PropertyUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

@ConfigRoot
@ConfigMapping
public interface IcebergConfig {
  String PROP_PREFIX = "debezium.sink.iceberg";
  String HADOOP_PROP_PREFIX = "debezium.sink.iceberg.hadoop";
  String CATALOG_PROP_PREFIX = "debezium.sink.iceberg.catalog";
  String TABLE_PROP_PREFIX = "debezium.sink.iceberg.table";

  @WithName(PROP_PREFIX)
  Map<String, String> icebergConfigs();

  @WithName(TABLE_PROP_PREFIX)
  Map<String, IcebergTableConfig> tableConfigs();

  @WithName(HADOOP_PROP_PREFIX)
  Map<String, String> icebergHadoopConfigs();

  @WithName(CATALOG_PROP_PREFIX)
  Map<String, String> icebergCatalogConfigs();

  @WithName("debezium.sink.iceberg.upsert-op-field")
  @WithDefault("__op")
  String cdcOpField();

  @WithName("debezium.sink.iceberg.upsert-dedup-column")
  @WithDefault("__source_ts_ns")
  String cdcSourceTsField();

  @WithName("debezium.sink.iceberg.upsert")
  @WithDefault("false")
  boolean upsert();

  @WithName("debezium.sink.iceberg.upsert-keep-deletes")
  @WithDefault("true")
  boolean keepDeletes();

  @WithName("debezium.sink.iceberg." + CatalogProperties.WAREHOUSE_LOCATION)
  String warehouseLocation();

  @WithName("debezium.sink.iceberg.table-mapper")
  @WithDefault("default-mapper")
  String tableMapper();

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

  @WithName("debezium.sink.iceberg." + DEFAULT_FILE_FORMAT)
  @WithDefault(DEFAULT_FILE_FORMAT_DEFAULT)
  String writeFormat();

  @WithName("debezium.sink.iceberg.allow-field-addition")
  @WithDefault("true")
  boolean allowFieldAddition();

  @WithName("debezium.sink.iceberg.excluded-columns")
  Optional<List<String>> excludedColumns();

  @WithName("debezium.sink.iceberg.io-impl")
  @WithDefault("org.apache.iceberg.io.ResolvingFileIO")
  String ioImpl();

  @WithName("debezium.sink.iceberg.preserve-required-property")
  @WithDefault("false")
  boolean preserveRequiredProperty();

  @WithName("debezium.sink.iceberg.nested-as-variant")
  @WithDefault("false")
  boolean nestedAsVariant();

}