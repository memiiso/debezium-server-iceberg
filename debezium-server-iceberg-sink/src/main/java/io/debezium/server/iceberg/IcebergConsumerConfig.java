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
    public static final String PROP_PREFIX = "debezium.sink.iceberg";
    @WithName(value = PROP_PREFIX)
    public Map<String, String> icebergConfigs();

    @WithName(value = "debezium.sink.iceberg.upsert-op-field")
    @WithDefault(value = "__op")
    public String cdcOpField();
    @WithName(value = "debezium.sink.iceberg.upsert-dedup-column")
    @WithDefault(value = "__source_ts_ms")
    public String cdcSourceTsMsField();

    @WithName(value = "debezium.source.time.precision.mode")
    public TemporalPrecisionMode temporalPrecisionMode();

    @WithName(value = "debezium.format.value")
    @WithDefault(value = "json")
    String valueFormat();
    @WithName(value = "debezium.format.key")
    @WithDefault(value = "json")
    String keyFormat();
    @WithName(value = "debezium.sink.iceberg.upsert")
    @WithDefault(value = "true")
    public boolean upsert();
    @WithName(value = "debezium.sink.iceberg." + CatalogProperties.WAREHOUSE_LOCATION)
    String warehouseLocation();
    @WithName(value = "debezium.sink.iceberg.destination-regexp")
//    @WithDefault(value = "")
    public Optional<String> destinationRegexp();
    @WithName(value = "debezium.sink.iceberg.destination-regexp-replace")
//    @WithDefault(value = "")
    public Optional<String> destinationRegexpReplace();
    @WithName(value = "debezium.sink.iceberg.destination-uppercase-table-names")
    @WithDefault(value = "false")
    public boolean destinationUppercaseTableNames();
    @WithName(value = "debezium.sink.iceberg.destination-lowercase-table-names")
    @WithDefault(value = "false")
    public boolean destinationLowercaseTableNames();
    @WithName(value = "debezium.sink.iceberg.table-prefix")
//    @WithDefault(value = "")
    Optional<String> tablePrefix();
    @WithName(value = "debezium.sink.iceberg.table-namespace")
    @WithDefault(value = "default")
    public String namespace();
    @WithName(value = "debezium.sink.iceberg.catalog-name")
    @WithDefault(value = "default")
    String catalogName();
    @WithName(value = "debezium.sink.iceberg.create-identifier-fields")
    @WithDefault(value = "true")
    public boolean createIdentifierFields();
    @WithName(value = "debezium.sink.batch.batch-size-wait")
    @WithDefault(value = "NoBatchSizeWait")
    String batchSizeWaitName();
    @WithName(value = "debezium.format.value.schemas.enable")
    @WithDefault(value = "false")
    boolean eventSchemaEnabled();
    @WithName(value = "debezium.sink.iceberg." + DEFAULT_FILE_FORMAT)
    @WithDefault(value = DEFAULT_FILE_FORMAT_DEFAULT)
    String writeFormat();
    @WithName(value = "debezium.sink.iceberg.allow-field-addition")
    @WithDefault(value = "true")
    public boolean allowFieldAddition();
}