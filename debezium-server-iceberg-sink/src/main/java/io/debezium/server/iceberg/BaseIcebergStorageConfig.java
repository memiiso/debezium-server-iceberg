package io.debezium.server.iceberg;

import com.google.common.collect.Maps;
import io.debezium.config.Configuration;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.Map;
import java.util.Properties;


public abstract class BaseIcebergStorageConfig {
  private static final String PROP_SINK_PREFIX = "debezium.sink.";
  public Properties configCombined = new Properties();

  public BaseIcebergStorageConfig(Configuration config, String configuration_field_prefix) {
    Configuration confIcebergSubset = config.subset(configuration_field_prefix + "iceberg.", true);
    confIcebergSubset.forEach(configCombined::put);

    // debezium is doing config filtering before passing it down to this class!
    // so we are taking additional config using ConfigProvider with this we take full iceberg config
    Map<String, String> icebergConf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_SINK_PREFIX + "iceberg.");
    icebergConf.forEach(configCombined::putIfAbsent);
  }

  public String catalogName() {
    return this.configCombined.getProperty("catalog-name", "default");
  }

  public String tableNamespace() {
    return this.configCombined.getProperty("table-namespace", "default");
  }

  abstract public String tableName();

  public org.apache.hadoop.conf.Configuration hadoopConfig() {
    final org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    configCombined.forEach((key, value) -> hadoopConfig.set((String) key, (String) value));
    return hadoopConfig;
  }

  public Map<String, String> icebergProperties() {
    return Maps.fromProperties(configCombined);
  }

}
