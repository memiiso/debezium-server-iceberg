package io.debezium.server.iceberg.offset;

import io.debezium.config.Configuration;
import io.debezium.server.iceberg.BaseIcebergStorageConfig;


public  class IcebergOffsetBackingStoreConfig extends BaseIcebergStorageConfig {
    public IcebergOffsetBackingStoreConfig(Configuration config, String configuration_field_prefix) {
      super(config, configuration_field_prefix);
    }

    @Override
    public String tableName() {
      return this.configCombined.getProperty("table-name", "debezium_offset_storage");
    }

    public String getMigrateOffsetFile() {
      return this.configCombined.getProperty("migrate-offset-file","");
    }

  }
