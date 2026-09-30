package io.debezium.server.iceberg.mapper;

import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergUtil;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.util.Locale;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@Named("default-mapper")
@Dependent
public class DefaultIcebergTableMapper implements IcebergTableMapper {
  @Inject GlobalConfig config;

  @Override
  public TableIdentifier mapDestination(String destination) {
    String tableName = destination;
    if (config.iceberg().destinationRegexp().isPresent()
        && !config.iceberg().destinationRegexp().get().isEmpty()) {
      tableName =
          tableName.replaceAll(
              config.iceberg().destinationRegexp().get(),
              config.iceberg().destinationRegexpReplace().orElse(""));
    }
    tableName = tableName.replace(".", "_");

    Namespace ns = IcebergUtil.parseNamespace(config.iceberg().namespace());
    if (config.iceberg().destinationUppercaseTableNames()) {
      return TableIdentifier.of(
          ns, (config.iceberg().tablePrefix().orElse("") + tableName).toUpperCase(Locale.ROOT));
    } else if (config.iceberg().destinationLowercaseTableNames()) {
      return TableIdentifier.of(
          ns, (config.iceberg().tablePrefix().orElse("") + tableName).toLowerCase(Locale.ROOT));
    } else {
      return TableIdentifier.of(ns, config.iceberg().tablePrefix().orElse("") + tableName);
    }
  }
}
