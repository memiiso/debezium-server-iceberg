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
    String regexp = config.iceberg().destinationRegexp().orElse("");
    if (!regexp.isEmpty()) {
      tableName =
          tableName.replaceAll(regexp, config.iceberg().destinationRegexpReplace().orElse(""));
    }
    tableName = tableName.replace(".", "_");

    String finalTableName = config.iceberg().tablePrefix().orElse("") + tableName;
    if (config.iceberg().destinationUppercaseTableNames()) {
      finalTableName = finalTableName.toUpperCase(Locale.ROOT);
    } else if (config.iceberg().destinationLowercaseTableNames()) {
      finalTableName = finalTableName.toLowerCase(Locale.ROOT);
    }

    Namespace ns = IcebergUtil.parseNamespace(config.iceberg().namespace());
    return TableIdentifier.of(ns, finalTableName);
  }
}
