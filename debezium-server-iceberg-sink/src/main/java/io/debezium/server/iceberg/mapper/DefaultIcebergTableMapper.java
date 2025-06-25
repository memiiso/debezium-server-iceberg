package io.debezium.server.iceberg.mapper;

import io.debezium.server.iceberg.GlobalConfig;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@Named("default-mapper")
@Dependent
public class DefaultIcebergTableMapper implements IcebergTableMapper {
    @Inject
    GlobalConfig config;

    @Override
    public TableIdentifier mapDestination(String destination) {
        final String tableName = destination
                .replaceAll(config.iceberg().destinationRegexp().orElse(""), config.iceberg().destinationRegexpReplace().orElse(""))
                .replace(".", "_");

        if (config.iceberg().destinationUppercaseTableNames()) {
            return TableIdentifier.of(Namespace.of(config.iceberg().namespace()), (config.iceberg().tablePrefix().orElse("") + tableName).toUpperCase());
        } else if (config.iceberg().destinationLowercaseTableNames()) {
            return TableIdentifier.of(Namespace.of(config.iceberg().namespace()), (config.iceberg().tablePrefix().orElse("") + tableName).toLowerCase());
        } else {
            return TableIdentifier.of(Namespace.of(config.iceberg().namespace()), config.iceberg().tablePrefix().orElse("") + tableName);
        }
    }
}
