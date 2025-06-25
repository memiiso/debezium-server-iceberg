package io.debezium.server.iceberg.mapper;

import io.debezium.server.iceberg.GlobalConfig;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@Named("custom-mapper")
@Dependent
public class CustomMapper implements IcebergTableMapper {
    @Inject
    GlobalConfig config;

    @Override
    public TableIdentifier mapDestination(String destination) {
        String[] parts = destination.split("\\.");
        String tableName = parts[2];
        return TableIdentifier.of(Namespace.of(config.iceberg().namespace()), "CUSTOM_MAPPER_" + tableName);
    }
}
