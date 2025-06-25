package io.debezium.server.iceberg.mapper;

import org.apache.iceberg.catalog.TableIdentifier;

public interface IcebergTableMapper {
    TableIdentifier mapDestination(String destination);
}
