package io.debezium.server.iceberg;

import io.smallrye.config.WithConverter;

import java.util.List;
import java.util.Optional;

public interface IcebergTableConfig {

    @WithConverter(PartitionByConverter.class)
    Optional<List<String>> partitionBy();
}
