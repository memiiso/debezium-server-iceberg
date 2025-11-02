package io.debezium.server.iceberg;

import java.util.List;
import java.util.Optional;

public interface IcebergTableConfig {

    Optional<List<String>> partitionBy();
}
