package io.debezium.server.iceberg;

import java.util.Map;

public interface IcebergTableConfig {
    Map<String, String> props();
}
