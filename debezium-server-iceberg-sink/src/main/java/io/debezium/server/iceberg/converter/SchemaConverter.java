package io.debezium.server.iceberg.converter;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;

public interface SchemaConverter {
  @Override
  int hashCode();

  @Override
  boolean equals(Object o);

  Schema icebergSchema(boolean withIdentifierFields);

  default Schema icebergSchema() {
    return icebergSchema(true);
  }

  SortOrder sortOrder(Schema schema);
}
