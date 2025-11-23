package io.debezium.server.iceberg.converter;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;

public interface SchemaConverter {
  @Override
  int hashCode();

  @Override
  boolean equals(Object o);

  Schema icebergSchema() ;

  SortOrder sortOrder(Schema schema);
}
