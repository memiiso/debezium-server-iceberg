package io.debezium.server.iceberg.converter;

import org.apache.iceberg.Schema;

public interface SchemaConverter {
  @Override
  int hashCode();

  @Override
  boolean equals(Object o);

  Schema icebergSchema() ;
}
