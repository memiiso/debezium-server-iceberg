package io.debezium.server.iceberg;

import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

record IcebergChangeEventSchemaData(List<Types.NestedField> fields, Set<Integer> identifierFieldIds,
                                    AtomicInteger nextFieldId) {


  public IcebergChangeEventSchemaData(Integer nextFieldId) {
    this(new ArrayList<>(), new HashSet<>(), new AtomicInteger(nextFieldId));
  }

  public IcebergChangeEventSchemaData() {
    this(new ArrayList<>(), new HashSet<>(), new AtomicInteger(1));
  }

  public IcebergChangeEventSchemaData copyKeepNextFieldId() {
    return new IcebergChangeEventSchemaData(new ArrayList<>(), new HashSet<>(), this.nextFieldId);
  }


}
