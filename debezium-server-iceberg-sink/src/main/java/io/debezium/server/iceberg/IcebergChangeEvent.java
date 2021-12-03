/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;

import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class IcebergChangeEvent<K,V> implements ChangeEvent<K,V> {

  private final ChangeEvent<K,V> event;
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeEvent.class);
  protected static final ObjectMapper objectMapper = new ObjectMapper();

  public IcebergChangeEvent(ChangeEvent<K,V> e) {
    event = e;
  }

  @Override
  public K key() {
    return event.key();
  }

  @Override
  public V value() {
    return event.value();
  }

  @Override
  public String destination() {
    return event.destination();
  }
  
  public String destinationTable() {
    return event.destination().replace(".","_");
  }

  public GenericRecord getIcebergRecord(Schema schema, JsonNode data) {
    return IcebergUtil.getIcebergRecord(schema.asStruct(), data);
  }
  
  public Schema getSchema() {
    
    if (this.value() == null) {
      throw new RuntimeException("Failed to get event schema event value is null, destination:" + this.destination());
    }

    List<Types.NestedField> tableColumns = IcebergUtil.getIcebergFieldsFromEventSchema(getBytes(this.value()));
    List<Types.NestedField> keyColumns =
        IcebergUtil.getIcebergFieldsFromEventSchema(this.key() == null ? null : getBytes(this.key()));

    if (tableColumns.isEmpty()) {
      throw new RuntimeException("Failed to get schema destination:" + this.destination());
    }

    return getSchema(tableColumns, keyColumns);
  }

  private Schema getSchema(List<Types.NestedField> tableColumns,
                                 List<Types.NestedField> keyColumns) {

    Set<Integer> identifierFieldIds = new HashSet<>();

    for (Types.NestedField ic : keyColumns) {
      boolean found = false;

      ListIterator<Types.NestedField> colsIterator = tableColumns.listIterator();
      while (colsIterator.hasNext()) {
        Types.NestedField tc = colsIterator.next();
        if (Objects.equals(tc.name(), ic.name())) {
          identifierFieldIds.add(tc.fieldId());
          // set column as required its part of identifier filed
          colsIterator.set(tc.asRequired());
          found = true;
          break;
        }
      }

      if (!found) {
        throw new ValidationException("Table Row identifier field `" + ic.name() + "` not found in table columns");
      }

    }

    return new Schema(tableColumns, identifierFieldIds);
  }


  protected byte[] getBytes(Object object) {
    if (object instanceof byte[]) {
      return (byte[]) object;
    }
    else if (object instanceof String) {
      return ((String) object).getBytes();
    }
    throw new DebeziumException(unsupportedTypeMessage(object));
  }

  protected String unsupportedTypeMessage(Object object) {
    final String type = (object == null) ? "null" : object.getClass().getName();
    return "Unexpected data type '" + type + "'";
  }
}
