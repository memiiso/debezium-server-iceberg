/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.TableProperties.*;

/**
 *
 * @author Ismail Simsek
 */
public class DebeziumToIcebergTable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumToIcebergTable.class);

  private final List<Types.NestedField> tableColumns;
  private final List<Types.NestedField> tableRowIdentifierColumns;

  public DebeziumToIcebergTable(byte[] eventVal, byte[] eventKey) {
    tableColumns = extractSchema(eventVal);
    tableRowIdentifierColumns = (eventKey == null) ? null : extractSchema(eventKey);
  }

  public DebeziumToIcebergTable(byte[] eventVal) {
    this(eventVal, null);
  }

  private List<Types.NestedField> extractSchema(byte[] eventVal) {
    try {
      JsonNode jsonEvent = IcebergUtil.jsonObjectMapper.readTree(eventVal);
      if (IcebergUtil.hasSchema(jsonEvent)) {
        return IcebergUtil.getIcebergSchema(jsonEvent.get("schema"));
      }

      LOGGER.trace("Event schema not found in the given data:!");
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean hasSchema() {
    return tableColumns != null;
  }

  private SortOrder getSortOrder(Schema schema) {
    SortOrder so = SortOrder.unsorted();

    if (this.tableRowIdentifierColumns != null) {
      SortOrder.Builder sob = SortOrder.builderFor(schema);
      for (Types.NestedField coll : tableRowIdentifierColumns) {
        sob = sob.asc(coll.name(), NullOrder.NULLS_FIRST);
      }
      so = sob.build();
    }

    return so;
  }

  private Set<Integer> getRowIdentifierFieldIds() {

    if (this.tableRowIdentifierColumns == null) {
      return ImmutableSet.of();
    }

    Set<Integer> identifierFieldIds = new HashSet<>();

    for (Types.NestedField ic : this.tableRowIdentifierColumns) {
      boolean found = false;

      ListIterator<Types.NestedField> colsIterator = this.tableColumns.listIterator();
      while (colsIterator.hasNext()) {
        Types.NestedField tc = colsIterator.next();
        if (Objects.equals(tc.name(), ic.name())) {
          identifierFieldIds.add(tc.fieldId());
          // set columns as required its part of identifier filed
          colsIterator.set(tc.asRequired());
          found = true;
          break;
        }
      }

      if (!found) {
        throw new ValidationException("Table Row identifier field `" + ic.name() + "` not found in table columns");
      }

    }

    return identifierFieldIds;
  }

  public Table create(Catalog icebergCatalog, TableIdentifier tableIdentifier, String writeFormat) {

    Schema schema = new Schema(this.tableColumns, getRowIdentifierFieldIds());

    if (this.hasSchema()) {
      Catalog.TableBuilder tb = icebergCatalog.buildTable(tableIdentifier, schema)
          .withProperty(FORMAT_VERSION, "2")
          .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
          .withSortOrder(getSortOrder(schema));

      LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
          schema.identifierFieldNames());

      return tb.create();
    }

    throw new RuntimeException("Failed to create table "+ tableIdentifier);
  }

}
