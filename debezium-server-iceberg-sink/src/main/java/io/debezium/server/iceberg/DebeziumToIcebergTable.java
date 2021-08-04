/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class DebeziumToIcebergTable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumToIcebergTable.class);

  private final Schema tableSchema;
  private final Schema tableRowIdentifierSchema;

  public DebeziumToIcebergTable(byte[] eventKey, byte[] eventVal) throws IOException {
    tableSchema = extractSchema(eventVal);
    tableRowIdentifierSchema = (eventVal == null) ? null : extractSchema(eventKey);
  }

  public DebeziumToIcebergTable(byte[] eventVal) throws IOException {
    this(eventVal, null);
  }

  private Schema extractSchema(byte[] eventVal) throws IOException {

    JsonNode jsonEvent = IcebergUtil.jsonObjectMapper.readTree(eventVal);

    if (IcebergUtil.hasSchema(jsonEvent)) {
      return IcebergUtil.getIcebergSchema(jsonEvent.get("schema"));
    }

    LOGGER.trace("Event schema not found in the given data:!");
    return null;
  }

  public Schema getTableSchema() {
    return tableSchema;
  }

  public Schema getTableRowIdentifierSchema() {
    return tableRowIdentifierSchema;
  }

  private Schema getIcebergSchema(JsonNode eventSchema) {
    return IcebergUtil.getIcebergSchema(eventSchema);
  }

  public boolean hasSchema() {
    return tableSchema != null;
  }

  private SortOrder getSortOrder() {
    SortOrder so = SortOrder.unsorted();

    if (this.tableRowIdentifierSchema != null) {
      SortOrder.Builder sob = SortOrder.builderFor(tableSchema);
      for (Types.NestedField coll : tableRowIdentifierSchema.columns()) {
        sob = sob.asc(coll.name(), NullOrder.NULLS_FIRST);
      }
      so = sob.build();
    }

    return so;
  }

  public Table create(Catalog icebergCatalog, TableIdentifier tableIdentifier) {

    if (this.hasSchema()) {
      Catalog.TableBuilder tb = icebergCatalog.buildTable(tableIdentifier, this.tableSchema)
          .withProperty("format-version", "2")
          .withSortOrder(getSortOrder());

      LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, tableSchema,
          tableRowIdentifierSchema);

      Table table = tb.create();
      if (tableRowIdentifierSchema != null) {
        table.updateSchema().setIdentifierFields(tableRowIdentifierSchema.identifierFieldNames()).commit();
      }

      return table;
    }

    return null;
  }

}
