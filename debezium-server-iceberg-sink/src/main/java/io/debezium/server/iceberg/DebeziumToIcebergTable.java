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
import org.apache.iceberg.*;
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
    tableRowIdentifierSchema = extractSchema(eventKey);
  }

  public DebeziumToIcebergTable(byte[] eventVal) throws IOException {
    tableSchema = extractSchema(eventVal);
    tableRowIdentifierSchema = null;
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

  public Table create(Catalog icebergCatalog, TableIdentifier tableIdentifier) {

    if (this.hasSchema()) {
      Catalog.TableBuilder tb = icebergCatalog.buildTable(tableIdentifier, this.tableSchema);

      if (this.tableRowIdentifierSchema != null) {
        SortOrder.Builder sob = SortOrder.builderFor(tableSchema);
        for (Types.NestedField coll : tableRowIdentifierSchema.columns()) {
          sob = sob.asc(coll.name(), NullOrder.NULLS_FIRST);
        }
        tb.withSortOrder(sob.build());
        // "@TODO waiting spec v2 // use as PK / RowKeyIdentifier
      }

      LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, tableSchema,
          tableRowIdentifierSchema);
      Table table = tb.create();
      // @TODO remove once spec v2 released
      return upgradeToFormatVersion2(table);
    }

    return null;
  }

  // @TODO remove once spec v2 released! upgrading table to V2
  public Table upgradeToFormatVersion2(Table icebergTable) {
    TableOperations ops = ((BaseTable) icebergTable).operations();
    TableMetadata meta = ops.current();
    ops.commit(ops.current(), meta.upgradeToFormatVersion(2));
    icebergTable.refresh();
    return icebergTable;
  }

}
