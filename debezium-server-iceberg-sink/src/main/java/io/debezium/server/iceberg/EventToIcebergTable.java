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
public class EventToIcebergTable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(EventToIcebergTable.class);

  private final Schema schemaTable;
  private final Schema schemaTableRowKeyIdentifier;

  public EventToIcebergTable(byte[] eventKey, byte[] eventVal) throws IOException {
    schemaTable = extractSchema(eventVal);
    schemaTableRowKeyIdentifier = extractSchema(eventKey);
  }

  public EventToIcebergTable(byte[] eventVal) throws IOException {
    schemaTable = extractSchema(eventVal);
    schemaTableRowKeyIdentifier = null;
  }

  private Schema extractSchema(byte[] eventVal) throws IOException {

    JsonNode jsonEvent = IcebergUtil.jsonObjectMapper.readTree(eventVal);

    if (IcebergUtil.hasSchema(jsonEvent)) {
      return IcebergUtil.getIcebergSchema(jsonEvent.get("schema"));
    }

    LOGGER.trace("Event schema not found in the given data:!");
    return null;
  }

  public Schema getSchemaTable() {
    return schemaTable;
  }

  public Schema getSchemaTableRowKeyIdentifier() {
    return schemaTableRowKeyIdentifier;
  }

  private Schema getIcebergSchema(JsonNode eventSchema) {
    return IcebergUtil.getIcebergSchema(eventSchema);
  }

  public boolean hasSchema() {
    return schemaTable != null;
  }

  public Table create(Catalog icebergCatalog, TableIdentifier tableIdentifier) {

    if (this.hasSchema()) {
      Catalog.TableBuilder tb = icebergCatalog.buildTable(tableIdentifier, this.schemaTable);

      if (this.schemaTableRowKeyIdentifier != null) {
        SortOrder.Builder sob = SortOrder.builderFor(schemaTable);
        for (Types.NestedField coll : schemaTableRowKeyIdentifier.columns()) {
          sob = sob.asc(coll.name(), NullOrder.NULLS_FIRST);
        }
        tb.withSortOrder(sob.build());
        // "@TODO waiting spec v2 // use as PK / RowKeyIdentifier
      }

      LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schemaTable,
          schemaTableRowKeyIdentifier);
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
