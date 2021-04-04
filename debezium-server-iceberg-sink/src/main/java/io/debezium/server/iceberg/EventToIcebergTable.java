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
public class EventToIcebergTable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(EventToIcebergTable.class);

  private final Schema schemaTable;
  // @TODO align name to iceberg standard!
  private final Schema schemaTablePrimaryKey;

  public EventToIcebergTable(byte[] eventKey, byte[] eventVal) throws IOException {
    schemaTable = extractSchema(eventVal);
    schemaTablePrimaryKey = extractSchema(eventKey);
  }

  public EventToIcebergTable(byte[] eventVal) throws IOException {
    schemaTable = extractSchema(eventVal);
    schemaTablePrimaryKey = null;
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

  public Schema getSchemaTablePrimaryKey() {
    return schemaTablePrimaryKey;
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
      if (this.schemaTablePrimaryKey != null) {
        SortOrder.Builder sob = SortOrder.builderFor(schemaTable);
        for (Types.NestedField coll : schemaTablePrimaryKey.columns()) {
          sob = sob.asc(coll.name(), NullOrder.NULLS_FIRST);
        }
        tb.withSortOrder(sob.build());
        LOGGER.trace("@TODO waiting spec v2");
        // @TODO use as PK / row identifier
      }
      LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schemaTable,
          schemaTablePrimaryKey);
      return tb.create();
    }
    return null;
  }

}
