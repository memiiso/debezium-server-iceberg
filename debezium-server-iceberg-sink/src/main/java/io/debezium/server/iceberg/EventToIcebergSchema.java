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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class EventToIcebergSchema {
  protected static final Logger LOGGER = LoggerFactory.getLogger(EventToIcebergSchema.class);

  private final Schema schema;
  // @TODO align name to iceberg standard!
  private final String rowIdentifier;

  public EventToIcebergSchema(byte[] event) throws IOException {
    // @TODO move schema extraction logic here?!
    // todo fix the logic of schema node checking - if
    JsonNode jsonEvent = IcebergUtil.jsonObjectMapper.readTree(event);
    if (jsonEvent.has("schema")) {
      schema = IcebergUtil.getIcebergSchema(jsonEvent.get("schema"));
      LOGGER.debug("Extracted Iceberg schema: {}", schema);
    } else {
      LOGGER.trace("Event schema not found in the given event:!");
      schema = null;
    }
    // @TODO extract PK from schema and create iceberg RowIdentifier, and sort order
    rowIdentifier = null;
  }

  public Schema getSchema() {
    return schema;
  }

  public String getRowIdentifier() {
    return rowIdentifier;
  }

  private Schema getIcebergSchema(JsonNode eventSchema) {
    return IcebergUtil.getIcebergSchema(eventSchema);
  }

  public boolean hasSchema() {
    return schema != null;
  }

  public Table create(Catalog icebergCatalog, TableIdentifier tableIdentifier) {
    if (this.hasSchema()) {
      Catalog.TableBuilder tb = icebergCatalog.buildTable(tableIdentifier, this.schema);
      if (this.rowIdentifier != null) {
        LOGGER.trace("@TODO waiting spec v2");
        // + use it as sort order! tb.withSortOrder(sortOrder);
      }
      LOGGER.warn("Creating table '{}'\nWith\nschema:{}\n rowIdentifier:{}", tableIdentifier, schema, rowIdentifier);
      return tb.create();
    }
    return null;
  }

}
