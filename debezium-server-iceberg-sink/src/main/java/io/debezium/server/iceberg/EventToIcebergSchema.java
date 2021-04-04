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
import org.apache.iceberg.SortOrder;
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
  private final SortOrder sortOrder;
  private final String primaryKey;

  public EventToIcebergSchema(byte[] event) throws IOException {
    // @TODO move the logic here?!
    schema = IcebergUtil.getIcebergSchema(IcebergUtil.jsonObjectMapper.readTree(event));
    sortOrder = null;
    primaryKey = null;
  }

  public Schema getSchema() {
    return schema;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public String getPrimaryKey() {
    return primaryKey;
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
      // @TODO extract PK from schema and create iceberg RowIdentifier, and sort order
      // @TODO use schema of key event to create primary key definition! for upsert
      if (this.sortOrder != null) {
        tb.withSortOrder(sortOrder);
      }
      if (this.primaryKey != null) {
        LOGGER.trace("@TODO waiting spec v2");
      }
      LOGGER.warn("Creating table '{}'\nWith\nschema:{}\nsortOrder:{}", tableIdentifier, schema, sortOrder);
      return tb.create();
    }
    return null;
  }

}
