package io.debezium.server.iceberg;

import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper to perform operations in iceberg tables
 * @author Rafael Acevedo
 */
public class IcebergTableOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperations.class);

  private final Catalog catalog;

  public IcebergTableOperations(Catalog catalog) {
    this.catalog = catalog;
  }

  public Optional<Table> loadTable(TableIdentifier tableId) {
    try {
      Table table = catalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.warn("table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }
}
