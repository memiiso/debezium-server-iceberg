/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.DebeziumException;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.TableProperties.*;

/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static Map<String, String> getConfigSubset(Config config, String prefix) {
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getValue(propName, String.class));
      }
    }

    return ret;
  }

  public static <T> T selectInstance(Instance<T> instances, String name) {

    Instance<T> instance = instances.select(NamedLiteral.of(name));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + name + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + name + "' is available");
    }

    LOGGER.info("Using {}", instance.getClass().getName());
    return instance.get();
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, boolean partition, String partitionField) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    final PartitionSpec ps;
    if (partition) {
      if (schema.findField(partitionField) == null) {
        LOGGER.warn("Table schema dont contain partition field {}! Creating table without partition", partition);
        ps = PartitionSpec.builderFor(schema).build();
      } else {
        ps = PartitionSpec.builderFor(schema).day(partitionField).build();
      }
    } else {
      ps = PartitionSpec.builderFor(schema).build();
    }

    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperty(FORMAT_VERSION, "2")
        .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
        .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
        .withPartitionSpec(ps)
        .create();
  }

  private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    for (String fieldName : schema.identifierFieldNames()) {
      sob = sob.asc(fieldName);
    }

    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.warn("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    return new GenericAppenderFactory(
        icebergTable.schema(),
        icebergTable.spec(),
        Ints.toArray(icebergTable.schema().identifierFieldIds()),
        icebergTable.schema(),
        null);
  }

}
