/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.google.common.primitives.Ints;
import io.debezium.DebeziumException;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;


/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);


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
    String className = instance.getClass().getName();
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple '" + className + "' class instances named '" + name + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No '" + className + "' class instance named '" + name + "' is available");
    }

    LOGGER.info("Using {}", className);
    return instance.get();
  }

  public static void createNamespaceIfNotExists(Catalog icebergCatalog, Namespace namespace) {

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(namespace)) {
      try {
        ((SupportsNamespaces) icebergCatalog).createNamespace(namespace);
        LOGGER.warn("Created namespace:'{}'", namespace);
      } catch (AlreadyExistsException e) {
        // ignore
      }
    }
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {
    createNamespaceIfNotExists(icebergCatalog, tableIdentifier.namespace());

    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, String formatVersion) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());
    createNamespaceIfNotExists(icebergCatalog, tableIdentifier.namespace());

    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperty(FORMAT_VERSION, formatVersion)
        .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
        .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
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
    if (icebergCatalog.tableExists(tableId)){
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    }
    LOGGER.debug("Table not found: {}", tableId.toString());
    return Optional.empty();
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    final Set<Integer> identifierFieldIds = icebergTable.schema().identifierFieldIds();
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          null,
          null,
          null)
          .setAll(icebergTable.properties());
    } else {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          Ints.toArray(identifierFieldIds),
          TypeUtil.select(icebergTable.schema(), Sets.newHashSet(identifierFieldIds)),
          null)
          .setAll(icebergTable.properties());
    }
  }

  public static OutputFileFactory getTableOutputFileFactory(Table icebergTable, FileFormat format) {
    return OutputFileFactory.builderFor(icebergTable,
            IcebergUtil.partitionId(), 1L)
        .defaultSpec(icebergTable.spec())
        .operationId(UUID.randomUUID().toString())
        .format(format)
        .build();
  }

  public static int partitionId() {
    return Integer.parseInt(dtFormater.format(Instant.now()));
  }

}
