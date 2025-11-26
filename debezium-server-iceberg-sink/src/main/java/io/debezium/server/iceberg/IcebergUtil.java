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
import org.apache.iceberg.PartitionSpec;
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
import org.apache.iceberg.util.Pair;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;


/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

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
                                         Schema schema, PartitionSpec partitionSpec, String writeFormat, String formatVersion) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}\npartitionSpec: {}", tableIdentifier, schema,
            schema.identifierFieldNames(), partitionSpec);
    createNamespaceIfNotExists(icebergCatalog, tableIdentifier.namespace());

    return icebergCatalog.buildTable(tableIdentifier, schema)
            .withProperty(FORMAT_VERSION, formatVersion)
            .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
            .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
            .withPartitionSpec(partitionSpec)
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

  /**
   * Creates an Iceberg {@link PartitionSpec} based on the provided schema and partitioning expressions.
   *
   * <p>
   * The partitioning expressions in the {@code partitionBy} list can be simple field names (for identity partitioning)
   * or transform expressions such as {@code year(field)}, {@code month(field)}, {@code day(field)}, {@code hour(field)},
   * {@code bucket(field, N)}, or {@code truncate(field, N)}.
   * </p>
   *
   * @param schema      the Iceberg schema to partition
   * @param partitionBy a list of partitioning expressions or field names
   * @return a {@link PartitionSpec} representing the partitioning strategy
   * @throws UnsupportedOperationException if an unsupported transform is specified
   * @throws IllegalArgumentException      if transform arguments are invalid
   */
  public static PartitionSpec createPartitionSpec(
          org.apache.iceberg.Schema schema, List<String> partitionBy) {
    if (partitionBy.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    partitionBy.forEach(
            partitionField -> {
              Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
              if (matcher.matches()) {
                String transform = matcher.group(1);
                switch (transform) {
                  case "year":
                  case "years":
                    specBuilder.year(matcher.group(2));
                    break;
                  case "month":
                  case "months":
                    specBuilder.month(matcher.group(2));
                    break;
                  case "day":
                  case "days":
                    specBuilder.day(matcher.group(2));
                    break;
                  case "hour":
                  case "hours":
                    specBuilder.hour(matcher.group(2));
                    break;
                  case "bucket":
                  {
                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                    specBuilder.bucket(args.first(), args.second());
                    break;
                  }
                  case "truncate":
                  {
                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                    specBuilder.truncate(args.first(), args.second());
                    break;
                  }
                  default:
                    throw new UnsupportedOperationException("Unsupported transform: " + transform);
                }
              } else {
                specBuilder.identity(partitionField);
              }
            });
    return specBuilder.build();
  }

  private static Pair<String, Integer> transformArgPair(String argsStr) {
    String[] parts = argsStr.split(",");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
    }
    return Pair.of(parts[0].trim(), Integer.parseInt(parts[1].trim()));
  }

}
