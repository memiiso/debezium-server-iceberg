/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.history;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.*;
import io.debezium.util.FunctionalReadWriteLock;
import io.debezium.util.Strings;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * A {@link SchemaHistory} implementation that stores the schema history to database table
 *
 * @author Ismail Simsek
 */
@ThreadSafe
@Incubating
public final class IcebergSchemaHistory extends AbstractSchemaHistory {

  static final Schema DATABASE_HISTORY_TABLE_SCHEMA = new Schema(
      required(1, "id", Types.StringType.get()),
      optional(2, "history_data", Types.StringType.get()),
      optional(3, "record_insert_ts", Types.TimestampType.withZone()
      )
  );
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSchemaHistory.class);
  private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
  private final DocumentWriter writer = DocumentWriter.defaultWriter();
  private final DocumentReader reader = DocumentReader.defaultReader();
  private final AtomicBoolean running = new AtomicBoolean();
  IcebergSchemaHistoryConfig historyConfig;
  Catalog icebergCatalog;
  private String tableFullName;
  private TableIdentifier tableId;
  private Table historyTable;

  @Override
  public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
    super.configure(config, comparator, listener, useCatalogBeforeSchema);
    this.historyConfig = new IcebergSchemaHistoryConfig(config);
    icebergCatalog = CatalogUtil.buildIcebergCatalog(this.historyConfig.catalogName(),
        this.historyConfig.icebergProperties(), this.historyConfig.hadoopConfig());
    tableFullName = String.format("%s.%s", this.historyConfig.catalogName(), this.historyConfig.tableName());
    tableId = TableIdentifier.of(Namespace.of(this.historyConfig.catalogName()), this.historyConfig.tableName());

    if (running.get()) {
      throw new SchemaHistoryException("Iceberg database history process already initialized table: " + tableFullName);
    }
  }

  @Override
  public void start() {
    super.start();
    LOG.info("Starting IcebergSchemaHistory storage table:" + tableFullName);
    lock.write(() -> {
      if (running.compareAndSet(false, true)) {
        try {
          if (!storageExists()) {
            initializeStorage();
          }
        } catch (Exception e) {
          throw new SchemaHistoryException("Unable to create history table: " + tableFullName + " : " + e.getMessage(),
              e);
        }
      }
    });
  }

  public String getTableFullName() {
    return tableFullName;
  }

  @Override
  protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
    if (record == null) {
      return;
    }
    lock.write(() -> {
      if (!running.get()) {
        throw new DebeziumException("The history has been stopped and will not accept more records");
      }
      try {
        String recordDocString = writer.write(record.document());
        LOG.trace("Saving history data {}", recordDocString);
        OffsetDateTime currentTs = OffsetDateTime.now(ZoneOffset.UTC);
        /// iceberg append
        GenericRecord icebergRecord = GenericRecord.create(DATABASE_HISTORY_TABLE_SCHEMA);
        Record row = icebergRecord.copy(
            "id", UUID.randomUUID().toString(),
            "history_data", recordDocString,
            "record_insert_ts", currentTs
        );
        OutputFile out;
        try (FileIO tableIo = historyTable.io()) {
          out = tableIo.newOutputFile(historyTable.locationProvider().newDataLocation(UUID.randomUUID() + "-data-001"));
        }
        FileAppender<Record> writer = Parquet.write(out)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .forTable(historyTable)
            .overwrite()
            .build();
        try (Closeable ignored = writer) {
          writer.add(row);
        }
        DataFile dataFile = DataFiles.builder(historyTable.spec())
            .withFormat(FileFormat.PARQUET)
            .withPath(out.location())
            .withFileSizeInBytes(writer.length())
            .withSplitOffsets(writer.splitOffsets())
            .withMetrics(writer.metrics())
            .build();
        historyTable.newOverwrite().addFile(dataFile).commit();
        /// END iceberg append
        LOG.trace("Successfully saved history data to Iceberg table");
      } catch (IOException e) {
        throw new SchemaHistoryException("Failed to store record: " + record, e);
      }
    });
  }

  @Override
  public void stop() {
    running.set(false);
    super.stop();
  }

  @Override
  protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
    lock.write(() -> {
      if (exists()) {
        try (CloseableIterable<Record> rs = IcebergGenerics.read(historyTable)
            .build()) {
          for (Record row : rs) {
            String line = (String) row.getField("history_data");
            if (line == null) {
              break;
            }
            if (!line.isEmpty()) {
              records.accept(new HistoryRecord(reader.read(line)));
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Override
  public boolean storageExists() {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return table != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public boolean exists() {

    if (!storageExists()) {
      return false;
    }

    int numRows = 0;
    try (CloseableIterable<Record> rs = IcebergGenerics.read(historyTable)
        .build()) {
      for (Record ignored : rs) {
        numRows++;
        break;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return numRows > 0;
  }

  @Override
  public String toString() {
    return "Iceberg database history storage: " + (tableFullName != null ? tableFullName : "(unstarted)");
  }

  @Override
  public void initializeStorage() {
    if (!storageExists()) {
      try {
        LOG.debug("Creating table {} to store database history", tableFullName);
        historyTable = icebergCatalog.createTable(tableId, DATABASE_HISTORY_TABLE_SCHEMA);
        LOG.warn("Created database history storage table {} to store history", tableFullName);

        if (!Strings.isNullOrEmpty(historyConfig.getMigrateHistoryFile().strip())) {
          LOG.warn("Migrating history from file {}", historyConfig.getMigrateHistoryFile());
          this.loadFileSchemaHistory(new File(historyConfig.getMigrateHistoryFile()));
        }
      } catch (Exception e) {
        throw new SchemaHistoryException("Creation of database history topic failed, please create the topic manually", e);
      }
    } else {
      LOG.debug("Storage is exists, skipping initialization");
    }
  }

  private void loadFileSchemaHistory(File file) {
    LOG.warn(String.format("Migrating file database history from:'%s' to Iceberg database history storage: %s",
        file.toPath(), tableFullName));
    AtomicInteger numRecords = new AtomicInteger();
    lock.write(() -> {
      try (BufferedReader historyReader = Files.newBufferedReader(file.toPath())) {
        while (true) {
          String line = historyReader.readLine();
          if (line == null) {
            break;
          }
          if (!line.isEmpty()) {
            this.storeRecord(new HistoryRecord(reader.read(line)));
            numRecords.getAndIncrement();
          }
        }
      } catch (IOException e) {
        logger.error("Failed to migrate history record from history file at {}", file.toPath(), e);
      }
    });
    LOG.warn("Migrated {} database history record. " +
             "Migrating file database history to Iceberg database history storage successfully completed", numRecords.get());
  }

  public static class IcebergSchemaHistoryConfig {

    final org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    private final Configuration config;
    Map<String, String> icebergProperties = new ConcurrentHashMap<>();

    public IcebergSchemaHistoryConfig(Configuration config) {
      this.config = config;

      final Map<String, String> conf = new HashMap<>();
      this.config.forEach((propName, value) -> {
        if (propName.startsWith(CONFIGURATION_FIELD_PREFIX_STRING + "iceberg.")) {
          final String newPropName = propName.substring((CONFIGURATION_FIELD_PREFIX_STRING + "iceberg.").length());
          conf.put(newPropName, value);
        }
      });

      conf.forEach(hadoopConfig::set);
      icebergProperties.putAll(conf);
    }

    public String catalogName() {
      return this.config.getString(Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "iceberg.catalog-name").withDefault("default"));
    }

    public String tableName() {
      return this.config.getString(Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "iceberg.table-name").withDefault(
          "debezium_database_history_storage"));
    }

    public org.apache.hadoop.conf.Configuration hadoopConfig() {
      return hadoopConfig;
    }

    public Map<String, String> icebergProperties() {
      return icebergProperties;
    }

    public String getMigrateHistoryFile() {
      return this.config.getString(Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "iceberg.migrate-history-file").withDefault(""));
    }
  }

}
