/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.offset;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.server.iceberg.IcebergChangeConsumer;
import io.debezium.util.Strings;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import javax.enterprise.context.Dependent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Implementation of OffsetBackingStore that saves data to database table.
 */
@Dependent
public class IcebergOffsetBackingStore extends MemoryOffsetBackingStore implements OffsetBackingStore {

  static final Schema OFFSET_STORAGE_TABLE_SCHEMA = new Schema(
      required(1, "id", Types.StringType.get()),
      optional(2, "offset_data", Types.StringType.get()),
      optional(3, "record_insert_ts", Types.TimestampType.withZone()
      )
  );
  protected static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(IcebergOffsetBackingStore.class);
  protected Map<String, String> data = new HashMap<>();
  Catalog icebergCatalog;
  private String tableFullName;
  private TableIdentifier tableId;
  private Table offsetTable;
  IcebergOffsetBackingStoreConfig offsetConfig;
  public IcebergOffsetBackingStore() {
  }

  @Override
  public void configure(WorkerConfig config) {
    super.configure(config);
    offsetConfig = new IcebergOffsetBackingStoreConfig(Configuration.from(config.originalsStrings()));
    icebergCatalog = CatalogUtil.buildIcebergCatalog(offsetConfig.catalogName(),
        offsetConfig.icebergProperties(), offsetConfig.hadoopConfig());
    tableFullName = String.format("%s.%s", offsetConfig.catalogName(), offsetConfig.tableName());
    tableId = TableIdentifier.of(Namespace.of(offsetConfig.catalogName()), offsetConfig.tableName());
  }

  @Override
  public synchronized void start() {
    super.start();
    LOG.info("Starting IcebergOffsetBackingStore table:{}", tableFullName);
    initializeTable();
    load();
  }

  private void initializeTable() {
    if (icebergCatalog.tableExists(tableId)) {
      offsetTable = icebergCatalog.loadTable(tableId);
    } else {
      LOG.debug("Creating table {} to store offset", tableFullName);
      offsetTable = icebergCatalog.createTable(tableId, OFFSET_STORAGE_TABLE_SCHEMA);
      if (!icebergCatalog.tableExists(tableId)) {
        throw new DebeziumException("Failed to create table " + tableId + " to store offset");
      }

      if (!Strings.isNullOrEmpty(offsetConfig.getMigrateOffsetFile().strip())) {
        LOG.warn("Migrating offset from file {}", offsetConfig.getMigrateOffsetFile());
        this.loadFileOffset(new File(offsetConfig.getMigrateOffsetFile()));
      }

    }
  }

  private void loadFileOffset(File file) {
    try (SafeObjectInputStream is = new SafeObjectInputStream(Files.newInputStream(file.toPath()))) {
      Object obj = is.readObject();

      if (!(obj instanceof HashMap))
        throw new ConnectException("Expected HashMap but found " + obj.getClass());

      Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
      for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
        ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
        ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
        data.put(fromByteBuffer(key), fromByteBuffer(value));
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new DebeziumException("Failed migrating offset from file", e);
    }

    LOG.warn("Loaded file offset, saving it to iceberg offset storage");
    save();
  }

  protected void save() {
    LOG.debug("Saving offset data to iceberg table...");
    try {
      String dataJson = mapper.writeValueAsString(data);
      LOG.debug("Saving offset data {}", dataJson);
      OffsetDateTime currentTs = OffsetDateTime.now(ZoneOffset.UTC);

      GenericRecord record = GenericRecord.create(OFFSET_STORAGE_TABLE_SCHEMA);
      Record row = record.copy(
          "id", UUID.randomUUID().toString(),
          "offset_data", dataJson,
          "record_insert_ts", currentTs);
      OutputFile out;
      try (FileIO tableIo = offsetTable.io()) {
        out = tableIo.newOutputFile(offsetTable.locationProvider().newDataLocation(tableId.name() + "-data-001"));
      }
      FileAppender<Record> writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(offsetTable)
          .overwrite()
          .build();
      try (Closeable ignored = writer) {
        writer.add(row);
      }
      DataFile dataFile = DataFiles.builder(offsetTable.spec())
          .withFormat(FileFormat.PARQUET)
          .withPath(out.location())
          .withFileSizeInBytes(writer.length())
          .withSplitOffsets(writer.splitOffsets())
          .withMetrics(writer.metrics())
          .build();

      Transaction t = offsetTable.newTransaction();
      t.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
      t.newAppend().appendFile(dataFile).commit();
      t.commitTransaction();
      LOG.debug("Successfully saved offset data to iceberg table");

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void load() {
    try {
      String dataJsonString = null;

      int rowNum = 0;
      try (CloseableIterable<Record> rs = IcebergGenerics.read(offsetTable)
          .build()) {
        for (Record row : rs) {
          dataJsonString = (String) row.getField("offset_data");
          rowNum++;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (rowNum > 1) {
        throw new DebeziumException("Failed recover offset data from iceberg, Found multiple offset row!");
      }

      if (dataJsonString != null) {
        this.data = mapper.readValue(dataJsonString, new TypeReference<>() {
        });
        LOG.debug("Loaded offset data {}", dataJsonString);
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DebeziumException("Failed recover offset data from iceberg", e);
    }
  }

  @Override
  public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                          final Callback<Void> callback) {
    return executor.submit(() -> {
      for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
        if (entry.getKey() == null) {
          continue;
        }
        data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
      }
      save();
      if (callback != null) {
        callback.onCompletion(null, null);
      }
      return null;
    });
  }

  @Override
  public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
    return executor.submit(() -> {
      Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
      for (ByteBuffer key : keys) {
        result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
      }
      return result;
    });
  }

  public String fromByteBuffer(ByteBuffer data) {
    return (data != null) ? String.valueOf(StandardCharsets.UTF_16.decode(data.asReadOnlyBuffer())) : null;
  }

  public ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_16)) : null;
  }

  public static class IcebergOffsetBackingStoreConfig extends WorkerConfig {
    final org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    private final Configuration config;
    Map<String, String> icebergProperties = new ConcurrentHashMap<>();

    public IcebergOffsetBackingStoreConfig(Configuration config) {
      super(new ConfigDef(), config.asMap());
      this.config = config;

      final Map<String, String> conf = new HashMap<>();
      this.config.forEach((propName, value) -> {
        if (propName.startsWith(IcebergChangeConsumer.PROP_PREFIX)) {
          final String newPropName = propName.substring(IcebergChangeConsumer.PROP_PREFIX.length());
          conf.put(newPropName, value);
        }
      });

      conf.forEach(hadoopConfig::set);
      icebergProperties.putAll(conf);
    }

    public String catalogName() {
      return this.config.getString(Field.create("debezium.sink.iceberg.catalog-name").withDefault("default"));
    }

    public String tableName() {
      return this.config.getString(Field.create("offset.storage.iceberg.table-name").withDefault("debezium_offset_storage"));
    }

    public String getMigrateOffsetFile() {
      return this.config.getString(Field.create("offset.storage.iceberg.migrate-offset-file").withDefault(""));
    }

    public org.apache.hadoop.conf.Configuration hadoopConfig() {
      return hadoopConfig;
    }

    public Map<String, String> icebergProperties() {
      return icebergProperties;
    }
  }

}
