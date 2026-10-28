/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.offset;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.tableoperator.UnpartitionedAppendWriter;
import io.debezium.storage.kafka.offset.KafkaOffsetStoreConverter;
import io.debezium.util.Strings;
import jakarta.enterprise.context.Dependent;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of OffsetBackingStore that saves data to Iceberg table. */
@Dependent
public class IcebergOffsetBackingStore extends MemoryOffsetBackingStore
    implements OffsetBackingStore {

  static final Schema OFFSET_STORAGE_TABLE_SCHEMA =
      new Schema(
          required(1, "id", Types.StringType.get()),
          optional(2, "offset_data", Types.StringType.get()),
          optional(3, "record_insert_ts", Types.TimestampType.withZone()));
  protected static final ObjectMapper mapper = new ObjectMapper();
  public static final String CONFIGURATION_FIELD_PREFIX_STRING = "offset.storage.";
  private static final Logger LOG = LoggerFactory.getLogger(IcebergOffsetBackingStore.class);
  // the store started by the Debezium engine, used by the consumer to commit the offsets
  // atomically with the data, see debezium.sink.iceberg.atomic-offset-commit. Static because the
  // engine instantiates the store by reflection, outside of CDI.
  private static volatile IcebergOffsetBackingStore instance;
  protected Map<String, String> data = new HashMap<>();
  // offset data as currently stored in the offset table
  private Map<String, String> persistedData = new HashMap<>();
  // namespace of the offset keys, the engine writes offsets under its own name
  private String engineName;
  Catalog icebergCatalog;
  private String tableFullName;
  private TableIdentifier tableId;
  private Table offsetTable;
  IcebergOffsetBackingStoreConfig storageConfig;
  FileFormat format;
  FileWriterFactory<Record> fileWriterFactory;
  OutputFileFactory fileFactory;

  public IcebergOffsetBackingStore() {}

  @Override
  public void configure(WorkerConfig config) {
    super.configure(config);

    storageConfig =
        new IcebergOffsetBackingStoreConfig(
            Configuration.from(config.originalsStrings()), CONFIGURATION_FIELD_PREFIX_STRING);
    icebergCatalog = storageConfig.icebergCatalog();
    tableFullName = storageConfig.tableFullName();
    tableId = storageConfig.tableIdentifier();
    engineName = config.originalsStrings().get("name");
  }

  @Override
  public synchronized void start() {
    super.start();
    LOG.info("Starting IcebergOffsetBackingStore table:{}", tableFullName);
    if (storageConfig.atomicOffsetCommit() && Strings.isNullOrEmpty(engineName)) {
      throw new DebeziumException(
          "Engine name not found in offset storage configuration, required by atomic offset commit!");
    }
    initializeTable();
    load();
    instance = this;
  }

  @Override
  public synchronized void stop() {
    if (executor != null) {
      shutdownExecutorServiceQuietly(executor, 30, TimeUnit.SECONDS);
      executor = null;
    }
    if (instance == this) {
      instance = null;
    }
    LOG.info("Stopped IcebergOffsetBackingStore table:{}", tableFullName);
  }

  /**
   * Shuts down an executor service in two phases, first by calling shutdown to reject incoming
   * tasks, and then calling shutdownNow, if necessary, to cancel any lingering tasks. After the
   * timeout/on interrupt, the service is forcefully closed.
   *
   * @param executorService The service to shut down.
   * @param timeout The timeout of the shutdown.
   * @param timeUnit The time unit of the shutdown timeout.
   */
  public static void shutdownExecutorServiceQuietly(
      ExecutorService executorService, long timeout, TimeUnit timeUnit) {
    executorService.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!executorService.awaitTermination(timeout, timeUnit)) {
        executorService.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!executorService.awaitTermination(timeout, timeUnit)) {
          LOG.error("Executor {} did not terminate in time", executorService);
        }
      }
    } catch (InterruptedException e) {
      // (Re-)Cancel if current thread also interrupted
      executorService.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  private void initializeTable() {
    Optional<Table> table = IcebergUtil.loadIcebergTable(icebergCatalog, tableId);
    if (table.isPresent()) {
      offsetTable = table.get();
    } else {
      LOG.debug("Creating table {} to store offset", tableFullName);
      try {
        offsetTable =
            IcebergUtil.createIcebergTable(icebergCatalog, tableId, OFFSET_STORAGE_TABLE_SCHEMA);
      } catch (AlreadyExistsException e) {
        LOG.warn("Table {} was created concurrently, loading existing table", tableFullName);
        offsetTable = icebergCatalog.loadTable(tableId);
      }

      if (!Strings.isNullOrEmpty(storageConfig.getMigrateOffsetFile().strip())) {
        LOG.warn("Migrating offset from file {}", storageConfig.getMigrateOffsetFile());
        this.loadFileOffset(new File(storageConfig.getMigrateOffsetFile()));
      }
    }

    format = IcebergUtil.getTableFileFormat(offsetTable);
    fileWriterFactory = IcebergUtil.getTableWriterFactory(offsetTable);
    fileFactory = IcebergUtil.getTableOutputFileFactory(offsetTable, format);
  }

  private void loadFileOffset(File file) {
    try (SafeObjectInputStream is =
        new SafeObjectInputStream(Files.newInputStream(file.toPath()))) {
      Object obj = is.readObject();

      if (!(obj instanceof HashMap))
        throw new ConnectException("Expected HashMap but found " + obj.getClass());

      @SuppressWarnings("unchecked")
      Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
      for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
        ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
        ByteBuffer value =
            (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
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
    replaceOffsetRow(offsetTable.newOverwrite(), writeOffsetFile(data)).commit();
    persistedData = new HashMap<>(data);
    LOG.debug("Successfully saved offset data to iceberg table");
  }

  // the offset table holds a single row, replaced on each save
  private static OverwriteFiles replaceOffsetRow(OverwriteFiles overwrite, WriteResult files) {
    overwrite.overwriteByRowFilter(Expressions.alwaysTrue());
    Arrays.stream(files.dataFiles()).forEach(overwrite::addFile);
    return overwrite;
  }

  /**
   * Writes the offset data as a single row data file, without committing it to the offset table.
   */
  private WriteResult writeOffsetFile(Map<String, String> offsetData) {
    try {
      String dataJson = mapper.writeValueAsString(offsetData);
      LOG.debug("Saving offset data {}", dataJson);
      OffsetDateTime currentTs = OffsetDateTime.now(ZoneOffset.UTC);

      GenericRecord record = GenericRecord.create(OFFSET_STORAGE_TABLE_SCHEMA);
      Record row =
          record.copy(
              "id", UUID.randomUUID().toString(),
              "offset_data", dataJson,
              "record_insert_ts", currentTs);

      try (BaseTaskWriter<Record> writer =
          new UnpartitionedAppendWriter(
              offsetTable.spec(),
              format,
              fileWriterFactory,
              fileFactory,
              offsetTable.io(),
              Long.MAX_VALUE)) {
        writer.write(row);
        writer.close();
        return writer.complete();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void load() {
    try {
      String dataJsonString = null;

      int rowNum = 0;
      try (CloseableIterable<Record> rs = IcebergGenerics.read(offsetTable).build()) {
        for (Record row : rs) {
          dataJsonString = (String) row.getField("offset_data");
          rowNum++;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (rowNum > 1) {
        throw new DebeziumException(
            "Failed recover offset data from iceberg, Found multiple offset row!");
      }

      if (dataJsonString != null) {
        this.data = mapper.readValue(dataJsonString, new TypeReference<>() {});
        LOG.debug("Loaded offset data {}", dataJsonString);
      }
      this.persistedData = new HashMap<>(data);
    } catch (JsonProcessingException e) {
      //      e.printStackTrace();
      throw new DebeziumException("Failed recover offset data from iceberg", e);
    }
  }

  @Override
  public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
    return executor.submit(
        () -> {
          try {
            update(values);
            if (callback != null) {
              callback.onCompletion(null, null);
            }
          } catch (Throwable e) {
            LOG.error("Failed to save offset data to iceberg table", e);
            if (callback != null) {
              callback.onCompletion(e, null);
            }
            throw e;
          }
          return null;
        });
  }

  private synchronized void update(Map<ByteBuffer, ByteBuffer> values) {
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
    }
    // skip when unchanged, e.g. already committed with the data by the consumer
    if (data.equals(persistedData)) {
      LOG.debug("Offset data unchanged, skipping save");
      return;
    }
    save();
  }

  @Override
  public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
    return executor.submit(
        () -> {
          Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
          for (ByteBuffer key : keys) {
            result.put(key, toByteBuffer(offsetValue(fromByteBuffer(key))));
          }
          return result;
        });
  }

  private synchronized String offsetValue(String key) {
    return data.get(key);
  }

  public static String fromByteBuffer(ByteBuffer data) {
    return (data != null)
        ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer()))
        : null;
  }

  public static ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)) : null;
  }

  public Set<Map<String, Object>> connectorPartitions(String connectorName) {
    return null;
  }

  /**
   * @return the started offset store, empty when Debezium uses another offset storage
   */
  public static Optional<IcebergOffsetBackingStore> instance() {
    return Optional.ofNullable(instance);
  }

  public TableIdentifier tableIdentifier() {
    return tableId;
  }

  /**
   * @return uri of the catalog holding the offset table, null for non REST catalogs
   */
  public String catalogUri() {
    return storageConfig.icebergProperties().get(CatalogProperties.URI);
  }

  /**
   * Prepares the offset table update for a batch of records, without committing it.
   *
   * <p>The offsets are serialized with the same {@link OffsetStorageWriter} and converters the
   * Debezium engine uses, so once committed the engine finds them on restart, and its own flush of
   * the same offsets is a no-op.
   *
   * @param records the records of the batch, in source order
   * @return the merged offset data and the data file holding it
   */
  public synchronized PendingOffset prepare(List<SourceRecord> records) {
    Map<String, String> merged = new HashMap<>(data);
    serializeOffsets(records)
        .forEach(
            (key, value) -> {
              if (key != null) {
                merged.put(fromByteBuffer(key), fromByteBuffer(value));
              }
            });
    return new PendingOffset(merged, writeOffsetFile(merged));
  }

  /** Marks the prepared offsets as committed to the offset table. */
  public synchronized void committed(PendingOffset pendingOffset) {
    data = new HashMap<>(pendingOffset.data());
    persistedData = new HashMap<>(pendingOffset.data());
    offsetTable.refresh();
  }

  /** Deletes the data file of offsets that were not committed. */
  public void abort(PendingOffset pendingOffset) {
    for (DataFile file : pendingOffset.files().dataFiles()) {
      try {
        offsetTable.io().deleteFile(file.location());
      } catch (Exception e) {
        LOG.warn("Failed to delete uncommitted offset file {}", file.location(), e);
      }
    }
  }

  private Map<ByteBuffer, ByteBuffer> serializeOffsets(List<SourceRecord> records) {
    Map<ByteBuffer, ByteBuffer> serialized = new HashMap<>();
    OffsetBackingStore capturingStore =
        new MemoryOffsetBackingStore() {
          @Override
          public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            serialized.putAll(values);
            if (callback != null) {
              callback.onCompletion(null, null);
            }
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public Set<Map<String, Object>> connectorPartitions(String connectorName) {
            return Set.of();
          }
        };
    OffsetStorageWriter writer =
        new OffsetStorageWriter(
            capturingStore,
            engineName,
            KafkaOffsetStoreConverter.jsonConverter(true),
            KafkaOffsetStoreConverter.jsonConverter(false));
    for (SourceRecord record : records) {
      writer.offset(record.sourcePartition(), record.sourceOffset());
    }
    if (writer.beginFlush()) {
      try {
        writer.doFlush(null).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DebeziumException("Interrupted while serializing offsets", e);
      } catch (ExecutionException e) {
        throw new DebeziumException("Failed to serialize offsets", e.getCause());
      }
    }
    return serialized;
  }

  /**
   * Offset data prepared for a batch, committed to the offset table in the same transaction as the
   * data.
   *
   * @param data the offset data, including the offsets of the batch
   * @param files the data file holding the offset data row
   */
  public record PendingOffset(Map<String, String> data, WriteResult files) {

    /** Replaces the offset table row in the transaction. */
    public void applyTo(Transaction tx) {
      replaceOffsetRow(tx.newOverwrite(), files).commit();
    }
  }
}
