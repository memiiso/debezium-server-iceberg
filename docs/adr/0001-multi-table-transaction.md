# ADR-0001: Multi-Table Batch Transaction & Synchronous Offset Commit

* **Status**: Proposed / Draft
* **Date**: 2026-10-27
* **Issues**: [#773](https://github.com/memiiso/debezium-server-iceberg/issues/773), [#761](https://github.com/memiiso/debezium-server-iceberg/issues/761)

---

## Context

In `IcebergChangeConsumer`, a single Debezium CDC batch often spans multiple source tables (or multiple topics mapped to the same destination table). Previously:

1. **Per-Table Independent Commits**: Each destination table was committed to the Iceberg catalog independently via `icebergTable.newAppend().commit()` / `icebergTable.newRowDelta().commit()` (and `icebergTable.updateSchema().commit()`).
2. **Global Source Offset vs. Partial Table Commits**: Unlike Kafka (where offsets are tracked per topic-partition), a relational database CDC stream has a **single global stream offset** (e.g., PostgreSQL LSN, MySQL binlog coordinates, Oracle SCN). If a batch updates 5 tables and fails on the 3rd table, tables 1 and 2 are already committed in Iceberg while the batch offset is not committed—leading to duplicate records on retry in append mode or cross-table inconsistency.
3. **Multiple Topics Mapped to the Same Table ([#761](https://github.com/memiiso/debezium-server-iceberg/issues/761))**: Grouping batch events by raw `EventConverter::destination` (topic name) before applying `mapDestination()` caused multiple commits to the same Iceberg table within a single batch (and concurrent conflicting commits when `concurrent-uploads > 1`).

---

## Decision

1. **Group Batch Events by Mapped `TableIdentifier` ([#761](https://github.com/memiiso/debezium-server-iceberg/issues/761))**:
   Filter skipped heartbeat events upfront and group batch events by `TableIdentifier` (`mapDestination(e.destination())`) so every destination table is written once per batch.

2. **Opt-In Multi-Table Transaction (`debezium.sink.iceberg.multi-table-transaction`)**:
   Introduce an opt-in configuration property `debezium.sink.iceberg.multi-table-transaction=true|false` (default `false`):
   - When `false` (default), the existing per-table commit behavior is preserved without changes.
   - When `true`, `IcebergChangeConsumer` verifies once during `@PostConstruct` initialization (`validateMultiTableTransactionSupport()`) that the configured catalog supports multi-table transactions (`RESTCatalog`), failing fast otherwise.

3. **4-Step Batch Lifecycle Synchronized with Debezium 3.7.0 Offset Commit**:
   Every batch in `IcebergChangeConsumer.handle()` executes a unified 4-step flow through a `BatchTransaction` abstraction:
   1. **Open transaction** (`try (BatchTransaction tx = openTransaction())`)
   2. **Consume batch & upload to tables** (supporting both **parallel** and **sequential** table processing)
   3. **Commit the multi-table transaction** (`tx.commit()` — executes a single atomic `RESTCatalog.commitTransaction(List<TableCommit>)` across all modified destination tables)
   4. **Commit the offset** (`record.commit()` on each `BatchEvent`, followed immediately by `debezium-quarkus` invoking `committer.markBatchFinished()`)

---

## Debezium 3.7.0 Offset Commit Synchronization

In Debezium 3.7.0 (`debezium-quarkus` and `debezium-embedded`):

1. **In-Memory Record Marking (`BatchEvent.commit()`)**:
   `CapturingEvents<BatchEvent>` provides `records()`, `destination()`, `source()`, and `engine()`. Calling `record.commit()` on each `BatchEvent` invokes `RecordCommitter.markProcessed(record)`, which buffers `(sourcePartition, sourceOffset)` in memory inside Kafka Connect's `OffsetStorageWriter` and increments `recordsSinceLastCommit`.
2. **Automatic Batch Finish (`committer.markBatchFinished()`)**:
   In `io.quarkus.debezium.engine.capture.consumer.DefaultChangeConsumerFactory.RoutedQuarkusChangeConsumer`, `committer.markBatchFinished()` is called **automatically right after `IcebergChangeConsumer.handle(events)` returns**.
3. **Synchronous Flush (`AsyncEmbeddedEngine.commitOffsets()`)**:
   Inside `AsyncEmbeddedEngine.markBatchFinished()`, when `OffsetCommitPolicy.performCommit(...)` returns `true`, `commitOffsets()` calls `offsetWriter.doFlush(...)` and **blocks synchronously on `flushFuture.get(commitTimeout, TimeUnit.MILLISECONDS)`** followed by `task.commit()` before polling the next batch.
4. **Enabling Synchronous Per-Batch Offset Flush**:
   By default, Debezium uses `PeriodicCommitOffsetPolicy` (`debezium.source.offset.flush.interval.ms=60000`). To flush the offset **synchronously at the end of every batch** immediately after `MultiTableTransaction.commit()`, configure either:
   ```properties
   debezium.source.offset.flush.interval.ms=0
   # or
   debezium.source.offset.commit.policy=io.debezium.engine.spi.OffsetCommitPolicy$AlwaysCommitOffsetPolicy
   ```
   When `debezium.sink.iceberg.multi-table-transaction=true` and neither property is set, `IcebergChangeConsumer` logs a startup warning recommending synchronous per-batch offset flushing.

---

## Architecture & Sequence Flow

```mermaid
sequenceDiagram
    participant DBZ as Debezium Engine (Quarkus)
    participant ICC as IcebergChangeConsumer
    participant TX as BatchTransaction
    participant ITO as IcebergTableOperator
    participant CAT as RESTCatalog
    participant OBS as OffsetBackingStore

    DBZ->>ICC: handle(CapturingEvents<BatchEvent>)
    Note over ICC: Filter heartbeats & group events<br/>by mapped TableIdentifier (#761)
    ICC->>TX: 1. openTransaction()
    par 2. Parallel or Sequential per TableIdentifier
        ICC->>TX: tx.enlist(tableId, icebergTable)
        TX-->>ICC: TransactionTable (table.newTransaction().table())
        ICC->>ITO: addToTable(TransactionTable, tableEvents)
        Note over ITO: Uploads Parquet/ORC & manifest files;<br/>stages schema + data ops in BaseTransaction
    end
    ICC->>TX: 3. tx.commit()
    TX->>CAT: commitTransaction(List<TableCommit>)
    Note over CAT: Single atomic POST /v1/transactions/commit<br/>across all modified data tables
    ICC->>DBZ: 4. record.commit() for all BatchEvents (buffers in OffsetStorageWriter)
    ICC-->>DBZ: return from handle()
    DBZ->>OBS: 5. committer.markBatchFinished() -> flushFuture.get() (synchronous when flush.interval.ms=0 / AlwaysCommitOffsetPolicy)
```

---

## Modular Component Design

### 1. Zero Write-Path Intrusion via `BaseTransaction.TransactionTable`

In Apache Iceberg, `Transaction tx = table.newTransaction()` returns a `BaseTransaction`, and `tx.table()` returns a `BaseTransaction.TransactionTable` implementing `org.apache.iceberg.Table`.

Passing `tx.table()` to `IcebergTableOperator.addToTable(Table icebergTable, List<EventConverter> events)` requires no changes to `IcebergTableOperator`'s data writing logic:
- `icebergTable.updateSchema()...commit()` stages schema changes in memory inside `BaseTransaction` (`currentMetadata()`), and subsequent `icebergTable.schema()` calls within the same batch immediately reflect added columns.
- `icebergTable.newAppend()...commit()` and `icebergTable.newRowDelta()...commit()` upload data files and manifest files to object storage immediately and update `BaseTransaction.currentMetadata()` in memory without contacting the catalog.
- Because each `TableIdentifier` has its own isolated `Transaction` instance in `MultiTableTransaction`, parallel table uploads (`concurrent-uploads > 1`) execute concurrently without lock contention.

### 2. `BatchTransaction` Hierarchy

- **`BatchTransaction`** (`AutoCloseable`):
  - `Table enlist(TableIdentifier tableId, Table icebergTable)`
  - `void onCommit(Runnable action)`: post commit actions (commit log, OpenLineage event), only run once the data is committed.
  - `void commit()`
  - `void close()`: deletes the files written for the batch when it was not committed.
- **`NoOpBatchTransaction`** (used when `multi-table-transaction=false`):
  - `enlist` returns `icebergTable` directly (operations commit immediately per table).
  - `onCommit` runs the action right away, the table is already committed.
  - `commit` is a no-op.
- **`MultiTableTransaction`** (used when `multi-table-transaction=true`):
  - `enlist` creates or retrieves `table.newTransaction()` in a thread-safe `ConcurrentHashMap` and returns `tx.table()`.
  - `commit` builds `TableCommit.create(tableId, baseTx.startMetadata(), baseTx.currentMetadata())` for every modified table, calls `restCatalog.commitTransaction(tableCommits)`, and invalidates committed tables in the catalog cache.
  - On `CommitFailedException` (e.g. a concurrent compaction), each table is reloaded and the staged changes are re-applied on a new transaction: the staged schema change (only when the table schema did not change concurrently, so the field ids of the written files stay valid), then the data and delete files added by each staged snapshot. No data file is rewritten. Up to `multi-table-transaction-retries` retries.
  - `close` deletes the data, delete, manifest and manifest list files of every attempt when the transaction is not committed, unless a commit request failed with an unknown outcome (e.g. `CommitStateUnknownException`), then the files are kept.
- **`AtomicOffsetTransaction`** (used when `atomic-offset-commit=true`), extends `MultiTableTransaction`:
  - adds the offset table of `IcebergOffsetBackingStore` to the same `commitTransaction` call, see [Exactly-once](#exactly-once-atomic-offset-commit).

---

## Failure Modes & Guarantees

| Failure Point | Data Tables | Debezium Offset & Source DB Replication Slot | Recovery Behavior |
| :--- | :--- | :--- | :--- |
| **Crash / Exception during Step 2 (Table upload)** | Uncommitted (`BaseTransaction` never sent to catalog) | Uncommitted (`handle()` throws before `record.commit()` and `markBatchFinished()`) | Debezium replays the batch from previous offset; zero duplicate rows. |
| **Crash / Exception during Step 3 (`RESTCatalog.commitTransaction`)** | Atomically rejected by catalog | Uncommitted (`handle()` throws before `record.commit()` and `markBatchFinished()`) | Debezium replays the batch from previous offset; zero duplicate rows. |
| **Step 3 succeeds -> Step 4 & 5 (`record.commit()` + synchronous `markBatchFinished()`)** | All batch tables committed atomically in a single catalog transaction | Flushed synchronously via `flushFuture.get()` before polling the next batch (when `offset.flush.interval.ms=0` or `AlwaysCommitOffsetPolicy`) | Batch data and source offset are persisted before the next batch begins. |
| **Crash after Step 3, before the offset flush of Step 5** | Committed | Not flushed, two separate writes | Debezium replays the batch: upsert mode absorbs it, append mode duplicates it (at-least-once). Closed by `atomic-offset-commit`. |

---

## Exactly-once (atomic offset commit)

`multi-table-transaction` alone is at-least-once: the data commit and the offset flush are two writes, a crash between them replays the batch. A synchronous offset flush makes the window shorter, it cannot remove it.

With `debezium.sink.iceberg.atomic-offset-commit=true`, `AtomicOffsetTransaction` commits the offset of the batch in the same `commitTransaction` call as the data tables:

1. `IcebergOffsetBackingStore.prepare(records)` serializes the offsets of all the batch records (heartbeats included) with the same `OffsetStorageWriter` and converters as the Debezium engine, merges them with the stored offsets and writes the offset row data file, without committing it.
2. The offset table replacement (`overwriteByRowFilter(alwaysTrue)` + the new file) is added as one more `TableCommit` of the transaction. On retry, the offset table is reloaded and the replacement re-applied.
3. Once committed, the store marks the offsets as persisted. The engine flush that follows (`markBatchFinished()`) finds the same offsets and skips the write, a concurrent engine write to the offset table is handled as a commit conflict.

Requirements, checked at startup or on the first batch:

- the catalog is a `RESTCatalog`,
- `debezium.source.offset.storage` is `IcebergOffsetBackingStore`, with the same catalog `uri` as the sink.

The engine creates the offset store by reflection from its class name, not through CDI, so the started instance registers itself in a static field (`IcebergOffsetBackingStore.instance()`) for the consumer.

| Failure Point | Data Tables | Offset | Recovery Behavior |
| :--- | :--- | :--- | :--- |
| **Before or during `commitTransaction`** | Not committed | Not committed | Batch replayed from the previous offset, no duplicates. |
| **After `commitTransaction`** | Committed | Committed in the same transaction | Restart from the batch offset, nothing replayed. |
| **`commitTransaction` outcome unknown** | Committed or not, together with the offset | Same as the data | Files kept; on restart the stored offset tells whether the batch is replayed. |
