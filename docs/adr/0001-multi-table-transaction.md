# ADR-0001: Multi-Table Batch Transaction

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

3. **4-Step Batch Lifecycle**:
   Every batch in `IcebergChangeConsumer.handle()` executes a unified 4-step flow through a `BatchTransaction` abstraction:
   1. **Open transaction** (`try (BatchTransaction tx = openTransaction())`)
   2. **Consume batch & upload to tables** (supporting both **parallel** and **sequential** table processing)
   3. **Commit the multi-table transaction** (`tx.commit()` — executes a single atomic `RESTCatalog.commitTransaction(List<TableCommit>)` across all modified destination tables)
   4. **Commit the offset** (`record.commit()` on each `BatchEvent`, allowing Debezium's `RecordCommitter.markBatchFinished()` to persist the offset only after all tables in the batch are atomically committed)

---

## Architecture & Sequence Flow

```mermaid
sequenceDiagram
    participant DBZ as Debezium Engine
    participant ICC as IcebergChangeConsumer
    participant TX as BatchTransaction
    participant ITO as IcebergTableOperator
    participant CAT as RESTCatalog

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
    ICC->>DBZ: 4. record.commit() for all BatchEvents
    ICC-->>DBZ: return -> committer.markBatchFinished() flushes offset
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
  - `void commit()`
- **`NoOpBatchTransaction`** (used when `multi-table-transaction=false`):
  - `enlist` returns `icebergTable` directly (operations commit immediately per table).
  - `commit` is a no-op.
- **`MultiTableTransaction`** (used when `multi-table-transaction=true`):
  - `enlist` creates or retrieves `table.newTransaction()` in a thread-safe `ConcurrentHashMap` and returns `tx.table()`.
  - `commit` builds `TableCommit.create(tableId, baseTx.startMetadata(), baseTx.currentMetadata())` for every modified table, calls `restCatalog.commitTransaction(tableCommits)`, and invalidates committed tables in the catalog cache.

---

## Failure Modes & Guarantees

| Failure Point | Data Tables | Debezium Offset & Source DB Replication Slot | Recovery Behavior |
| :--- | :--- | :--- | :--- |
| **Crash / Exception during Step 2 (Table upload)** | Uncommitted (`BaseTransaction` never sent to catalog) | Uncommitted (`handle()` throws before `record.commit()`) | Debezium replays the batch from previous offset; zero duplicate rows. |
| **Crash / Exception during Step 3 (`RESTCatalog.commitTransaction`)** | Atomically rejected by catalog | Uncommitted (`handle()` throws before `record.commit()`) | Debezium replays the batch from previous offset; zero duplicate rows. |
