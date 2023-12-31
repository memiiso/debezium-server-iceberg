# Debezium Iceberg Consumers

Replicates database CDC events to Iceberg tables(Cloud storage, hdfs) without using Spark, Kafka or Streaming platform
in between.

![Debezium Iceberg](images/debezium-iceberg.png)

## `iceberg` Consumer

Iceberg consumer replicates database CDC events to destination Iceberg tables. It is possible to replicate source
data with upsert or append modes.
When event and key schema enabled (`debezium.format.value.schemas.enable=true`
, `debezium.format.key.schemas.enable=true`) destination Iceberg
tables created automatically with the first start.

#### Configuration properties

| Config                                             | Default           | Description                                                                                                      |
|----------------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------|
| `debezium.sink.iceberg.warehouse`                  |                   | Root path of the Iceberg data warehouse                                                                          |
| `debezium.sink.iceberg.catalog-name`               | `default`         | User-specified Iceberg catalog name.                                                                             |
| `debezium.sink.iceberg.table-namespace`            | `default`         | A namespace in the catalog. ex: `SELECT * FROM prod.db.table -- catalog: prod, namespace: db, table: table`      |
| `debezium.sink.iceberg.table-prefix`               | ``                | Iceberg table name prefix, Added to destination iceberg table names.                                             |
| `debezium.sink.iceberg.write.format.default`       | `parquet`         | Default file format for the table; `parquet`, `avro`, or `orc`                                                   |
| `debezium.sink.iceberg.allow-field-addition`       | `true`            | Allow field addition to target tables                                                                            |
| `debezium.sink.iceberg.upsert`                     | `true`            | Running upsert mode overwriting updated rows. explained below.                                                   |
| `debezium.sink.iceberg.upsert-keep-deletes`        | `true`            | When running with upsert mode, keeps deleted rows in target table.                                               |
| `debezium.sink.iceberg.upsert-dedup-column`        | `__source_ts_ms`  | With upsert mode used to deduplicate data. row with highest `__source_ts_ms` kept. _                             
 dont change!_                                      |
| `debezium.sink.iceberg.upsert-op-column`           | `__op`            | Used with upsert mode. _dont change!_                                                                            |
| `debezium.sink.iceberg.destination-regexp`         | ``                | Regexp to modify destination table. With this its possible to map `table_ptt1`,`table_ptt2` to `table_combined`. |
| `debezium.sink.iceberg.destination-regexp-replace` | ``                | Regexp Replace part to modify destination table                                                                  |
| `debezium.sink.batch.batch-size-wait`              | `NoBatchSizeWait` | Batch size wait strategy to optimize data files and upload interval. explained below.                            |
| `debezium.sink.iceberg.{iceberg.prop.name}`        |                   | [Iceberg config](https://iceberg.apache.org/docs/latest/configuration/) passed to Iceberg                        |

### Upsert

By default, Iceberg consumer is running with upsert mode `debezium.sink.iceberg.upsert=true`.
Upsert mode uses source Primary Key and does upsert on target table(delete followed by insert). For the tables without
Primary Key consumer falls back to append mode.

#### Upsert Mode Data Deduplication

With upsert mode data deduplication is done. Deduplication is done based on `__source_ts_ms` value and event type `__op`
.
its is possible to change this field using `debezium.sink.iceberg.upsert-dedup-column=__source_ts_ms` (Currently only
Long field type supported.)

Operation type priorities are `{"c":1, "r":2, "u":3, "d":4}`. When two records with same key and same `__source_ts_ms`
values received then the record with higher `__op` priority is kept and added to destination table and duplicate record
is dropped from the batch.

### Append

Setting `debezium.sink.iceberg.upsert=false` will set the operation mode to append. With append mode data deduplication
is not done and all received records are appended to destination table.
Note: For the tables without primary key operation mode falls back to append even upsert mode is used.

#### Keeping Deleted Records

By default `debezium.sink.iceberg.upsert-keep-deletes=true` keeps deletes in the Iceberg table, setting it to false
will remove deleted records from the destination Iceberg table too. With this config it's possible to keep last version
of a
deleted record in the destination Iceberg table(doing soft delete for this records `__deleted` is set to `true`).

### Optimizing batch size (or commit interval)

Debezium extracts database events in real time and this could cause too frequent commits and too many small files. Which
is not optimal for batch processing especially when near realtime data feed is sufficient.
To avoid this problem following batch-size-wait classes are available to adjust batch size and interval.

Batch size wait adds delay between consumer calls to increase total number of events consumed per call. Meanwhile,
events are collected in memory.
This setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size`
debezium properties

#### NoBatchSizeWait

This is default configuration by default consumer will not use any wait. All the events are consumed immediately.

#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size.
MaxBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size`.
Maximum wait and check intervals are controlled
by `debezium.sink.batch.batch-size-wait.max-wait-ms`, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties.

example setup to receive 2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size checked
every 5 seconds

```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048;
debezium.source.max.queue.size=16000";
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

### Table Name Mapping

Iceberg tables are named by following rule : `table-namespace`.`table-prefix``database.server.name`_`database`_`table`

For example:

```properties
debezium.sink.iceberg.table-namespace=default
database.server.name=testc
debezium.sink.iceberg.table-prefix=cdc_
```

With above config database table = `inventory.customers` is replicated to `default.testc_cdc_inventory_customers`

## IcebergOffsetBackingStore Offset Storage

This implementation saves CDC offset to an iceberg table. Debezium keeps source offset to track binlog position.

```
debezium.source.offset.storage=io.debezium.server.iceberg.offset.IcebergOffsetBackingStore
debezium.source.offset.storage.iceberg.table-name=debezium_offset_storage_table
```

## IcebergSchemaHistory Database History Storage

This implementation saves database history to an iceberg table.

```properties
debezium.source.database.history=io.debezium.server.iceberg.history.IcebergSchemaHistory
debezium.source.database.history.iceberg.table-name=debezium_database_history_storage_table
```

## Debezium Event Flattening

Iceberg consumer requires event flattening. This is required configuration.

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
```

### Configuring iceberg

All the properties starting with `debezium.sink.iceberg.__ICEBERG_CONFIG__` are passed to Iceberg, and to hadoopConf

```properties
debezium.sink.iceberg.{iceberg.prop.name}=xyz-value # passed to iceberg!
```

### Example Configuration

Read [application.properties.example](..%2Fdebezium-server-iceberg-dist%2Fsrc%2Fmain%2Fresources%2Fdistro%2Fconf%2Fapplication.properties.example)

## Schema Change Behaviour

It is possible to get out of sync schemas between source and target tables. For Example when the source database change
its schema, adds or drops field. Below possible schema changes and current behavior of the Iceberg consumer id
documented.

**NOTE**: Full schema evaluation is not supported. But sema expansion like field addition is supported,
see `debezium.sink.iceberg.allow-field-addition` setting.

#### Adding new column to source (A column missing in destination iceberg table)

When `debezium.sink.iceberg.allow-field-addition` is `false` data of the new column is ignored till the column added to
destination iceberg table.

For example: if a column not found in iceberg table its data ignored and not copied to target!
once iceberg table adds same column then data for this column recognized and populated for the new data.

#### Removing column from source (An extra column in iceberg table)

These column values are populated with null value for the new data.

#### Renaming column in source

This is combination of above two cases : old column will be populated with null values and new column will not be
recognized and populated till it's added to iceberg table

#### Different Data Types

This is the scenario when source field type and target Iceberg field type are different. In this case consumer converts
source field value to destination type value. Conversion is done by jackson If representation cannot be converted to
destination type then default value is returned!

for example this is conversion rule for Long type:

```Method that will try to convert value of this node to a Java long. Numbers are coerced using default Java rules; booleans convert to 0 (false) and 1 (true), and Strings are parsed using default Java language integer parsing rules.
If representation cannot be converted to a long (including structured types like Objects and Arrays), default value of 0 will be returned; no exceptions are thrown.
```

## `icebergevents` Consumer

This consumer appends CDC events to single Iceberg table as json string.
This table partitioned by `event_destination,event_sink_timestamptz`

````properties
debezium.sink.type=icebergevents
debezium.sink.iceberg.catalog-name=default
````

Destination table definition:

```java
static final String TABLE_NAME = "debezium_events";
static final Schema TABLE_SCHEMA = new Schema(
        required(1, "event_destination", Types.StringType.get()),
        optional(2, "event_key", Types.StringType.get()),
        optional(3, "event_value", Types.StringType.get()),
        optional(4, "event_sink_epoch_ms", Types.LongType.get()),
        optional(5, "event_sink_timestamptz", Types.TimestampType.withZone())
);
static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA)
        .identity("event_destination")
        .hour("event_sink_timestamptz")
        .build();
static final SortOrder TABLE_SORT_ORDER = SortOrder.builderFor(TABLE_SCHEMA)
        .asc("event_sink_epoch_ms", NullOrder.NULLS_LAST)
        .build();
```
