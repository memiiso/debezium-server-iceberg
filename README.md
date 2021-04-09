![Java CI with Maven](https://github.com/memiiso/debezium-server-iceberg/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Iceberg Consumers

-----
This project adds iceberg batch consumers
to [debezium server](https://debezium.io/documentation/reference/operations/debezium-server.html). it could be used to
replicate database changes to iceberg table, without requiring Spark, Kafka or Streaming platform.

## `iceberg` Consumer

Appends json events to destination iceberg tables. Destination tables are created automatically if event and key schemas
enabled `debezium.format.value.schemas.enable=true`, `debezium.format.key.schemas.enable=true`
when destination table is not exists Consumer will print a warning message and continue replication of other tables

### Upsert

By default `debezium.sink.iceberg.upsert=true` upsert feature is enabled, for tables with Primary Key definition it will
do upsert, for the tables without Primary Key it falls back to append mode

Setting `debezium.sink.iceberg.upsert=false` will change insert mode to append.

#### Data Deduplication

when iceberg consumer is doing upsert it does data deduplication for the batch, deduplication is done based
on `__source_ts_ms` field and event type `__op`
its is possible to change field using `debezium.sink.iceberg.upsert-source-ts-ms-column=__source_ts_ms`, Currently only
Long field type supported

operation type priorities are `{"c":1, "r":2, "u":3, "d":4}` when two record with same Key having same `__source_ts_ms`
values then the record with higher `__op` priority is kept

#### Keeping Deleted Records

By default `debezium.sink.iceberg.upsert-keep-deletes=true` will keep deletes in the iceberg table, setting it to false
will remove deleted records from the iceberg table too. with this feature its possible to keep last version of the
deleted record.

### Iceberg Table Names

iceberg table names are created by following rule : `table-namespace`
.`table-prefix``database.server.name`_`database`_`table`

For example

```properties
debezium.sink.iceberg.table-namespace = default
database.server.name = testc
debezium.sink.iceberg.table-prefix=cdc_
```

database table = `inventory.customers` will be replicated to `default.testc_cdc_inventory_customers`

### Example Configuration

```properties
debezium.sink.type = iceberg
debezium.sink.iceberg.table-prefix = debeziumcdc_
debezium.sink.iceberg.table-namespace = default
debezium.sink.iceberg.catalog-name = default
debezium.sink.iceberg.fs.defaultFS = s3a://MY_S3_BUCKET
debezium.sink.iceberg.warehouse = s3a://MY_S3_BUCKET/iceberg_warehouse
debezium.sink.iceberg.user.timezone = UTC
debezium.sink.iceberg.com.amazonaws.services.s3.enableV4 = true
debezium.sink.iceberg.com.amazonaws.services.s3a.enableV4 = true
debezium.sink.iceberg.fs.s3a.aws.credentials.provider = com.amazonaws.auth.DefaultAWSCredentialsProviderChain
debezium.sink.iceberg.fs.s3a.access.key = S3_ACCESS_KEY
debezium.sink.iceberg.fs.s3a.secret.key = S3_SECRET_KEY
debezium.sink.iceberg.fs.s3a.path.style.access = true
debezium.sink.iceberg.fs.s3a.endpoint = http://localhost:9000 # minio specific setting
debezium.sink.iceberg.fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem
debezium.sink.iceberg.dynamic-wait=true
debezium.sink.batch.dynamic-wait.max-wait-ms=300000
debezium.sink.iceberg.upsert=true
debezium.sink.iceberg.upsert-keep-deletes=true
debezium.sink.iceberg.upsert-op-column=__op
debezium.sink.iceberg.upsert-source-ts-ms-column=__source_ts_ms
debezium.format.value.schemas.enable=true
debezium.format.key.schemas.enable=true
```

All the properties starting with `debezium.sink.iceberg.**` are passed to iceberg, and hadoopConf

```properties
debezium.sink.iceberg.{iceberg.prop.name} = xyz-value # passed to iceberg!
```

## `icebergevents` Consumer

This consumer appends CDC events to single iceberg table as json string. This table is partitioned
by `event_destination,event_sink_timestamptz` and sorted by `event_sink_epoch_ms`

#### Example Configuration

````properties
debezium.sink.type = icebergevents
debezium.sink.iceberg.catalog-name = default
debezium.sink.iceberg.dynamic-wait=true
debezium.sink.batch.dynamic-wait.max-wait-ms=300000
````

Iceberg table definition:

```java
static final String TABLE_NAME="debezium_events";
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

## Debezium Event Flattening

Iceberg consumer requires event flattening, Currently nested events and complex data types(like maps) are not supported

```properties
debezium.transforms = unwrap
debezium.transforms.unwrap.type = io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields = op,table,lsn,source.ts_ms
debezium.transforms.unwrap.add.headers = db
debezium.transforms.unwrap.delete.handling.mode = rewrite
```

## Controlling Batch Size

`max.batch.size` Positive integer value that specifies the maximum size of each batch of events that should be processed
during each iteration of this connector. Defaults to 2048.

`debezium.sink.batch.dynamic-wait.max-wait-ms` Positive integer value that specifies the maximum number of milliseconds
dynamic wait could add delay to increase batch size. dynamic wait is calculated based on number of processed events in
last 3 batches. if last batch sizes are lower than `max.batch.size` max-wait-ms will increase and if last batch sizes
are bigger than 90% of `max.batch.size` max-wait-ms will decrease

it tries to keep batch size between 85%-90% of the `max.batch.size`, it does not guarantee consistent batch size.

Change `debezium.source.max.batch.size` and `debezium.sink.batch.dynamic-wait.max-wait-ms` if you want to have less
frequent commits with larger batch size

```properties
# Positive integer value that specifies the maximum size of each batch of events that should be processed during each iteration of this connector. Defaults to 2048.
debezium.source.max.batch.size = 2
debezium.sink.iceberg.dynamic-wait=true
debezium.sink.batch.dynamic-wait.max-wait-ms=300000
```