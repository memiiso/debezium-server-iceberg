![Java CI with Maven](https://github.com/memiiso/debezium-server-iceberg/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Iceberg Consumers

This project adds iceberg consumer to [debezium server application](https://debezium.io/documentation/reference/operations/debezium-server.html). it could be used to
replicate database changes to iceberg table(Cloud storage, hdfs) without requiring Spark, Kafka or Streaming platform.

## `iceberg` Consumer

Iceberg consumer appends or upserts debezium events to destination iceberg tables. When event and key schemas
enabled (`debezium.format.value.schemas.enable=true`, `debezium.format.key.schemas.enable=true`) destination iceberg
tables created automatically.

### Upsert

By default, iceberg consumer is running with upsert mode `debezium.sink.iceberg.upsert=true`. 
for the tables with Primary Key definition consumer does upsert, for the tables without Primary Key consumer falls back to append mode

#### Data Deduplication

With upsert mode data deduplication is done per batch, deduplication is done based on `__source_ts_ms` value and event type `__op`
its is possible to change field using `debezium.sink.iceberg.upsert-dedup-column=__source_ts_ms`, Currently only
Long field type supported

operation type priorities are `{"c":1, "r":2, "u":3, "d":4}` when two records with same Key and same `__source_ts_ms`
values received then the record with higher `__op` priority is added to destination table and duplicate record is dropped.

### Append
Setting `debezium.sink.iceberg.upsert=false` will set the operation mode to append, with append mode data deduplication is not done, all received records are appended to destination table
Note: For the tables without primary key operation mode is append even configuration is set to upsert mode

#### Keeping Deleted Records

By default `debezium.sink.iceberg.upsert-keep-deletes=true` will keep deletes in the iceberg table, setting it to false
will remove deleted records from the destination iceberg table. With this config its possible to keep last version of the deleted
record in the table(to do soft deletes).

### Optimizing batch size (or commit interval)

Debezium extracts database events in real time and this could cause too frequent commits or too many small files
which is not optimal for batch processing especially when near realtime data feed is sufficient. 
To avoid this problem following batch-size-wait classes are used. 

Batch size wait adds delay between consumer calls to increase total number of events received per call and meanwhile events are collected in memory
this should be configured with `debezium.source.max.queue.size` and `debezium.source.max.batch.size` debezium properties


#### NoBatchSizeWait

This is default configuration by default consumer will not use any batch size wait

#### DynamicBatchSizeWait

This wait strategy dynamically adds wait to increase batch size. Wait duration is calculated based on number of processed events in
last 3 batches. if last batch sizes are lower than `max.batch.size` Wait duration will increase and if last batch sizes
are bigger than 90% of `max.batch.size` Wait duration will decrease

This strategy tries to keep batch size between 85%-90% of the `max.batch.size`, it does not guarantee consistent batch size.

example setup to receive ~2048 events per commit. maximum wait is set to 5 seconds
```properties
debezium.source.max.queue.size=16000
debezium.source.max.batch.size=2048
debezium.sink.batch.batch-size-wait=DynamicBatchSizeWait
debezium.sink.batch.batch-size-wait.max-wait-ms=5000
```
#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size, this strategy is more precise compared to DynamicBatchSizeWait
DynamicBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size` 
maximum wait and check intervals are controlled by `debezium.sink.batch.batch-size-wait.max-wait-ms`, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties

example setup to receive ~2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size checked every 5 seconds
```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=testc
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=testc
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048");
debezium.source.max.queue.size=16000");
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

### Destination Iceberg Table Names

iceberg table names created by following rule : `table-namespace`.`table-prefix``database.server.name`_`database`_`table`

For example with following config

```properties
debezium.sink.iceberg.table-namespace=default
database.server.name=testc
debezium.sink.iceberg.table-prefix=cdc_
```

database table = `inventory.customers` will be replicated to `default.testc_cdc_inventory_customers`

## Debezium Event Flattening

Iceberg consumer requires event flattening, Currently nested events and complex data types(like Struct) are not supported

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
```

### Configuring iceberg 

All the properties starting with `debezium.sink.iceberg.__ICEBERG_CONFIG__` are passed to iceberg, and to hadoopConf

```properties
debezium.sink.iceberg.{iceberg.prop.name}=xyz-value # passed to iceberg!
```

### Example Configuration

```properties
debezium.sink.type=iceberg
debezium.sink.iceberg.table-prefix=debeziumcdc_
debezium.sink.iceberg.catalog-name=mycatalog
debezium.sink.iceberg.table-namespace=debeziumevents
debezium.sink.iceberg.fs.defaultFS=s3a://MY_S3_BUCKET
debezium.sink.iceberg.warehouse=s3a://MY_S3_BUCKET/iceberg_warehouse
debezium.sink.iceberg.com.amazonaws.services.s3.enableV4=true
debezium.sink.iceberg.com.amazonaws.services.s3a.enableV4=true
debezium.sink.iceberg.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain
debezium.sink.iceberg.fs.s3a.access.key=S3_ACCESS_KEY
debezium.sink.iceberg.fs.s3a.secret.key=S3_SECRET_KEY
debezium.sink.iceberg.fs.s3a.path.style.access=true
debezium.sink.iceberg.fs.s3a.endpoint=http://localhost:9000 # minio specific setting
debezium.sink.iceberg.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
debezium.sink.iceberg.upsert=true
debezium.sink.iceberg.upsert-keep-deletes=true
debezium.format.value.schemas.enable=true
debezium.format.key.schemas.enable=true
```

## `icebergevents` Consumer

This is second consumer developed with this project, This consumer appends CDC events to single iceberg table as json string. 
This table partitioned by `event_destination,event_sink_timestamptz` and sorted by `event_sink_epoch_ms`

#### Example Configuration

````properties
debezium.sink.type=icebergevents
debezium.sink.iceberg.catalog-name=default
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
