![Java CI with Maven](https://github.com/memiiso/debezium-server-iceberg/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Iceberg Consumers

This project adds iceberg batch consumers
to [debezium server](https://debezium.io/documentation/reference/operations/debezium-server.html)
someone could use this consumer and replicate database data to iceberg table, without Spark, Kafka or Streaming
platform.

## `iceberg` Consumer

Appends json events to destination iceberg tables, batch size is determined by debezium. Destination table created
Automatically if the schema is enabled `debezium.format.value.schemas.enable=true`.

Confiquration

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

```

all the properties starting with `debezium.sink.iceberg.**` are passed to iceberg, and hadoopConf

```properties
debezium.sink.iceberg.{iceberg.prop.name} = xyz-value # passed to iceberg!
```

debezium unwrap message

```properties
debezium.transforms = unwrap
debezium.transforms.unwrap.type = io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields = op,table,lsn,source.ts_ms
debezium.transforms.unwrap.add.headers = db
debezium.transforms.unwrap.delete.handling.mode = rewrite
```

## `icebergevents` Consumer

Appends json CDC events to single iceberg table

Confiquration

````properties
debezium.sink.type = icebergevents
debezium.sink.iceberg.catalog-name = default
````

iceberg table is

```java
static final String TABLE_NAME="debezium_events";
static final Schema TABLE_SCHEMA=new Schema(
    required(1,"event_destination",Types.StringType.get(),"event destination"),
    optional(2,"event_key",Types.StringType.get()),
    optional(3,"event_value",Types.StringType.get()),
    optional(4,"event_sink_timestamp",Types.TimestampType.withZone()));
static final PartitionSpec TABLE_PARTITION=PartitionSpec.builderFor(TABLE_SCHEMA).identity("event_destination").build();
```

## `icebergupsert` Consumer

@TODO Planed, requires iceberg spec V2

## Configuring Debezium Batch size

`max.batch.size` Positive integer value that specifies the maximum size of each batch of events that should be processed
during each iteration of this connector. Defaults to 2048.

`poll.interval.ms` Positive integer value that specifies the number of milliseconds the connector should wait for new
change events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.

```properties
# Positive integer value that specifies the maximum size of each batch of events that should be processed during each iteration of this connector. Defaults to 2048.
debezium.source.max.batch.size = 2
# Positive integer value that specifies the number of milliseconds the connector should wait for new change events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.
debezium.source.poll.interval.ms = 10000
```