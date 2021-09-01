# Using Debezium to Create ACID Data Lake

Do you need to build Data Lake with near realtime data pipeline, do you want it to support ACID transactions and updates on data lake?
It is possible with Debezium Server Iceberg( build using "Debezium" and "Apache Iceberg" projects). and it has no dependency to kafka or spark applications

#### Debezium
[Debezium](https://debezium.io/) is an open source distributed platform for change data capture. 
Debezium extracts realtime database changes as json, avro, protobuf events and delivers to event streaming platforms 
(Kafka, Kinesis, Google Pub/Sub, Pulsar are just some of [supported sinks](https://debezium.io/documentation/reference/operations/debezium-server.html#_sink_configuration)), 
it provides simple interface to [implement new sink](https://debezium.io/documentation/reference/operations/debezium-server.html#_implementation_of_a_new_sink)

#### Apache Iceberg
[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. 
Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink and Hive using a high-performance table format that works just like a SQL table.
It supports Insert, Row level Deletes/Updates. It has great and flexible foundation, its has [many other features](https://iceberg.apache.org)

## Debezium Server Iceberg

**Debezium Server Iceberg** project ads Iceberg consumer to debezium server.
Iceberg consumer converts received events to iceberg format and commits them to destination iceberg table. With the consumer it's possible to configure any supported iceberg destination/catalog. 
If destination table not found in the destination catalog consumer will automatically try to create it using event schema and key schema(record key)

on high level iceberg consumer
groups batch of events to event destination, 
for each destination, events are converted to iceberg records. at this step event schema used to do type mapping to iceberg record that's why `debezium.format.value.schemas.enable` should be enabled(true).
After debezium events converted to iceberg records, they are saved to iceberg parquet files(iceberg data and delete files(for upsert)), 
as last step these files are committed to destination table as data and delete file using iceberg java API.

Currently, Iceberg Consumer works only with json events. With json events it requires event schema to do data type conversion.
Currently, nested data types are not supported, so it requires event flattening. 

example configuration
```properties
debezium.sink.type=iceberg
# run with append mode
debezium.sink.iceberg.upsert=false
debezium.sink.iceberg.upsert-keep-deletes=true
# iceberg
debezium.sink.iceberg.table-prefix=debeziumcdc_
debezium.sink.iceberg.table-namespace=debeziumevents
debezium.sink.iceberg.fs.defaultFS=s3a://S3_BUCKET);
debezium.sink.iceberg.warehouse=s3a://S3_BUCKET/iceberg_warehouse
debezium.sink.iceberg.type=hadoop
debezium.sink.iceberg.catalog-name=mycatalog
debezium.sink.iceberg.catalog-impl=org.apache.iceberg.hadoop.HadoopCatalog
# enable event schemas
debezium.format.value.schemas.enable=true
debezium.format.value=json
# unwrap message
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db
debezium.transforms.unwrap.delete.handling.mode=rewrite
debezium.transforms.unwrap.drop.tombstones=true
```

### update, append
By default, Iceberg sink is running with upsert mode `debezium.sink.iceberg.upsert=true`. When a row updated on source table destination row replaced with the new updated version. 
With upsert mode data at destination kept identical to source data. Update mode uses iceberg equality delete feature and creates delete files using record key of target table
With update mode to avoid duplicate data deduplication is done on each batch

Note: For the tables without record key operation mode falls back to append even configuration is set to upsert mode

#### Keeping Deleted Records

For some use cases it's useful to keep deleted records as soft deletes, this is possible by setting `debezium.sink.iceberg.upsert-keep-deletes` to true
this setting will keep the latest version of deleted records (`__op=d`) in the iceberg table. Setting it to false will remove deleted records from the destination table.

### Append
this is most straightforward operation mode, setting `debezium.sink.iceberg.upsert` to false sets the operation mode to append, 
with append mode data deduplication is not done and all received records are appended to destination table

### Optimizing batch size (commit interval)

Debezium extracts/consumes database events in real time and this could cause too frequent commits( and too many small files) to iceberg table,
which is not optimal for batch processing especially when near realtime data feed is sufficient.
To avoid this problem it's possible to use following configuration and increase batch size per commit

**MaxBatchSizeWait**: This setting adds delay based on debezium metrics, 
it periodically monitors streaming queue size, and it starts processing events when it reaches `debezium.source.max.batch.size` value 
during the wait debezium events are collected in memory (in debezium streaming queue) and this way each commit receives more and consistent batch size
this setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size` debezium properties

example setting:
```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=testc
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=testc
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=50000
debezium.source.max.queue.size=400000
debezium.sink.batch.batch-size-wait.max-wait-ms=60000
debezium.sink.batch.batch-size-wait.wait-interval-ms=10000
```

### destination, iceberg catalog
The consumer uses iceberg catalog to read and commit data to destination table, all the catalog types and storage types used by Iceberg are supported.

### Contribution
This project is very new and there are many things to improve, please feel free to test it, give feedback, open feature request or send pull request.

- [for more details please see the project](https://github.com/memiiso/debezium-server-iceberg)
- [Releases](https://github.com/memiiso/debezium-server-iceberg/releases)