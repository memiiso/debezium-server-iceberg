# Using Debezium to Create ACID Data Lake House

Do you need to build flexible Data Lakehouse but don't know where to start, do you want your data pipeline to be near realtime and support ACID transactions and updates 
its possible using two great projects Debezium and Apache Iceberg without any dependency to kafka or spark

#### Debezium
Debezium is an open source distributed platform for change data capture. 
Debezium extracts realtime database changes as json, avro, protobuf events and delivers to event streaming platforms 
(Kafka, Kinesis, Google Pub/Sub, Pulsar are just some of [supported sinks](https://debezium.io/documentation/reference/operations/debezium-server.html#_sink_configuration)), 
it provides simple interface to [implement new sink](https://debezium.io/documentation/reference/operations/debezium-server.html#_implementation_of_a_new_sink)

#### Apache Iceberg
Apache Iceberg is an open table format for huge analytic datasets, with Concurrent ACID writes, it supports Insert and Row level Deletes(Update)[it has many other benefits](https://iceberg.apache.org)
Apache iceberg has great foundation and flexible API which currently supported by Spark, Presto, Trino, Flink and Hive

## Debezium Server Iceberg

[@TODO visual architecture diagram]

Iceberg sink uses both projects and enables realtime data pipeline to any cloud storage, hdfs destination supported by iceberg
With Iceberg sink it is possible to use great features provided by both projects like realtime structured data flow and ACID table format with update support on data lake

Debezium Iceberg extends [Debezium server quarkus application](https://debezium.io/documentation/reference/operations/debezium-server.html#_installation) and implements new sink, 

Iceberg sink converts debezium json events to iceberg parquet data file, delete file and commits them to destination iceberg table using iceberg Java API

since iceberg supports many cloud storages its easily possible to configure different destinations like s3, hdfs, ...
with debezium-server-iceberg its easily possible to replicate your RDBMS to cloud storage

### update, append
Iceberg sink by default works with upsert mode. When a row updated on source table destination row replaced with the new updated version. 
with upsert mode data at destination kept identical to source data

retain deletes as soft delete! 


V 0.12 iceberg
retain deletes as soft delete!
### wait delay batch size

wait by reading debezium metrics! another great feature of debezium
### destination, iceberg catalog


@Contribution ..etc

# Links
[Apache iceberg](https://iceberg.apache.org/)
[Apache iceberg Github](https://github.com/apache/iceberg)
[Debezium](https://debezium.io/)
[Debezium Github](https://github.com/debezium/debezium)