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

Debezium Server Iceberg project puts both projects together and enables realtime data pipeline to any cloud storage, hdfs destination supported by iceberg
Debezium Server Iceberg it is possible to use best features from both projects like realtime structured data pipeline and ACID table format with update support

Debezium Iceberg sink extends [Debezium server quarkus application](https://debezium.io/documentation/reference/operations/debezium-server.html#_installation), 

Iceberg consumer converts debezium json events to iceberg rows and commits them to destination iceberg table using iceberg API 
It's possible to append database events to iceberg tables or do upsert using source table primary key
since iceberg supports many cloud storage its easily possible to configure destination which could be any of hadoop storage cloud storage location. 
with debezium-server-iceberg its easily possible to replicate your RDBMS to cloud storage  

# update, append
Iceberg consumer by default works with upsert mode. When a row updated on source table destination row replaced with up-to-date record. 
with upsert mode data at destination is always deduplicate and kept up to date


V 0.12 iceberg
retain deletes as soft delete!
# wait delay batch size

wait by reading debezium metrics! another great feature of debezium
# destination, iceberg catalog

@Contribution ..etc

# Links
[Apache iceberg](https://iceberg.apache.org/)
[Apache iceberg Github](https://github.com/apache/iceberg)
[Debezium](https://debezium.io/)
[Debezium Github](https://github.com/debezium/debezium)