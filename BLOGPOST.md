# Using Debezium to Create ACID Data Lake House

Do you need to build flexible Data Lakehouse but dont know where to start, do you want your data pipeline to be near realtime and support ACID transactions and updates 
its possible using two great projects Debezium and Apache Iceberg without any dependency to kafka or spark

#### Debezium
Debezium is an open source distributed platform for change data capture. 
Debezium extracts realtime database changes as json, avro, protobuf events and delivers to event streaming platforms 
(Kafka, Kinesis, Google Pub/Sub, Pulsar are just some of [supported sinks](https://debezium.io/documentation/reference/operations/debezium-server.html#_sink_configuration)), 
it provides simple interface to [implement new sink](https://debezium.io/documentation/reference/operations/debezium-server.html#_implementation_of_a_new_sink)

#### Apache Iceberg
Apache Iceberg is an open table format for huge analytic datasets, with Concurrent ACID writes, it supports Insert and Row level Deletes(Update)  [plus many other benefits](https://iceberg.apache.org)
Apache iceberg has great foundation and flexible API which currently supported by Spark, Presto, Trino, Flink and Hive

## debezium-server-iceberg

[@TODO visual architecture diagram]

This project puts both projects together and enables realtime data pipeline to any cloud storage, hdfs destination
with this project its becomes possible to use best features from both projects enjoy realtime structured data feed and ACID table format with update support

### Extending Debezium Server with Iceberg sink
debezium-server Iceberg sink to [Debezium server quarkus application](https://debezium.io/documentation/reference/operations/debezium-server.html#_installation), 

debezium-server Iceberg sink received realtime json events converted to iceberg rows and processed using iceberg API 
received rows are either appended or updated to destination iceberg table as Parquet files, since iceberg supports many cloud storage its easily possible to configure destination which could be 
any of hadoop storage cloud storage location. with debezium-server-iceberg its easily possible to replicate your RDBMS to cloud storage  

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