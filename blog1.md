
@TODO cleanup documentation 
@TODO add help page contributing page 

# Using Debezium to Create ACID Data Lake House

Do you need to build flexible Data Lakehouse but dont know where to start, do you want your data pipeline to be near realtime and support ACID transactions and updates 
its possible using two great projects Debezium and Apache Iceberg without any dependency to kafka or spark

#### Debezium
Debezium is an open source distributed platform for change data capture. 
Debezium extracts realtime database changes as json, avro, protobuf events and delivers to event streaming platforms 
(Kafka, Kinesis, Google Pub/Sub, Pulsar are just some of them) LINK here, it provides very simple interface to extend and write new sinks 

#### Apache Iceberg
Apache Iceberg is an open table format for huge analytic datasets, with Concurrent ACID writes, it supports Insert and Update queries, plus many other features listed here
Link 
Apache iceberg has API fundation which supported by Spark and Presto and Trino, 

## debezium-server-iceberg

[@TODO visual architecture diagram]

Project puts both projects together and enables realtime data pipeline to any cloud storage, hdfs destination
with this project its becomes possible to use best features from both projects enjoy realtime structured data feed and ACID table format with update support

### Extending Debezium Server with custom sink
debezium-server-iceberg  adds custom sink to Debezium server quarkus application [link here], 
with custom sink received realtime json events converted to iceberg rows and processed using iceberg api 
received rows are either appended or updated target table using iceberg api, since iceberg supports many cloud storage its eaily porrible to configure destination which could be 
any of hadoop storage cloud storage location, consumed events are added to destination table as parquet files

# update, append

# destination, iceberg catalog

# wait delay batch size


@Contribution 


thanks to two projects