# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)

## 0.2.0.Beta

January 8th 2022

### New features since 0.1.0.Alpha

* Enable automatic field addition to support schema
  evolution [#74](https://github.com/memiiso/debezium-server-iceberg/pull/74)
* Support writing partitioned iceberg tables [#71](https://github.com/memiiso/debezium-server-iceberg/pull/71)
* Add support for array data type [#63](https://github.com/memiiso/debezium-server-iceberg/pull/63)
* Improving documentation [#61](https://github.com/memiiso/debezium-server-iceberg/pull/61)
* Update distribution to include oracle and db2
  connectors [#60](https://github.com/memiiso/debezium-server-iceberg/pull/60)
* add Dockerfile and release image to github container
  registry [#53](https://github.com/memiiso/debezium-server-iceberg/pull/53)
* Support all iceberg file formats parquet,orc,avro [#46](https://github.com/memiiso/debezium-server-iceberg/pull/46)
* Support nested data types [#41](https://github.com/memiiso/debezium-server-iceberg/pull/41)
* Add batch size wait to optimize batch size by monitoring debezium queue
  size [#16](https://github.com/memiiso/debezium-server-iceberg/pull/16)

## 0.1.0.Alpha

April 23 2021

### New features

* Implement `upsert` mode to consume deduplicated events to destination table in addition to append
  mode [#8](https://github.com/memiiso/debezium-server-iceberg/pull/8)
* Add dynamic wait feature to optimize batch size and commit
  interval [#6](https://github.com/memiiso/debezium-server-iceberg/pull/6)
* Partition `debezium_events` table by destination and event_sink
  hour [#5](https://github.com/memiiso/debezium-server-iceberg/pull/5)