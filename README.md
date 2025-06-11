[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Create Release](https://github.com/memiiso/debezium-server-iceberg/actions/workflows/release.yml/badge.svg)

# Debezium Iceberg Consumer

This project implements Debezium Server Iceberg consumer
see [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). It enables real-time
replication of Change Data Capture (CDC) events from any database to Iceberg tables. Without requiring Spark, Kafka or
Streaming platform in between.

See the [Documentation Page](https://memiiso.github.io/debezium-server-iceberg/) for more details.

![Debezium Iceberg](https://raw.githubusercontent.com/memiiso/debezium-server-iceberg/master/docs/images/debezium-iceberg-architecture.drawio.png)

## Installation
- Requirements:
  - JDK 21
  - Maven
### Building from source code

```bash
git clone https://github.com/memiiso/debezium-server-iceberg.git
cd debezium-server-iceberg
mvn -Passembly -Dmaven.test.skip package
# unzip and run the application
unzip debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist
cd appdist/debezium-server-iceberg
mv conf/application.properties.example conf/application.properties
bash run.sh
```

## Contributing

The Memiiso community welcomes anyone that wants to help out in any way, whether that includes reporting problems,
helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features.
See [contributing document](docs/contributing.md) for details.

### Contributors

<a href="https://github.com/memiiso/debezium-server-iceberg/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/debezium-server-iceberg" />
</a>
