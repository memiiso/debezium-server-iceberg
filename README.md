[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Create Release](https://github.com/memiiso/debezium-server-iceberg/actions/workflows/release.yml/badge.svg)

# Debezium Iceberg Consumer

This project adds Iceberg consumer
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). It could be used to
replicate any database(CDC changes) to cloud as an Iceberg table in realtime. Without requiring Spark, Kafka or
Streaming platform. It's possible to consume data in append or update modes.

This project introduces an Iceberg consumer for [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html), enabling real-time replication of Change Data Capture (CDC) events from any database to an Iceberg table. This eliminates the need for additional tools like Spark, Kafka, or dedicated streaming platforms.  The consumer supports data ingestion in both append and upsert modes.

See the [Documentation Page](https://memiiso.github.io/debezium-server-iceberg/) for more details
For a full understanding of current limitations and recommended solutions, please review
the [caveats](https://memiiso.github.io/debezium-server-iceberg/caveats/).

![Debezium Iceberg](https://raw.githubusercontent.com/memiiso/debezium-server-iceberg/master/docs/images/rdbms-debezium-iceberg_white.png)

## Installation
- Requirements:
  - JDK 21
  - Maven
### Building from source code
1. Clone the repository
2. Navigate to the project root directory 
3. Create distribution package.
4. Extract the contents of the server distribution package
5. Enter into unzipped folder
6. Create `application.properties` file. An example configuration file
   named [application.properties.example](https://raw.githubusercontent.com/memiiso/debezium-server-iceberg/refs/heads/master/debezium-server-iceberg-dist/src/main/resources/distro/conf/application.properties.example)
   is provided for your reference.
7. Run the provided script: `bash run.sh` This script will launch the server using the configuration you defined in the application.properties file.

```bash
git clone https://github.com/memiiso/debezium-server-iceberg.git
cd debezium-server-iceberg
mvn -Passembly -Dmaven.test.skip package
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
