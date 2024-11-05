[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI](https://github.com/memiiso/debezium-server-iceberg/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Iceberg Consumer

This project adds iceberg consumer
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). It could be used to
replicate any database(CDC changes) to could as an Iceberg table in realtime. Without requiring Spark, Kafka or
Streaming platform. It's possible to consume data in append or upsert modes.

This project introduces an Iceberg consumer for [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html), enabling real-time replication of Change Data Capture (CDC) events from any database to an Iceberg table. This eliminates the need for additional tools like Spark, Kafka, or dedicated streaming platforms.  The consumer supports data ingestion in both append and upsert modes.

See the [Documentation Page](docs/DOCS.md) for more details
For a full understanding of current limitations and recommended solutions, please review the [caveats](docs/CAVEATS.md).

![Debezium Iceberg](docs/images/debezium-iceberg.png)

# Installation
- Requirements:
  - JDK 11
  - Maven
### Building from source code
  - Clone the repository
  - Navigate to the project root directory and create distribution package.
```bash
git clone https://github.com/memiiso/debezium-server-iceberg.git
cd debezium-server-iceberg
mvn -Passembly -Dmaven.test.skip package
```
  - Extract the contents of the server distribution package
  - cd into unzipped folder
  - Create `application.properties` file. An example configuration file named [application.properties.example](debezium-server-iceberg-dist%2Fsrc%2Fmain%2Fresources%2Fdistro%2Fconf%2Fapplication.properties.example) is provided for your reference.
```bash
unzip debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist
cd appdist
nano conf/application.properties
```
  - Run the provided script: `bash run.sh` This script will launch the server using the configuration you defined in the application.properties file.
```bash
bash run.sh
```
# Python Runner for Debezium Server

It's possible to use python to run,operate debezium server

This project provides Python scripts to automate the startup, shutdown, and configuration of Debezium Server. By leveraging Python, you can manage Debezium Server.
example:

```commandline
pip install git+https://github.com/memiiso/debezium-server-iceberg.git@master#subdirectory=python
debezium
# running with custom arguments
debezium --debezium_dir=/my/debezium_server/dir/ --java_home=/my/java/homedir/
```

```python
from debezium import Debezium

d = Debezium(debezium_dir="/dbz/server/dir", java_home='/java/home/dir')
java_args = []
java_args.append("-Dquarkus.log.file.enable=true")
java_args.append("-Dquarkus.log.file.path=/logs/dbz_logfile.log")
d.run(*java_args)
```

```python
from debezium import DebeziumRunAsyn

java_args = []
java_args.append("-Dquarkus.log.file.enable=true")
java_args.append("-Dquarkus.log.file.path=/logs/dbz_logfile.log")
d = DebeziumRunAsyn(debezium_dir="/dbz/server/dir", java_home='/java/home/dir', java_args=java_args)
d.run()
d.join()
```

# Contributing

The Memiiso community welcomes anyone that wants to help out in any way, whether that includes reporting problems,
helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features.
See [contributing document](CONTRIBUTING.md) for details.

### Contributors

<a href="https://github.com/memiiso/debezium-server-iceberg/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/debezium-server-iceberg" />
</a>
