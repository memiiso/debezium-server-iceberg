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

## Release & Compatibility Matrix

The following table details version compatibility across all releases and tags, highlighting the core runtime and storage dependencies:

| Release / Tag | Release Date | Debezium Version | Iceberg Version | Spark Runtime | Java | Key Capabilities & Highlights |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`1.2.0.Final`** *(master)* | *Upcoming* | `3.6.3.Final` | `1.11.0` | `4.0.3` | `21` | Iceberg v3 Deletion Vectors (#720), parallel upload safety (#754), nullable field defaults (#749) |
| **`1.1.1.Final`** | 2026-09-29 | `3.6.3.Final` | `1.10.2` | `4.0.3` | `21` | Upgraded to Debezium 3.6.3.Final |
| **`1.1.0.Final`** | 2026-07-18 | `3.6.0.Final` | `1.10.2` | `4.0.3` | `21` | Debezium 3.6 baseline, Spark 4.0.3 runtime |
| **`1.0.0.Final`** | 2025-11-26 | `3.3.1.Final` | `1.10.0` | `4.0.0` | `21` | Java 21 migration, Iceberg 1.10.0 production GA |
| **`1.0.0.Beta1`** | 2025-09-12 | `3.3.0.Beta1` | `1.10.0` | `4.0.0` | `21` | Java 21 preview, Iceberg 1.10 pre-release |
| **`1.0.0.Alpha1`** | 2025-05-18 | `3.1.1.Final` | `1.8.1` | `4.0.0-preview2` | `17` | Initial 1.0 architecture line |
| **`0.9.0.Final`** | 2025-04-25 | `3.1.1.Final` | `1.8.1` | `4.0.0-preview2` | `17` | Debezium 3.1 upgrade, Iceberg 1.8.1 |
| **`0.9.0.Alpha`** | 2025-02-25 | `3.1.0.Alpha2` | `1.7.1` | `4.0.0-preview2` | `17` | Debezium 3.1 alpha testing |
| **`0.8.2.Final`** | 2025-01-31 | `3.0.7.Final` | `1.7.1` | `4.0.0-preview1` | `17` | Debezium 3.0.7 maintenance release |
| **`0.8.1.Final`** | 2024-12-17 | `2.7.4.Final` | `1.7.1` | `4.0.0-preview1` | `17` | Iceberg 1.7.1 patch update |
| **`0.8.0.Final`** | 2024-12-03 | `2.7.3.Final` | `1.7.0` | `4.0.0-preview1` | `17` | Iceberg 1.7.0 upgrade |
| **`0.7.0.Final`** | 2024-09-08 | `2.7.2.Final` | `1.6.1` | `4.0.0-preview1` | `17` | Iceberg 1.6.1, Debezium 2.7.2 |
| **`0.6.0.Final`** | 2024-08-03 | `2.7.0.Final` | `1.6.0` | `4.0.0-preview1` | `17` | Iceberg 1.6.0 GA release |
| **`0.5.0.Final`** | 2024-07-24 | `2.7.0.Final` | `1.6.0` | `4.0.0-preview1` | `17` | Java 17 baseline consolidation |
| **`0.5.0.Beta`** | 2024-06-28 | `2.7.0.Final` | `1.5.2` | `4.0.0-preview1` | `17` | Java 17 baseline migration |
| **`0.4.1.Final`** | 2024-05-25 | `2.7.0.Alpha1` | `1.5.2` | `4.0.0-preview1` | `17` | Early Debezium 2.7 test build |
| **`0.4.0.Final`** | 2024-05-02 | `2.5.4.Final` | `1.5.2` | `3.5.1` | `11` | Iceberg 1.5.2, Spark 3.5.1 |
| **`0.4.0.Beta`** | 2024-03-03 | `2.5.2.Final` | `1.5.0` | `3.5.1` | `11` | Spark 3.5 pre-release |
| **`0.3.0.Final`** | 2024-03-03 | `2.5.2.Final` | `1.5.0` | `3.5.1` | `11` | Debezium 2.5, Iceberg 1.5.0 GA |
| **`0.3.0.Beta`** | 2023-09-10 | `2.2.1.Final` | `1.3.1` | `3.3.2` | `11` | Iceberg 1.3.1 |
| **`0.2.0.Final`** | 2022-09-04 | `1.9.5.Final` | `0.14.0` | `3.2.2` | `11` | Iceberg 0.14.0, Debezium 1.9 |
| **`0.2.0.Beta`** | 2022-01-08 | `1.8.0.Final` | `0.12.1` | `3.1.2` | `11` | Early Beta build |
| **`0.1.0.Alpha`** | 2021-04-23 | `1.5.0.Final` | `0.11.1` | `3.0.2` | `11` | Initial project alpha release |

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
mv config/application.properties.example config/application.properties
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
