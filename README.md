[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI](https://github.com/memiiso/debezium-server-iceberg/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Iceberg Consumer

This project adds iceberg consumer
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). It could be used to
replicate any database(CDC changes) to could as an Iceberg table in realtime. Without requiring Spark, Kafka or
Streaming platform. It's possible to consume data in append or upsert modes.

More detail available in [Documentation Page](docs/DOCS.md)
Also, check [caveats](docs/CAVEATS.md) for better understanding the current limitation and proper workaround

For more details, refer to the [Documentation Page](docs/DOCS.md).
Additionally, to fully understand potential challenges please review the [Caveats Section](docs/CAVEATS.md)

![Debezium Iceberg](docs/images/debezium-iceberg.png)

# Install from source
- Requirements:
  - JDK 11
  - Maven
- Clone from repo: `git clone https://github.com/memiiso/debezium-server-iceberg.git`
- From the root of the project:
  - Build and package debezium server: `mvn -Passembly -Dmaven.test.skip package`
  - After building, unzip your server
    distribution: `unzip debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist`
  - cd into unzipped folder: `cd appdist`
  - Create `application.properties` file and config it: `nano conf/application.properties`, you can check the example
    configuration
    in [application.properties.example](debezium-server-iceberg-dist%2Fsrc%2Fmain%2Fresources%2Fdistro%2Fconf%2Fapplication.properties.example)
  - Run the server using provided script: `bash run.sh`

# Debezium python runner

It's possible to use python to run,operate debezium server

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
