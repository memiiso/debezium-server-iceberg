#! /usr/bin/env bash

#
# /*
#  * Copyright memiiso Authors.
#  *
#  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#  */
#

set -e

JAVA_HOME=$(/usr/libexec/java_home -v 11)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd $DIR
mvn clean install package -Passembly -DskipTests
cd ./debezium-server-dist/target
rm -rf debezium-server-iceberg
unzip debezium-server-dist-0.1.0-SNAPSHOT.zip
cd debezium-server-iceberg
mv ./conf/application.properties.example ./conf/application.properties
./run.sh
echo $?