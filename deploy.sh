#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
#mvn clean package -Passembly -Dmaven.test.skip=true
mvn clean package -Passembly -Dmaven.test.skip=true --quiet -Drevision=latest

cd "${SCRIPT_DIR}/debezium-server-iceberg-dist/target/"
echo $(pwd)
unzip "debezium-server-iceberg-dist*.zip"
cd debezium-server-iceberg
cp config/application.properties.example config/application.properties
bash run.sh