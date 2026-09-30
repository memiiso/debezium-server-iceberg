#!/bin/bash
#
# /*
#  * Copyright memiiso Authors.
#  *
#  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#  */
#

LIB_PATH="lib/*"

if [ "$OSTYPE" = "msys" ] || [ "$OSTYPE" = "cygwin" ]; then
  PATH_SEP=";"
else
  PATH_SEP=":"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

RUNNER=$(ls debezium-server-*runner.jar)

ENABLE_DEBEZIUM_SCRIPTING=${ENABLE_DEBEZIUM_SCRIPTING:-false}
if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" ]]; then
  LIB_PATH=$LIB_PATH$PATH_SEP"lib_opt/*"
fi

source ./jmx/enable_jmx.sh
source ./lib_metrics/enable_exporter.sh

DEFAULT_ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

JAVA_OPTS="$DEFAULT_ADD_OPENS $JAVA_OPTS"

exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp \
    $RUNNER$PATH_SEP$LIB_PATH io.debezium.server.Main