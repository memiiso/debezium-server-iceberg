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

exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp \
    $RUNNER$PATH_SEP$LIB_PATH io.debezium.server.Main