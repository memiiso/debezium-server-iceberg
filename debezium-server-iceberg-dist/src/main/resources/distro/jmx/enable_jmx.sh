#!/bin/bash
# To enable JMX functionality, export the JMX_HOST and JMX_PORT environment variables.
# Modify the jmxremote.access and jmxremote.password files accordingly.
if [ -n "${JMX_HOST}" -a -n "${JMX_PORT}" ]; then
     export JAVA_OPTS="-Dcom.sun.management.jmxremote.ssl=false \
     -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
     -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT} \
     -Dcom.sun.management.jmxremote.local.only=false \
     -Djava.rmi.server.hostname=${JMX_HOST} \
     -Dcom.sun.management.jmxremote.verbose=true"

  if [ -f "jmx/jmxremote.access" -a -f "jmx/jmxremote.password" ]; then
    chmod 600 jmx/jmxremote.password
    export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.authenticate=true \
       -Dcom.sun.management.jmxremote.access.file=jmx/jmxremote.access \
       -Dcom.sun.management.jmxremote.password.file=jmx/jmxremote.password"
  else
   export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
  fi
fi
