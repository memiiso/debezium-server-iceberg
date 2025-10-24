#!/bin/bash
# To enable Prometheus JMX exporter, set JMX_EXPORTER_PORT environment variable

if [ -n "${JMX_EXPORTER_PORT}" ]; then
  JMX_EXPORTER_CONFIG=${JMX_EXPORTER_CONFIG:-"config/metrics.yml"}
  JMX_EXPORTER_AGENT_JAR=$(find lib_metrics -name "jmx_prometheus_javaagent-*.jar")
  export JAVA_OPTS="-javaagent:${JMX_EXPORTER_AGENT_JAR}=0.0.0.0:${JMX_EXPORTER_PORT}:${JMX_EXPORTER_CONFIG} ${JAVA_OPTS}"
fi