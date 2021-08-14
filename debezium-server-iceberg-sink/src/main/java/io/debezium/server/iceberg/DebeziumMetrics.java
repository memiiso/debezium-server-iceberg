/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import javax.enterprise.context.Dependent;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ismail Simsek
 */
@Dependent
public class DebeziumMetrics {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumMetrics.class);
  final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
  @ConfigProperty(name = "debezium.sink.batch.metrics.snapshot-mbean", defaultValue = "")
  Optional<String> snapshotMbean;
  @ConfigProperty(name = "debezium.sink.batch.metrics.streaming-mbean", defaultValue = "")
  Optional<String> streamingMbean;
  @ConfigProperty(name = "debezium.source.max.queue.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE + "")
  int maxQueueSize;

  ObjectName snapshotMetricsObjectName;
  ObjectName streamingMetricsObjectName;

  public void initizalize() throws DebeziumException {
    assert snapshotMbean.isPresent() :
        "Snapshot metrics Mbean `debezium.sink.batch.metrics.snapshot-mbean` not provided";
    assert streamingMbean.isPresent() :
        "Streaming metrics Mbean `debezium.sink.batch.metrics.streaming-mbean` not provided";
    try {
      snapshotMetricsObjectName = new ObjectName(snapshotMbean.get());
      streamingMetricsObjectName = new ObjectName(streamingMbean.get());
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public boolean snapshotRunning() {
    try {
      return (boolean) mbeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public boolean snapshotCompleted() {
    try {
      return (boolean) mbeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public int streamingQueueRemainingCapacity() {
    try {
      return (int) mbeanServer.getAttribute(streamingMetricsObjectName, "QueueRemainingCapacity");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public int streamingQueueCurrentSize() {
    return maxQueueSize - streamingQueueRemainingCapacity();
  }

  public long streamingMilliSecondsBehindSource() {
    try {
      return (long) mbeanServer.getAttribute(streamingMetricsObjectName, "MilliSecondsBehindSource");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

}
