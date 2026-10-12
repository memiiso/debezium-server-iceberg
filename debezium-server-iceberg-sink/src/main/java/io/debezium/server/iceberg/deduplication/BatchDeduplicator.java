/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.deduplication;

import io.debezium.server.iceberg.converter.EventConverter;
import java.util.List;

/**
 * Strategy interface for deduplicating batches of change events before applying to Iceberg.
 *
 * @author Ismail Simsek
 */
public interface BatchDeduplicator {

  /**
   * Deduplicates a list of change events for a destination table within a batch.
   *
   * @param events list of raw events
   * @return list of deduplicated events
   */
  List<EventConverter> deduplicate(List<EventConverter> events);
}
