/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import io.debezium.server.iceberg.deduplication.RetainLastBatchDeduplicator;
import io.debezium.server.iceberg.deduplication.TimestampAndOpBatchDeduplicator;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for deduplicator resolution in {@link IcebergTableOperator}.
 *
 * @author Ismail Simsek
 */
class IcebergTableOperatorDeduplicatorTest {

  @Test
  void testResolveDeduplicator() {
    IcebergTableOperator operator = new IcebergTableOperator();
    GlobalConfig globalConfig = mock(GlobalConfig.class);
    IcebergConfig icebergConfig = mock(IcebergConfig.class);
    when(globalConfig.iceberg()).thenReturn(icebergConfig);
    operator.config = globalConfig;

    // 1. Explicit RetainLast
    when(icebergConfig.deduplicator()).thenReturn(Optional.of("RetainLast"));
    assertInstanceOf(RetainLastBatchDeduplicator.class, operator.resolveDeduplicator());

    // 2. Explicit retain-last
    when(icebergConfig.deduplicator()).thenReturn(Optional.of("retain-last"));
    assertInstanceOf(RetainLastBatchDeduplicator.class, operator.resolveDeduplicator());

    // 3. Explicit Default (maps to TimestampAndOpBatchDeduplicator)
    when(icebergConfig.deduplicator()).thenReturn(Optional.of("Default"));
    assertInstanceOf(TimestampAndOpBatchDeduplicator.class, operator.resolveDeduplicator());

    // 4. Explicit TimestampAndOp
    when(icebergConfig.deduplicator()).thenReturn(Optional.of("TimestampAndOp"));
    assertInstanceOf(TimestampAndOpBatchDeduplicator.class, operator.resolveDeduplicator());

    // 5. Implicit TimestampAndOp via cdcSourceTsField
    when(icebergConfig.deduplicator()).thenReturn(Optional.empty());
    when(icebergConfig.cdcSourceTsField()).thenReturn(Optional.of("__source_ts_ns"));
    assertInstanceOf(TimestampAndOpBatchDeduplicator.class, operator.resolveDeduplicator());

    // 6. Default fallback -> RetainLast
    when(icebergConfig.cdcSourceTsField()).thenReturn(Optional.empty());
    assertInstanceOf(RetainLastBatchDeduplicator.class, operator.resolveDeduplicator());
  }
}
