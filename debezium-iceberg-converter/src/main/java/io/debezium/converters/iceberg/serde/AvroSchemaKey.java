/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.converters.iceberg.serde;

import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

/** Package-private cache key identifying an Iceberg schema by structure and identifier fields. */
record AvroSchemaKey(Types.StructType struct, Set<Integer> identifierFieldIds) {

  static final String AVRO_ROOT_NAME = "table";

  static AvroSchemaKey of(Schema schema) {
    return new AvroSchemaKey(schema.asStruct(), schema.identifierFieldIds());
  }
}
