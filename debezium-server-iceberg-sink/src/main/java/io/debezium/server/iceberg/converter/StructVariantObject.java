/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.debezium.server.iceberg.converter;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An implementation of {@link org.apache.iceberg.variants.VariantObject} that wraps a Kafka Connect {@link Struct}.
 *
 * <p>This allows treating a Struct as a generic variant object, which can then be used to
 * create a serializable {@code ShreddedObject} or {@code SerializedObject}. This implementation is a
 * read-only wrapper and does not support direct serialization.
 */
public class StructVariantObject extends AbstractVariantObject {

  public StructVariantObject(Struct data) {
    super(createMetadata(data));
    Preconditions.checkArgument(data != null, "Invalid Struct: null");

    data.schema().fields().forEach(field -> {
      Object objectValue = data.get(field);
      shreddedObject.put(field.name(), toVariantValue(field.schema(), objectValue));
    });
  }

  private static VariantMetadata createMetadata(Struct data) {
    Preconditions.checkArgument(data != null, "Invalid Struct: null");
    return Variants.metadata(
        data.schema().fields().stream()
            .map(Field::name)
            .collect(Collectors.toList()));
  }

  private static VariantValue toVariantValue(Schema schema, Object val) {
    if (val == null) {
      return Variants.ofNull();
    }

    if (schema.name() != null) {
      if (Decimal.LOGICAL_NAME.equals(schema.name())) {
        return Variants.of((BigDecimal) val);
      }
      // Other logical types can be added here
    }

    switch (schema.type()) {
      case STRUCT:
        return new StructVariantObject((Struct) val);
      case MAP:
        Preconditions.checkArgument(schema.keySchema().type() == Schema.Type.STRING, "Only MAP with STRING key is supported");
        Map<String, ?> map = (Map<String, ?>) val;
        Schema valueSchema = schema.valueSchema();

        List<String> fieldNames = new ArrayList<>(map.keySet());
        VariantMetadata metadata = Variants.metadata(fieldNames);
        ShreddedObject shreddedObject = Variants.object(metadata);

        for (Map.Entry<String, ?> entry : map.entrySet()) {
          shreddedObject.put(entry.getKey(), toVariantValue(valueSchema, entry.getValue()));
        }
        return shreddedObject;
      case ARRAY:
        ValueArray arr = Variants.array();
        Schema elementSchema = schema.valueSchema();
        for (Object element : (List<?>) val) {
          arr.add(toVariantValue(elementSchema, element));
        }
        return arr;
      case STRING:
        return Variants.of((String) val);
      case BYTES:
        if (val instanceof byte[]) {
          return Variants.of(ByteBuffer.wrap((byte[]) val));
        }
        return Variants.of((ByteBuffer) val);
      case INT8:
        return Variants.of((Byte) val);
      case INT16:
        return Variants.of((Short) val);
      case INT32:
        return Variants.of((Integer) val);
      case INT64:
        return Variants.of((Long) val);
      case FLOAT32:
        return Variants.of((Float) val);
      case FLOAT64:
        return Variants.of((Double) val);
      case BOOLEAN:
        return Variants.of((Boolean) val);
      default:
        throw new UnsupportedOperationException("Unsupported Kafka Connect type: " + schema.type());
    }
  }
}
