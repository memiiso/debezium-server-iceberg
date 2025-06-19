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
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link VariantObject} that wraps a Kafka Connect {@link Struct}.
 *
 * <p>This allows treating a Struct as a generic variant object, which can then be used to
 * create a serializable {@code ShreddedObject} or {@code SerializedObject}. This implementation is a
 * read-only wrapper and does not support direct serialization.
 */
public class StructVariantObject implements VariantObject {

  private final Struct data;
  private final ArrayList<String> fieldnames;
  VariantMetadata metadata;

  public StructVariantObject(Struct data) {
    Preconditions.checkArgument(data != null, "Invalid Struct: null");
    this.data = data;
    this.fieldnames = new ArrayList<>();
    data.schema().fields().forEach(f -> fieldnames.add(f.name()));
    this.metadata = org.apache.iceberg.variants.Variants.metadata(this.fieldnames);
  }

  @Override
  public VariantValue get(String name) {
    Field field = data.schema().field(name);
    if (field == null) {
      return null;
    }

    Object objectValue = data.get(field);
    return toVariantValue(field.schema(), objectValue);
  }

  @Override
  public Iterable<String> fieldNames() {
    return fieldnames;
  }

  public VariantMetadata metadata() {
    return metadata;
  }

  @Override
  public int numFields() {
    return data.schema().fields().size();
  }

  @Override
  public int sizeInBytes() {
    int totalDataSize = 0;
    for (String field : this.fieldNames()) {
      totalDataSize += this.get(field).sizeInBytes();
    }
    return totalDataSize;
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    Preconditions.checkArgument(
        buffer.order() == ByteOrder.LITTLE_ENDIAN, "Invalid byte order: big endian");
    int totalSize = offset;
    for (String field : this.fieldNames()) {
      totalSize += this.get(field).writeTo(buffer, totalSize);
    }
    return totalSize;
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
        // MAP is not supported by Variant/Iceberg schema-less evolution
        throw new UnsupportedOperationException("Unsupported Kafka Connect type: " + schema.type());
    }
  }
}
