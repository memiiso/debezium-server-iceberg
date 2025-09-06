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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SortedMerge;
import org.apache.iceberg.variants.SerializedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link VariantObject} that wraps a Kafka Connect {@link Struct}.
 *
 * <p>This allows treating a Struct as a generic variant object, which can then be used to
 * create a serializable {@code ShreddedObject} or {@code SerializedObject}. This implementation is a
 * read-only wrapper and does not support direct serialization.
 */
public class StructVariantObject implements VariantObject {

  private final Struct data;
  VariantMetadata metadata;
  Map<String, VariantValue> variantFields = Maps.newHashMap();
  private SerializationState serializationState;

  public StructVariantObject(Struct data) {
    Preconditions.checkArgument(data != null, "Invalid Struct: null");
    this.data = data;
    data.schema().fields().forEach(f -> variantFields.put(f.name(), this.toVariantvalue(f.name())));
    this.metadata = Variants.metadata(variantFields.keySet());
  }

  @Override
  public VariantValue get(String name) {
    return variantFields.get(name);
  }

  public VariantValue toVariantvalue(String name) {
    Field field = data.schema().field(name);
    if (field == null) {
      return Variants.ofNull();
    }

    Object objectValue = data.get(field);
    return toVariantValue(field.schema(), objectValue);
  }

  @Override
  public Iterable<String> fieldNames() {
    return variantFields.keySet();
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


  private static class SerializationState {
    private final VariantMetadata metadata;
    private final Map<String, ByteBuffer> unshreddedFields;
    private final Map<String, VariantValue> shreddedFields;
    private final int dataSize;
    private final int numElements;
    private final boolean isLarge;
    private final int fieldIdSize;
    private final int offsetSize;

    private SerializationState(VariantMetadata metadata, VariantObject unshredded, Map<String, VariantValue> shreddedFields, Set<String> removedFields) {
      this.metadata = metadata;
      this.fieldIdSize = VariantUtil.sizeOf(metadata.dictionarySize());
      this.shreddedFields = Maps.newHashMap(shreddedFields);
      int totalDataSize = 0;
      ImmutableMap.Builder<String, ByteBuffer> unshreddedBuilder = ImmutableMap.builder();
      if (unshredded instanceof SerializedObject) {
        SerializedObject serialized = (SerializedObject)unshredded;

        for(Map.Entry<String, Integer> field : serialized.fields()) {
          String name = (String)field.getKey();
          boolean replaced = shreddedFields.containsKey(name) || removedFields.contains(name);
          if (!replaced) {
            ByteBuffer value = serialized.sliceValue((Integer)field.getValue());
            unshreddedBuilder.put(name, value);
            totalDataSize += value.remaining();
          }
        }
      } else if (unshredded != null) {
        for(String name : unshredded.fieldNames()) {
          boolean replaced = shreddedFields.containsKey(name) || removedFields.contains(name);
          if (!replaced) {
            shreddedFields.put(name, unshredded.get(name));
          }
        }
      }

      this.unshreddedFields = unshreddedBuilder.build();
      this.numElements = this.unshreddedFields.size() + shreddedFields.size();
      this.isLarge = this.numElements > 255;

      for(VariantValue value : shreddedFields.values()) {
        totalDataSize += value.sizeInBytes();
      }

      this.dataSize = totalDataSize;
      this.offsetSize = VariantUtil.sizeOf(totalDataSize);
    }

    private int size() {
      return 1 + (this.isLarge ? 4 : 1) + this.numElements * this.fieldIdSize + (1 + this.numElements) * this.offsetSize + this.dataSize;
    }

    private int writeTo(ByteBuffer buffer, int offset) {
      int fieldIdListOffset = offset + 1 + (this.isLarge ? 4 : 1);
      int offsetListOffset = fieldIdListOffset + this.numElements * this.fieldIdSize;
      int dataOffset = offsetListOffset + (1 + this.numElements) * this.offsetSize;
      byte header = VariantUtil.objectHeader(this.isLarge, this.fieldIdSize, this.offsetSize);
      VariantUtil.writeByte(buffer, header, offset);
      VariantUtil.writeLittleEndianUnsigned(buffer, this.numElements, offset + 1, this.isLarge ? 4 : 1);
      Iterable<String> fields = SortedMerge.of(() -> this.unshreddedFields.keySet().stream().sorted().iterator(), () -> this.shreddedFields.keySet().stream().sorted().iterator());
      int nextValueOffset = 0;
      int index = 0;

      for(String field : fields) {
        int id = this.metadata.id(field);
        Preconditions.checkState(id >= 0, "Invalid metadata, missing: %s", field);
        VariantUtil.writeLittleEndianUnsigned(buffer, id, fieldIdListOffset + index * this.fieldIdSize, this.fieldIdSize);
        VariantUtil.writeLittleEndianUnsigned(buffer, nextValueOffset, offsetListOffset + index * this.offsetSize, this.offsetSize);
        VariantValue shreddedValue = (VariantValue)this.shreddedFields.get(field);
        int valueSize;
        if (shreddedValue != null) {
          valueSize = shreddedValue.writeTo(buffer, dataOffset + nextValueOffset);
        } else {
          valueSize = VariantUtil.writeBufferAbsolute(buffer, dataOffset + nextValueOffset, (ByteBuffer)this.unshreddedFields.get(field));
        }

        nextValueOffset += valueSize;
        ++index;
      }

      VariantUtil.writeLittleEndianUnsigned(buffer, nextValueOffset, offsetListOffset + index * this.offsetSize, this.offsetSize);
      return dataOffset - offset + this.dataSize;
    }
  }
}
