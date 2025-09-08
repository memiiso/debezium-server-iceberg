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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * An implementation of {@link org.apache.iceberg.variants.VariantObject} that wraps a Jackson {@link JsonNode}.
 *
 * <p>This allows treating a JSON object as a generic variant object, which can then be used to
 * create a serializable {@code ShreddedObject} or {@code SerializedObject}. This implementation is a
 * read-only wrapper and does not support direct serialization.
 */
public class JsonVariantObject extends AbstractVariantObject {

  public JsonVariantObject(JsonNode node) {
    super(createMetadata(node));
    Preconditions.checkArgument(node.isObject(), "Invalid JSON: not an object or null");

    node.fieldNames().forEachRemaining(fieldName -> {
      JsonNode fieldNode = node.get(fieldName);
      shreddedObject.put(fieldName, toVariantValue(fieldNode));
    });
  }

  private static VariantMetadata createMetadata(JsonNode node) {
    Preconditions.checkArgument(
        node != null && node.isObject(), "Invalid JSON: not an object or null");
    ArrayList<String> fieldNames = Lists.newArrayList(node.fieldNames());
    return Variants.metadata(fieldNames);
  }

  private static VariantValue toVariantValue(JsonNode val) {
    if (val == null || val.isNull()) {
      return Variants.ofNull();
    }

    switch (val.getNodeType()) {
      case OBJECT:
        // JSON objects are treated as maps.
        ArrayList<String> fieldNames = Lists.newArrayList(val.fieldNames());
        VariantMetadata metadata = Variants.metadata(fieldNames);
        ShreddedObject shreddedObject = Variants.object(metadata);
        val.fields().forEachRemaining(entry -> {
          shreddedObject.put(entry.getKey(), toVariantValue(entry.getValue()));
        });
        return shreddedObject;
      case ARRAY:
        ValueArray arr = Variants.array();
        for (JsonNode element : val) {
          arr.add(toVariantValue(element));
        }
        return arr;
      case STRING:
        return Variants.of(val.asText());
      case NUMBER:
        return convertNumber(val);
      case BOOLEAN:
        return Variants.of(val.asBoolean());
      case BINARY:
        try {
          return Variants.of(ByteBuffer.wrap(val.binaryValue()));
        } catch (IOException e) {
          throw new RuntimeException("Failed to get binary value from JsonNode", e);
        }
      case MISSING:
      case POJO:
      case NULL:
      default:
        return Variants.ofNull();
    }
  }

  private static VariantValue convertNumber(JsonNode val) {
    switch (val.numberType()) {
      case INT:
        return Variants.of(val.intValue());
      case LONG:
        return Variants.of(val.longValue());
      case FLOAT:
        return Variants.of(val.floatValue());
      case DOUBLE:
        return Variants.of(val.doubleValue());
      case BIG_DECIMAL:
        return Variants.of(val.decimalValue());
      case BIG_INTEGER:
        return Variants.of(new BigDecimal(val.bigIntegerValue()));
      default:
        // Should not happen with a standard JsonNode
        throw new IllegalArgumentException("Unknown number type: " + val.numberType());
    }
  }
}
