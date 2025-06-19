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

import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

import java.nio.ByteBuffer;

public abstract class AbstractVariantObject implements VariantObject {

  protected final ShreddedObject shreddedObject;
  protected final VariantMetadata metadata;

  protected AbstractVariantObject(VariantMetadata metadata) {
    this.metadata = metadata;
    this.shreddedObject = Variants.object(this.metadata);
  }

  public VariantMetadata metadata() {
    return this.metadata;
  }

  @Override
  public VariantValue get(String name) {
    return shreddedObject.get(name);
  }

  @Override
  public Iterable<String> fieldNames() {
    return shreddedObject.fieldNames();
  }

  @Override
  public int numFields() {
    return shreddedObject.numFields();
  }

  @Override
  public int sizeInBytes() {
    return shreddedObject.sizeInBytes();
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    return shreddedObject.writeTo(buffer, offset);
  }
}
