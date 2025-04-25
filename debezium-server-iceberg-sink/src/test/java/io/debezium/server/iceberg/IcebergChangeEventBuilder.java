/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

/**
 * helper class used to generate test change events
 *
 * @author Ismail Simsek
 */
@ApplicationScoped
public class IcebergChangeEventBuilder {

  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumerTest.class);
  ObjectNode payload = JsonNodeFactory.instance.objectNode();
  ObjectNode keyPayload = JsonNodeFactory.instance.objectNode();
  String destination = "test";

  @Inject
  GlobalConfig config;

  public IcebergChangeEventBuilder destination(String destination) {
    this.destination = destination;
    return this;
  }

  public IcebergChangeEventBuilder addField(String parentFieldName, String name, String val) {
    ObjectNode nestedField = JsonNodeFactory.instance.objectNode();
    nestedField.put(name, val);
    this.payload.set(parentFieldName, nestedField);
    return this;
  }

  public IcebergChangeEventBuilder addField(String parentFieldName, String name, int val) {
    ObjectNode nestedField = JsonNodeFactory.instance.objectNode();
    nestedField.put(name, val);
    this.payload.set(parentFieldName, nestedField);
    return this;
  }

  public IcebergChangeEventBuilder addField(String parentFieldName, String name, boolean val) {

    ObjectNode nestedField = JsonNodeFactory.instance.objectNode();
    if (this.payload.has(parentFieldName)) {
      nestedField = (ObjectNode) this.payload.get(parentFieldName);
    }
    nestedField.put(name, val);
    this.payload.set(parentFieldName, nestedField);
    return this;
  }

  public IcebergChangeEventBuilder addField(String name, int val) {
    payload.put(name, val);
    return this;
  }

  public IcebergChangeEventBuilder addField(String name, String val) {
    payload.put(name, val);
    return this;
  }

  public IcebergChangeEventBuilder addField(String name, long val) {
    payload.put(name, val);
    return this;
  }

  public IcebergChangeEventBuilder addField(String name, double val) {
    payload.put(name, val);
    return this;
  }

  public IcebergChangeEventBuilder addField(String name, boolean val) {
    payload.put(name, val);
    return this;
  }

  public IcebergChangeEventBuilder addKeyField(String name, int val) {
    keyPayload.put(name, val);
    payload.put(name, val);
    return this;
  }

  public IcebergChangeEventBuilder addKeyField(String name, String val) {
    keyPayload.put(name, val);
    payload.put(name, val);
    return this;
  }

  public RecordConverter build() {
    RecordConverter result = new RecordConverter(
        this.destination,
        ("{" +
            "\"schema\":" + this.valueSchema() + "," +
            "\"payload\":" + payload.toString() +
            "} "),
        ("{" +
            "\"schema\":" + this.keySchema() + "," +
            "\"payload\":" + (keyPayload.isEmpty() ? "null" : keyPayload.toString()) +
            "} "),
        this.config
    );
    // reset the builder
    this.reset();
    return result;
  }

  private void reset() {
    payload = JsonNodeFactory.instance.objectNode();
    keyPayload = JsonNodeFactory.instance.objectNode();
    destination = "test";
  }


  private ObjectNode valueSchema() {
    return getSchema(payload);
  }

  private ObjectNode keySchema() {
    return getSchema(keyPayload);
  }

  private ObjectNode getSchema(ObjectNode node) {
    ObjectNode schema = JsonNodeFactory.instance.objectNode();

    ArrayNode fs = getSchemaFields(node);
    if (fs.isEmpty()) {
      return null;
    } else {
      schema.put("type", "struct");
      schema.set("fields", fs);
      return schema;
    }
  }

  private ArrayNode getSchemaFields(ObjectNode node) {
    ArrayNode fields = JsonNodeFactory.instance.arrayNode();
    Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
    while (iter.hasNext()) {
      Map.Entry<String, JsonNode> field = iter.next();

      ObjectNode schemaField = JsonNodeFactory.instance.objectNode();
      if (field.getValue().isContainerNode()) {
        schemaField.put("type", "struct");
        schemaField.set("fields", getSchemaFields((ObjectNode) field.getValue()));
      } else if (field.getValue().isInt()) {
        schemaField.put("type", "int32");
      } else if (field.getValue().isLong()) {
        schemaField.put("type", "int64");
      } else if (field.getValue().isBoolean()) {
        schemaField.put("type", "boolean");
      } else if (field.getValue().isTextual()) {
        schemaField.put("type", "string");
      } else if (field.getValue().isFloat()) {
        schemaField.put("type", "float64");
      }
      schemaField.put("optional", !keyPayload.has(field.getKey()));
      schemaField.put("field", field.getKey());
      fields.add(schemaField);
    }

    return fields;
  }


}