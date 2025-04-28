package io.debezium.server.iceberg.converter;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A builder class to conveniently create Kafka Connect Struct objects
 * by **inferring** the schema from the provided field names and values.
 * <p>
 * Note: Schema inference has limitations and relies on the runtime type
 * of the provided values. All inferred fields are marked as **optional**.
 * For precise schema control (required fields, specific logical types, defaults),
 * </p>
 */
public class StructBuilder {

  private static final Logger log = LoggerFactory.getLogger(StructBuilder.class);

  private final String schemaName;
  // Use LinkedHashMap to preserve field order for schema building
  private final Map<String, Object> fields = new LinkedHashMap<>();

  private StructBuilder(String schemaName) {
    Objects.requireNonNull(schemaName, "Schema name cannot be null");
    if (schemaName.trim().isEmpty()) {
      throw new IllegalArgumentException("Schema name cannot be empty");
    }
    this.schemaName = schemaName;
  }

  /**
   * Static factory method to start building a Struct, inferring its schema.
   *
   * @param schemaName The desired name for the inferred schema (e.g., "my.event.Value").
   * @return A new InferringStructBuilder instance.
   */
  public static StructBuilder create(String schemaName) {
    return new StructBuilder(schemaName);
  }

  /**
   * Adds a field and its value. The schema type will be inferred from the value
   * during the build() phase.
   *
   * @param fieldName The name of the field.
   * @param value     The value for the field. Used for schema inference. Null values are allowed,
   *                  but their type might be inferred generically (e.g., String) if no other
   *                  field provides a concrete type hint during build. It's best practice
   *                  to provide non-null values for reliable inference.
   * @return This builder instance for chaining.
   */
  public StructBuilder field(String fieldName, Object value) {
    Objects.requireNonNull(fieldName, "Field name cannot be null");
    if (fieldName.trim().isEmpty()) {
      throw new IllegalArgumentException("Field name cannot be empty");
    }
    fields.put(fieldName, value);
    return this;
  }

  /**
   * Builds the Struct by first inferring the schema based on the fields added
   * and then populating the Struct with the provided values.
   *
   * @return The constructed Struct with an inferred schema.
   * @throws org.apache.kafka.connect.errors.DataException if schema inference fails for any field or
   *                                                       if putting a value into the struct fails.
   */
  public Struct build() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(schemaName);
    Map<String, Schema> inferredFieldSchemas = new HashMap<>();

    // 1. Infer schema for each field based on its value
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      String fieldName = entry.getKey();
      Object value = entry.getValue();
      try {
        Schema fieldSchema = inferSchema(value); // Infer schema for this value
        // All fields are optional in this simple inference model
        schemaBuilder.field(fieldName, fieldSchema);
        inferredFieldSchemas.put(fieldName, fieldSchema); // Store for struct creation
      } catch (Exception e) {
        throw new org.apache.kafka.connect.errors.DataException(
            "Failed to infer schema for field '" + fieldName + "' with value type "
                + (value != null ? value.getClass().getName() : "null"), e);
      }
    }

    Schema finalSchema = schemaBuilder.build();
    Struct struct = new Struct(finalSchema);

    // 2. Populate the struct using the inferred schema, preparing values if needed
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      String fieldName = entry.getKey();
      Object value = entry.getValue();
      Schema fieldSchema = inferredFieldSchemas.get(fieldName);
      // Prepare value (e.g., convert byte[] to ByteBuffer, UUID to String, time types)
      Object valueToPut = prepareValueForSchema(value, fieldSchema);
      try {
        // Struct.put performs final validation against the inferred schema type
        struct.put(fieldName, valueToPut);
      } catch (org.apache.kafka.connect.errors.DataException e) {
        // This might happen if inference was slightly off or value is incompatible
        throw new org.apache.kafka.connect.errors.DataException(
            "Failed to put value for field '" + fieldName + "' into struct with inferred schema. "
                + "Value type: " + (value != null ? value.getClass().getName() : "null")
                + ", Inferred Schema: " + fieldSchema, e);
      }
    }

    return struct;
  }

  public EmbeddedEngineChangeEvent buildChangeEvent() {
    return buildChangeEvent(null);
  }

  public EmbeddedEngineChangeEvent buildChangeEvent(Struct key) {
    Struct value = build();
    return EventFactory.createMockChangeEvent(key, value, "test-destionation");
  }

  /**
   * Infers the Kafka Connect Schema based on the runtime type of the value.
   * Marks the inferred schema as optional.
   * Handles basic types, common logical types, and basic collections.
   */
  private Schema inferSchema(Object value) {
    if (value == null) {
      log.warn("Inferring schema for null value as OPTIONAL_STRING_SCHEMA. Provide non-null value or explicit type for better inference.");
      return Schema.OPTIONAL_STRING_SCHEMA;
    }

    // Basic Types
    if (value instanceof String) return Schema.OPTIONAL_STRING_SCHEMA;
    if (value instanceof Integer) return Schema.OPTIONAL_INT32_SCHEMA;
    if (value instanceof Long) return Schema.OPTIONAL_INT64_SCHEMA;
    if (value instanceof Float) return Schema.OPTIONAL_FLOAT32_SCHEMA;
    if (value instanceof Double) return Schema.OPTIONAL_FLOAT64_SCHEMA;
    if (value instanceof Boolean) return Schema.OPTIONAL_BOOLEAN_SCHEMA;
    if (value instanceof byte[] || value instanceof ByteBuffer) return Schema.OPTIONAL_BYTES_SCHEMA;

    // Logical Types & Time
    if (value instanceof BigDecimal) {
      int scale = ((BigDecimal) value).scale();
      scale = Math.max(0, scale); // Ensure scale is non-negative
      log.debug("Inferring Decimal schema with scale: {}", scale);
      return Decimal.builder(scale).optional().build();
    }
    if (value instanceof java.util.Date || value instanceof Instant || value instanceof LocalDateTime) {
      if (value instanceof LocalDateTime) {
        log.warn("Inferring java.time.LocalDateTime as Timestamp schema. Timezone information is lost/assumed.");
      } else if (value instanceof java.util.Date) {
        log.debug("Inferring java.util.Date as Timestamp schema.");
      }
      return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
    }
    if (value instanceof LocalDate) return org.apache.kafka.connect.data.Date.builder().optional().build();
    if (value instanceof LocalTime) return org.apache.kafka.connect.data.Time.builder().optional().build();
    if (value instanceof UUID) {
      log.debug("Inferring UUID as String schema.");
      return Schema.OPTIONAL_STRING_SCHEMA; // Standard practice to store UUIDs as Strings in Connect
    }

    // Collections (Basic Recursive Inference)
    if (value instanceof List) {
      List<?> list = (List<?>) value;
      Schema elementSchema = inferCollectionElementSchema(list);
      return SchemaBuilder.array(elementSchema).optional().build();
    }
    if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      Schema keySchema = Schema.OPTIONAL_STRING_SCHEMA; // Default keys to String
      Schema valueSchema = inferCollectionElementSchema(map.values());
      // Attempt to infer key type if possible (less common)
      Optional<?> firstKey = map.keySet().stream().filter(Objects::nonNull).findFirst();
      if (firstKey.isPresent()) {
        try {
          keySchema = inferSchema(firstKey.get());
        } catch (Exception e) {
          log.warn("Could not infer Map key type from first key [{}], defaulting to String.", firstKey.get(), e);
        }
      } else if (!map.isEmpty()) {
        log.warn("Map keys are all null or map is non-empty but couldn't get first key, defaulting key schema to String.");
      }
      return SchemaBuilder.map(keySchema, valueSchema).optional().build();
    }

    // Fallback for unknown types
    throw new org.apache.kafka.connect.errors.DataException("Unsupported type for schema inference: " + value.getClass().getName());
  }

  /**
   * Helper to infer schema for elements within a collection (List or Map values)
   */
  private Schema inferCollectionElementSchema(Collection<?> collection) {
    // Infer from the first non-null element
    Optional<?> firstNonNull = collection.stream().filter(Objects::nonNull).findFirst();
    if (firstNonNull.isPresent()) {
      try {
        return inferSchema(firstNonNull.get());
      } catch (Exception e) {
        log.warn("Could not infer collection element type from first element [{}], defaulting to String.", firstNonNull.get(), e);
        return Schema.OPTIONAL_STRING_SCHEMA;
      }
    } else {
      // All elements are null or collection is empty
      log.warn("Inferring schema for collection with no non-null elements as OPTIONAL_STRING_SCHEMA.");
      return Schema.OPTIONAL_STRING_SCHEMA;
    }
  }

  /**
   * Prepares the value to be put into the Struct, potentially converting types
   * to match the inferred schema (e.g., byte[] to ByteBuffer, UUID to String, time types).
   */
  private Object prepareValueForSchema(Object value, Schema fieldSchema) {
    if (value == null) {
      return null;
    }
    Schema.Type schemaType = fieldSchema.type();
    String logicalTypeName = fieldSchema.name();

    // --- Type Conversions ---
    if (schemaType == Schema.Type.BYTES && value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    }
    if (schemaType == Schema.Type.STRING && value instanceof UUID) {
      return value.toString();
    }

    // --- Logical Type Conversions (to expected Connect representation) ---
    if (logicalTypeName != null) {
      switch (logicalTypeName) {
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          if (value instanceof Instant) return java.util.Date.from((Instant) value);
          if (value instanceof LocalDateTime) {
            // Lossy conversion assuming UTC
            return java.util.Date.from(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
          }
          // Keep java.util.Date as is
          break; // Important break
        case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
          if (value instanceof LocalDate) {
            // Date expects days since epoch as Integer
            return (int) ((LocalDate) value).toEpochDay();
          }
          break; // Important break
        case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
          if (value instanceof LocalTime) {
            // Time expects millis since midnight as Integer
            // Use long division to avoid potential overflow before casting
            return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000L);
          }
          break; // Important break
        // Decimal usually expects BigDecimal, no conversion needed here typically
      }
    }

    // TODO: Add recursive preparation for List/Map elements if necessary for complex types

    return value; // Return original value if no specific conversion needed
  }

  public EmbeddedEngineChangeEvent buildChangeEvent(Struct key, String destination) {
    Struct value = build();
    return EventFactory.createMockChangeEvent(key, value, destination);
  }
}