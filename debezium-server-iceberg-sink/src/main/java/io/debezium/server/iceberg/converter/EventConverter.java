package io.debezium.server.iceberg.converter;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;

/**
 * Interface for converting CDC events from various formats (e.g., Json, Debezium Connect format)
 * into Iceberg records and extracting relevant metadata.
 */
public interface EventConverter {

  /**
   * Extracts the key part of the CDC event.
   * The actual type depends on the source event format (e.g., Struct, String).
   *
   * @param <T> The expected type of the key.
   * @return The event key, or potentially null if the event has no key.
   */
  @Nullable
  <T> T key();

  /**
   * Checks if the event contains key data. Useful for distinguishing
   * events with explicit null keys from events without keys.
   *
   * @return true if key data is present, false otherwise.
   */
  boolean hasKeyData();


  /**
   * Extracts the value/payload part of the CDC event.
   * The actual type depends on the source event format (e.g., Struct, String).
   *
   * @param <T> The expected type of the value.
   * @return The event value, or null for delete events (tombstones).
   */
  @Nullable
  <T> T value();

  /**
   * Extracts the CDC operation type (Create, Update, Delete, Read).
   *
   * @return The {@link Operation} enum value.
   */
  @NotNull
  Operation cdcOpValue();

  /**
   * True if the first operation for a key in a batch is an insert
   */
  boolean isNewKey();
  void setNewKey(boolean newKey);

  /**
   * Provides a converter capable of transforming the event's schema representation
   * into an Iceberg {@link Schema}.
   *
   * @return The schema converter instance.
   */
  @NotNull
  SchemaConverter schemaConverter();

  /**
   * Indicates whether this event represents a schema change event rather than a data change event.
   *
   * @return true if it's a schema change event, false otherwise.
   */
  boolean isSchemaChangeEvent();

  /**
   * Gets the Iceberg {@link Schema} that corresponds to the data payload (`value()`)
   * of this specific event, potentially derived from schema information embedded within the event.
   * This might differ from the target table's schema if schema evolution is occurring.
   * Returns null if the event is a schema change event or has no associated data schema.
   *
   * @return The Iceberg schema for the event's data, or null.
   */
  @Nullable
  Schema icebergSchema(boolean withIdentifierFields);

  default Schema icebergSchema() {
    return icebergSchema(true);
  }

  /**
   * Gets the Iceberg {@link SortOrder} that corresponds to the data key of this specific event.
   * @param schema The Iceberg schema for {@link SortOrder.Builder}.
   * @return The Iceberg {@link SortOrder}.
   */
  @Nullable
  SortOrder sortOrder(Schema schema);

  /**
   * Gets the destination identifier (e.g., logical table name) for this event.
   *
   * @return The destination string.
   */
  @NotNull
  String destination();

  /**
   * Converts the event data into a {@link RecordWrapper} suitable for direct append operations,
   * using the provided target Iceberg schema. This might optimize by only including necessary fields
   * for an append (e.g., the 'after' state).
   *
   * @param schema The target Iceberg schema to conform to.
   * @return A {@link RecordWrapper} containing the data formatted for appending.
   */
  @NotNull
  RecordWrapper convertAsAppend(@NotNull Schema schema); // Added @NotNull

  /**
   * Converts the event data into a {@link RecordWrapper} suitable for general iceberg consumption
   * (Create, Update, Delete), using the provided target Iceberg schema.
   *
   * @param schema The target Iceberg schema to conform to.
   * @return A {@link RecordWrapper} containing the data formatted for iceberg table.
   */
  @NotNull
  RecordWrapper convert(@NotNull Schema schema); // Added @NotNull
}
