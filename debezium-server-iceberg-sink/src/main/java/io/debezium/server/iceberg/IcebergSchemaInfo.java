package io.debezium.server.iceberg;

import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A record class (Java 14+) representing schema data for Iceberg records.
 * This class helps manage the fields, identifier fields, and the next available
 * field ID when building Iceberg schemas.
 *
 * @param fields             A list of `Types.NestedField` objects representing the fields
 *                             in the schema.  `Types.NestedField` contains information about
 *                             the field's ID, name, type, and nullability.
 * @param identifierFieldIds A set of integer IDs that identify the fields that are
 *                             part of the record's key or identifier.
 * @param nextFieldId        An `AtomicInteger` that keeps track of the next available
 *                             field ID to ensure unique IDs are assigned to new fields.  Using
 *                             an `AtomicInteger` makes this class thread-safe.
 */

public record IcebergSchemaInfo(List<Types.NestedField> fields, Set<Integer> identifierFieldIds,
                         AtomicInteger nextFieldId) {

  /**
   * Constructor for `IcebergSchemaInfo` that initializes the `fields` list and
   * `identifierFieldIds` set to empty and sets the `nextFieldId` to the provided
   * value.
   *
   * @param nextFieldId The starting ID to use for new fields.
   */
  public IcebergSchemaInfo(Integer nextFieldId) {
    this(new ArrayList<>(), new HashSet<>(), new AtomicInteger(nextFieldId));
  }

  /**
   * Default constructor for `IcebergSchemaInfo` that initializes the `fields`
   * list and `identifierFieldIds` set to empty and sets the `nextFieldId` to 1.
   */
  public IcebergSchemaInfo() {
    this(1);
  }

  /**
   * Creates a copy of this `IcebergSchemaInfo` object, but *keeps* the original's
   * `identifierFieldIds` and `nextFieldId`.  This is useful when you want to
   * create a new schema builder based on an existing one but need to preserve
   * the identifier field information and the next field ID counter.  The `fields`
   * list is initialized as a new empty list in the copy.
   *
   * @return A new `IcebergSchemaInfo` object with the same identifier fields and
   *         next field ID, but an empty fields list.
   */
  public IcebergSchemaInfo copyPreservingMetadata() {
    return new IcebergSchemaInfo(new ArrayList<>(), this.identifierFieldIds, this.nextFieldId);
  }
}