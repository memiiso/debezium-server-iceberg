package io.debezium.server.iceberg;

import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IcebergUtilTest {

  @Test
  void testCreatePartitionSpecValid() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    PartitionSpec spec = IcebergUtil.createPartitionSpec(schema, List.of("id"));
    Assertions.assertTrue(spec.isPartitioned());
    Assertions.assertEquals("id", spec.fields().get(0).name());
  }

  @Test
  void testCreatePartitionSpecMissingColumnThrowsException() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergUtil.createPartitionSpec(schema, List.of("missing_column"), true));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "Partition field 'missing_column' (source column 'missing_column') not found in schema."));
  }

  @Test
  void testCreatePartitionSpecMissingColumnLenientMode() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    PartitionSpec spec = IcebergUtil.createPartitionSpec(schema, List.of("missing_column"), false);
    Assertions.assertFalse(spec.isPartitioned());
  }
}
