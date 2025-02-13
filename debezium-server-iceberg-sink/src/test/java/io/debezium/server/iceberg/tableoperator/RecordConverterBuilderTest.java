/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.RecordConverter;
import io.debezium.server.iceberg.testresources.CatalogJdbc;
import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;

import java.util.List;
import java.util.Set;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = CatalogJdbc.class, restrictToAnnotatedClass = true)
class RecordConverterBuilderTest {

  @Test
  public void testIcebergChangeEventBuilder() {
    Schema schema1 = new Schema(
        List.of(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "preferences", Types.StructType.of(
                optional(4, "feature1", Types.BooleanType.get()),
                optional(5, "feature2", Types.BooleanType.get())
            ))
        )
        , Set.of(1)
    );

    IcebergChangeEventBuilder b = new IcebergChangeEventBuilder();
    RecordConverter t = b.
        addKeyField("id", 1)
        .addField("data", "testdatavalue")
        .addField("preferences", "feature1", true)
        .addField("preferences", "feature2", true)
        .build();
    Assertions.assertTrue(schema1.sameSchema(t.icebergSchema(true)));

    Schema schema2 = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            optional(4, "feature1", Types.BooleanType.get()),
            optional(5, "feature2", Types.BooleanType.get())
        ))
    );

    b = new IcebergChangeEventBuilder();
    t = b.
        addField("id", 1)
        .addField("data", "testdatavalue")
        .addField("preferences", "feature1", true)
        .addField("preferences", "feature2", true)
        .build();
    Assertions.assertTrue(schema2.sameSchema(t.icebergSchema(true)));
  }


}