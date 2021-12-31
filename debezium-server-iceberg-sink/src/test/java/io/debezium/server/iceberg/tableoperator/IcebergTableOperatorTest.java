/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergChangeEvent;
import io.debezium.server.iceberg.testresources.BaseSparkTest;
import io.debezium.server.iceberg.testresources.IcebergChangeEventBuilder;
import io.quarkus.test.junit.QuarkusTest;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
class IcebergTableOperatorTest extends BaseSparkTest {

  @Test
  public void testIcebergChangeEventBuilder() {

    IcebergChangeEventBuilder b = new IcebergChangeEventBuilder();
    IcebergChangeEvent t = b.
        addKeyField("id", 1)
        .addField("data", "testdatavalue")
        .addField("preferences", "feature1", true)
        .addField("preferences", "feature2", true)
        .build();
    System.out.println(t.jsonSchema().keySchema());

    b = new IcebergChangeEventBuilder();
    b.addField("id", 1);
    b.addField("preferences", "feature1", true);
    b.addField("preferences", "feature2", true);

    Schema schema1 = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "preferences", Types.StructType.of(
            required(3, "feature1", Types.BooleanType.get()),
            optional(4, "feature2", Types.BooleanType.get())
        ))
    );

    System.out.println(b.build().icebergSchema());
  }
}