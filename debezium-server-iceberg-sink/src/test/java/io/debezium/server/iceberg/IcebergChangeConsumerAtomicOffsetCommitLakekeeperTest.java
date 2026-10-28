/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.server.iceberg.testresources.CatalogLakekeeper;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

/** Atomic offset commit with the Lakekeeper catalog. */
@QuarkusTest
@QuarkusTestResource(value = CatalogLakekeeper.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(BaseAtomicOffsetCommitTest.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class IcebergChangeConsumerAtomicOffsetCommitLakekeeperTest
    extends BaseAtomicOffsetCommitTest {}
