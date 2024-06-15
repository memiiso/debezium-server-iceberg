package io.debezium.server.iceberg;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class IcebergUtilTest {


  @Test
  void configIncludesUnwrapSmt() {
    // mongodb transforms
    String mongoConfVal = "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState";
    SmallRyeConfig mongodbConf = new SmallRyeConfigBuilder().
        withProfile("mongodb").
        withDefaultValue("debezium.transforms", "unwrap,someothersmt").
        withDefaultValue("%mongodb.debezium.transforms.unwrap.type", mongoConfVal).
        build();
    assertEquals(mongodbConf.getConfigValue("debezium.transforms.unwrap.type").getValue(), mongoConfVal);
    assertTrue(IcebergUtil.configIncludesUnwrapSmt(mongodbConf));

    String defaultConfVal = "io.debezium.transforms.ExtractNewRecordState";
    SmallRyeConfig defaultConf = new SmallRyeConfigBuilder().
        withProfile("mysql").
        withDefaultValue("%mysql.debezium.transforms", "unwrapdata,someothersmt").
        withDefaultValue("%mysql.debezium.transforms.unwrapdata.type", defaultConfVal).
        build();
    assertEquals(defaultConf.getConfigValue("debezium.transforms.unwrapdata.type").getValue(), defaultConfVal);
    assertTrue(IcebergUtil.configIncludesUnwrapSmt(defaultConf));


    SmallRyeConfig testConf = new SmallRyeConfigBuilder().
        withProfile("testing").
        withDefaultValue("%testing.test.property", "test-Value").
        build();
    assertEquals(testConf.getConfigValue("test.property").getValue(), "test-Value");
    assertFalse(IcebergUtil.configIncludesUnwrapSmt(testConf));
  }

}