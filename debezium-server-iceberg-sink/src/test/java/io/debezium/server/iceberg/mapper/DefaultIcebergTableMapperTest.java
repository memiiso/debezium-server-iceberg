package io.debezium.server.iceberg.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergConfig;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultIcebergTableMapperTest {

  private GlobalConfig globalConfig;
  private IcebergConfig icebergConfig;
  private DefaultIcebergTableMapper mapper;

  @BeforeEach
  void setUp() {
    globalConfig = mock(GlobalConfig.class);
    icebergConfig = mock(IcebergConfig.class);
    when(globalConfig.iceberg()).thenReturn(icebergConfig);

    when(icebergConfig.namespace()).thenReturn("my_namespace");
    when(icebergConfig.destinationRegexp()).thenReturn(Optional.empty());
    when(icebergConfig.destinationRegexpReplace()).thenReturn(Optional.empty());
    when(icebergConfig.tablePrefix()).thenReturn(Optional.empty());
    when(icebergConfig.destinationUppercaseTableNames()).thenReturn(false);
    when(icebergConfig.destinationLowercaseTableNames()).thenReturn(false);

    mapper = new DefaultIcebergTableMapper();
    mapper.config = globalConfig;
  }

  @Test
  void testDefaultMapping() {
    TableIdentifier tableId = mapper.mapDestination("inventory.customers");
    assertEquals("my_namespace", tableId.namespace().toString());
    assertEquals("inventory_customers", tableId.name());
  }

  @Test
  void testMappingWithRegexAndReplacement() {
    when(icebergConfig.destinationRegexp()).thenReturn(Optional.of("server1\\.(.*)"));
    when(icebergConfig.destinationRegexpReplace()).thenReturn(Optional.of("$1"));

    TableIdentifier tableId = mapper.mapDestination("server1.inventory.customers");
    assertEquals("inventory_customers", tableId.name());
  }

  @Test
  void testMappingWithEmptyRegex() {
    when(icebergConfig.destinationRegexp()).thenReturn(Optional.of(""));
    when(icebergConfig.destinationRegexpReplace()).thenReturn(Optional.of("ignored"));

    TableIdentifier tableId = mapper.mapDestination("inventory.customers");
    assertEquals("inventory_customers", tableId.name());
  }

  @Test
  void testMappingWithPrefix() {
    when(icebergConfig.tablePrefix()).thenReturn(Optional.of("cdc_"));

    TableIdentifier tableId = mapper.mapDestination("inventory.customers");
    assertEquals("cdc_inventory_customers", tableId.name());
  }

  @Test
  void testMappingWithUppercase() {
    when(icebergConfig.destinationUppercaseTableNames()).thenReturn(true);
    when(icebergConfig.tablePrefix()).thenReturn(Optional.of("cdc_"));

    TableIdentifier tableId = mapper.mapDestination("inventory.customers");
    assertEquals("CDC_INVENTORY_CUSTOMERS", tableId.name());
  }

  @Test
  void testMappingWithLowercase() {
    when(icebergConfig.destinationLowercaseTableNames()).thenReturn(true);

    TableIdentifier tableId = mapper.mapDestination("Inventory.CUSTOMERS");
    assertEquals("inventory_customers", tableId.name());
  }
}
