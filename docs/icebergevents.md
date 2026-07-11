# DEPRECATED

Using the `iceberg` consumer with the following settings is recommended to achieve the same results:

```properties
# Store nested data in variant fields
debezium.sink.iceberg.nested-as-variant=true
# Ensure event flattening is disabled (flattening is the default behavior)
debezium.transforms=,
```

# `icebergevents` Consumer

This consumer appends all Change Data Capture (CDC) events as JSON strings to a single Iceberg table. The table is
partitioned by `event_destination` and `event_sink_timestamptz` for efficient data organization and query performance.

````properties
debezium.sink.type=icebergevents
debezium.sink.iceberg.catalog-name=default
````

Iceberg table definition:

```java
static final String TABLE_NAME = "debezium_events";
static final Schema TABLE_SCHEMA =
    new Schema(
        required(1, "event_destination", Types.StringType.get()),
        optional(2, "event_key_schema", Types.StringType.get()),
        optional(3, "event_key_payload", Types.StringType.get()),
        optional(4, "event_value_schema", Types.StringType.get()),
        optional(5, "event_value_payload", Types.StringType.get()),
        optional(6, "event_sink_epoch_ms", Types.LongType.get()),
        optional(7, "event_sink_timestamptz", Types.TimestampType.withZone())
);
static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA)
        .identity("event_destination")
        .hour("event_sink_timestamptz")
        .build();
static final SortOrder TABLE_SORT_ORDER = SortOrder.builderFor(TABLE_SCHEMA)
        .asc("event_sink_epoch_ms", NullOrder.NULLS_LAST)
        .asc("event_sink_timestamptz", NullOrder.NULLS_LAST)
        .build();
```
