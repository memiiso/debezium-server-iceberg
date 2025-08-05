# Migration Guide

This document provides guidance on upgrading the Debezium Iceberg consumer and handling potential migration challenges.

## General Upgrade Process

Please be aware that each release may include backward-incompatible changes. Thorough testing in a staging environment is strongly recommended before deploying any new version to production.

If you encounter any issues not covered here, please feel free to report them as GitHub Issues.

## Handling Incompatible Data Type Changes

An incompatible data type change can occur in two main scenarios:
1.  **Upgrading the Connector:** A newer version of Debezium might improve how it handles certain data types. For example, it might change its representation of timestamps from a `long` (epoch milliseconds) to a logical `timestamp` type.
2.  **Source Database Schema Change:** The schema in your source database might change in a way that results in an incompatible type change in the Debezium event.

In either case, the Debezium Iceberg consumer will fail to write the new data and log an error similar to this:

```
java.lang.IllegalArgumentException: Cannot change column type: order_created_ts_ms: long -> timestamp
```

To handle such a change, you need to perform a manual migration step on your Iceberg table. The strategy is to rename the old column, allowing the consumer to create a new column with the correct type for incoming data.

### Migration Steps

Let's use the example of a column `order_created_ts_ms` changing from `long` to `timestamp`. Migrating consumer from 0.8.x to 0.9.x.

1.  **Stop the Debezium Server** to prevent further write attempts.

2.  **Rename the existing column** in your Iceberg table. **This is a fast, metadata-only operation**. You can use any tool that can manage Iceberg tables, such as Spark, Flink, or the Iceberg REST catalog API.

    Using Spark SQL:
    ```sql
    ALTER TABLE my_catalog.my_db.my_table RENAME COLUMN order_created_ts_ms TO order_created_ts_ms_legacy;
    ```

3.  **Upgrade and Restart the Debezium Server**.

### What Happens Next?

When the consumer processes the new events, it will find that the `order_created_ts_ms` column no longer exists. It will then add it to the table schema as a new column with the correct `timestamp` type.

After this process, your table will have both columns:
- `order_created_ts_ms_legacy` (`long`): Contains the old data. New rows will have `null` in this column.
- `order_created_ts_ms` (`timestamp`): Contains the new data. Old rows will have `null` in this column.

This approach preserves all your data while allowing the schema to evolve to accommodate the new data type. You can later decide to backfill the data and consolidate it into a single column if needed.

or you can simply could use COALESCE and read consolidated data
```sql
SLEECT COALESCE(rom_unixtime(order_created_ts_ms_legacy), order_created_ts_ms) AS order_created_ts_ms FROM MY_TABLE
```
