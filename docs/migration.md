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

2.  **Adjust the Table Schema**

    You have two primary options to resolve the schema mismatch. Choose the one that best fits your table size and operational requirements.

    **Option 1: Rewrite the Table (for small tables)**

    If your table is small, you can rewrite its entire contents while converting the problematic column to the new data type. This approach avoids having separate columns for old and new data but can be very expensive for large tables.

    ⚠️ **Warning:** This operation rewrites the entire table and can be very slow and costly. It is generally not recommended for large production tables.

    Using Spark SQL, you can replace the table with the result of a query. The new table schema will be inferred from the `SELECT` statement.

    ```sql
    -- Make sure to include ALL columns from the original table to avoid data loss.
    INSERT OVERWRITE my_catalog.my_db.my_table
    SELECT
      id,
      -- other_column_1,
      -- other_column_2,
      timestamp_millis(order_created_ts_ms) AS order_created_ts_ms
    FROM my_catalog.my_db.my_table;
    ```

    **Option 2: Rename the Column (Recommended for large tables)**

    This is the **recommended approach for most production scenarios**. Renaming a column is a fast, metadata-only operation that does not require rewriting any data files. It is nearly instantaneous, making it ideal for large tables.

    You can use any tool that supports Iceberg table management, such as Spark, Flink, or the Iceberg REST catalog API.

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
SELECT COALESCE(timestamp_millis(order_created_ts_ms_legacy), order_created_ts_ms) AS order_created_ts_ms FROM my_catalog.my_db.my_table
```
