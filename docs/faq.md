# Frequently Asked Questions (FAQ)

???+ question "How does the connector handle deletes, and what are the performance implications?"

    This connector writes data to Iceberg tables using the V2 specification. To optimize write performance, delete events are recorded in delete files, avoiding costly data file rewrites. While this approach significantly improves write performance, it can impact read performance, especially in `upsert` mode. In `append` mode, this performance trade-off is not applicable.
    
    To optimize read performance, you must run periodic table maintenance jobs to compact data and rewrite the delete files. This is especially critical for `upsert` mode.

???+ question "Does the connector support schema evolution?"

    Full schema evolution, such as converting incompatible data types, is not currently supported. However, **schema expansion**—including adding new fields or promoting field data types—is supported. To enable this behavior, set the `debezium.sink.iceberg.allow-field-addition` configuration property to `true`.
    
    For a more robust way to handle schema changes, you can configure the connector to store all nested data in a `variant` field. This approach can seamlessly absorb many schema changes.

    ```properties
    # Store nested data in variant fields
    debezium.sink.iceberg.nested-as-variant=true
    # Ensure event flattening is disabled (flattening is the default behavior)
    debezium.transforms=,
    ```

???+ question "How can I replicate only specific tables from my source database?"

    By default, the Debezium connector replicates all tables in the database, which can result in unnecessary load. To avoid replicating tables you don't need, configure the `debezium.source.table.include.list` property to specify the exact tables to replicate. This will streamline your data pipeline and reduce overhead. For more details, refer to the [Debezium server source](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-table-include-list documentation.

???+ question "How do I configure AWS S3 credentials?"

    You can set up AWS credentials in one of the following ways:
    
    - **In `application.properties`**: Use the `debezium.sink.iceberg.fs.s3a.access.key` and `debezium.sink.iceberg.fs.s3a.secret.key` properties.
      - **As environment variables**: Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
      - **Using Hadoop's configuration**: Set up the `HADOOP_HOME` environment variable and add S3A configuration to `core-site.xml`. More information can be found [here](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3).
