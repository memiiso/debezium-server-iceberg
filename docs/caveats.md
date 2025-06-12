# Caveats

## Equality Delete Performance
This connector writes data to Iceberg tables using the V2 specification. To optimize write performance, delete events are recorded in delete files, avoiding costly data file rewrites. While this approach significantly improves write performance, it can impact read performance, especially in `upsert` mode. However, in `append` mode, this performance trade-off is not applicable.

To optimize read performance end user must run periodic table maintenance job. do data compaction and rewrite the delete
filed. This is especially critical for `upsert` mode.

## Schema Evolution (Only Schema Expansion is supported)

Full schema evolution, such as converting incompatible data types, is not currently supported. However, **schema
expansion**, including adding new fields or expanding existing field data types, is supported. To enable this behavior,
set the
`debezium.sink.iceberg.allow-field-addition` configuration property to `true`.

To seamlessly absorb schema changes, consider using this setting. All nested data will be stored in a variant fields.

```properties
# Store nested data in variant fields
debezium.sink.iceberg.nested-as-variant=true
# Ensure event flattening is disabled (flattening is the default behavior)
debezium.transforms=,
```

## Specific tables replication
By default, the Debezium connector will replicate all the tables in the database, resulting in unnecessary load. To avoid replicating tables you don't need, configure the `debezium.source.table.include.list` property to specify the exact tables to replicate. This will streamline your data pipeline and reduce the overhead. For more details on this configuration, refer to the [Debezium server source](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-table-include-list) documentation.

## AWS S3 credentials

You can setup aws credentials in the following ways:

- Option 1: use `debezium.sink.iceberg.fs.s3a.access.key` and `debezium.sink.iceberg.fs.s3a.secret.key`
  in `application.properties`
- Option 2: inject credentials to environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`
- Option 3: setup proper `HADOOP_HOME` env then add s3a configuration into `core-site.xml`, more information can be
  found [here](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3).