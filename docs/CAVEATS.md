# Caveats
## Only iceberg V2 table supported
This connector only writes using iceberg table V2 spec (delete events will be written to delete files instead of rewrite data files)

## No automatic schema evolution
Currently, there is no handler to detect schema changes and auto evolve the schema. Schema change events can make the connector throw error. To workaround this, turn off schema change event in `source` setting.

- For SQL Server, set `debezium.source.include.schema.changes=false`

## Specific tables replication
By default, debezium connector will publish all snapshot of the tables in the database, that leads to unnessesary iceberg table snapshot of all tables. Unless you want to replicate all table from the database into iceberg table, set `debezium.source.table.include.list` to specific tables that you want to replicate. By this way, you avoid replicate too many tables that you don't really want to. More on this setting in [Debezium server source](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-table-include-list).

## AWS S3 credentials
You can setup aws credentials in the following ways:
- Option 1: use `debezium.sink.iceberg.fs.s3a.access.key` and `debezium.sink.iceberg.fs.s3a.secret.key` in `application.properties`
- Option 2: inject credentials to environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`
- Option 3: setup proper `HADOOP_HOME` env then add s3a configuration into `core-site.xml`, more information can be found [here](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3).