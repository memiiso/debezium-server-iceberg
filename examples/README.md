# Examples

## Verification
### PG/TSDB
Connect to the Postgres/Timescale source instance.
```bash
psql postgres://postgres:postgres@localhost
```

Fetch all of the data from the orders table.
```sql
select * from inventory.orders;
```

### Spark-SQL
Connect to the spark-sql client.
```bash
docker exec -it spark-iceberg spark-sql
```

Fetch all of the data from the orders table according to the latest snapshot.
```sql
select * from icebergdata.debeziumcdc_dbz__inventory_orders;
```
