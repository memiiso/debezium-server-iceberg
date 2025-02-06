# Examples
## Build | External Project Changes
In order to iterate quick on making changes to external projects (such as iceberg-core), it is often preferable to make changes to the repo on your workstation, and then integrate those changes into a docker build for testing/verification.

In order to accomplish this, a few important steps are needed:
1. Build the external project ensuring that its artifacts are in Maven form, and in your local ~/.m2 cache.
2. Update the Dockerfile in this repo to mount your local m2 cache for any steps which need access to the artifacts. This will typically look something like: `RUN --mount=from=m2,rw,target=/path/to/your/.m2,dst=/root/.m2 mvn package ...`.
3. Lastly, ensure that the docker build invocation adds a build context for your m2 cache: `docker build -t imageTag:foo --build-context m2=$HOME/.m2 .`.
    - This declares a named build context `m2`.
    - The Dockerfile uses this named context in the mount instruction `RUN --mount=from=m2,...`.

## Verification
### PG/TSDB
```bash
# Connect to the Postgres/Timescale source instance.
psql postgres://postgres:postgres@localhost
```

```sql
-- Fetch all data from the orders table.
select * from inventory.orders;

-- Setup a new hypertable.
CREATE TABLE conditions (time TIMESTAMPTZ NOT NULL, location TEXT NOT NULL, temperature DOUBLE PRECISION NULL, humidity DOUBLE PRECISION NULL);
SELECT create_hypertable('conditions', 'time');
INSERT INTO conditions VALUES(NOW(), 'San Antonio', 22.8,  53.3);
```

### Spark-SQL
Connect to the spark-sql client.
```bash
```

Fetch all data from the orders table according to the latest snapshot.
```sql
select * from icebergdata.debeziumcdc_dbz__inventory_orders;
select * from icebergdata.debeziumcdc_dbz___timescaledb_internal_bgw_job_stat;
select * from icebergdata.debeziumcdc_dbz___timescaledb_internal__hyper_1_1_chunk;
```
