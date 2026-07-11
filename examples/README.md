# Debezium Server Iceberg Examples

This directory contains configuration templates and Docker Compose environments for setting up and testing the Debezium Server Iceberg Consumer with different catalog backends.

## Available Examples

### 1. [Nessie Catalog](nessie/)
An end-to-end environment running Debezium Server with the Project Nessie catalog, MinIO for S3 storage, and a PostgreSQL source database.
- **Components**: PostgreSQL, Debezium Server Iceberg, Project Nessie, MinIO
- **Catalog Type**: `nessie`

### 2. [Lakekeeper REST Catalog](lakekeeper/)
An end-to-end environment running Debezium Server with the Lakekeeper REST catalog, MinIO for S3 storage, a PostgreSQL source database, and a Jupyter/PySpark notebook for querying data.
- **Components**: PostgreSQL, Debezium Server Iceberg, Lakekeeper, MinIO, Jupyter/PySpark
- **Catalog Type**: `rest`

---

## How to Run the Examples

### Step 1: Build the Debezium Server Distribution
Before spinning up the Docker Compose files, make sure the local Debezium Server runner package is built:
```bash
# Build the project distribution
mvn clean package -Passembly -Dmaven.test.skip=true -Drevision=latest
```

### Step 2: Spin Up the Stack
Navigate to your desired catalog example folder (e.g., `nessie` or `lakekeeper`) and start the docker compose setup:
```bash
cd examples/nessie
docker compose up -d
```

### Step 3: Run the Data Generator
Both examples contain a `produce_data.py` helper script that continuously writes fake records to the source PostgreSQL database to simulate live CDC events.

To run the script, first install the Python dependencies:
```bash
pip install psycopg2-binary faker
```
Then, execute the script:
```bash
python produce_data.py
```
This will insert customer and order records every 10 seconds. Debezium Server will automatically capture these changes and replicate them directly into your Iceberg tables stored in MinIO.
