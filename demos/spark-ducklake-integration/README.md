# Spark + DuckLake Integration Demo

This demo shows how to integrate **DuckLake** with **PySpark** using **DuckDB** as the compute engine.  
It demonstrates a hybrid workflow where Spark interacts with data stored in DuckLake via the DuckDB JDBC driver.

---

## Overview

DuckLake provides a modern **lakehouse abstraction** on top of object storage and metadata management.  
In this demo, we use:

- **PostgreSQL** → metadata store for DuckLake  
- **MinIO (S3-compatible)** → object storage backend  
- **DuckDB** → embedded compute engine used by both DuckLake and PySpark  
- **Spark (PySpark)** → distributed compute layer using DuckDB as its JDBC data source

The integration flow:

> PySpark APP → DuckDB JDBC Driver → JNI → DuckDB (native lib) → DuckLake extension → data files on S3 (MinIO)

(Rref: https://github.com/duckdb/ducklake/issues/154)

---

## Features Demonstrated

1. Use **Python DuckDB** to convert a raw Parquet file into a DuckLake table.  
2. Use **SparkSQL** to query the DuckLake table and perform analytics.  
3. Use **PySpark Dataframe** API to write the result as a **new DuckLake table**.  
4. Use **PySpark Dataframe** API to verify and inspect the output.

---

## Requirements

| Component | Version |
| --------- | ------- |
| Python    | 3.13    |
| DuckDB    | 1.3.2   |
| PySpark   | 3.5.1   |

All services (Spark, Postgres, MinIO) are provided through **Docker** and managed with [docker-compose](docker/docker-compose.yml).

---

## Environment Setup

The environment includes:

- **PostgreSQL** (for DuckLake metadata)
- **MinIO** (as object storage)
- **Spark Master & Worker**
- **DuckDB-based PySpark compute engine**

---

## Source Data

The demo uses the **NYC Taxi Trip Data** from the official NYC TLC dataset:
🔗 [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## Run Locally

Follow these steps to run the demo end-to-end on your machine.

### 1. Prepare data (required)

1. Download `yellow_tripdata_2025-09.parquet` from the NYC TLC Trip Record Data page:  
   https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Place it under the local directory: [data/](data/)
(The docker-compose setup will upload this file to MinIO)

### 2. Start services
Start Postgres, MinIO, Spark master, and Spark worker:
```bash
docker compose up -d --build
```
This will:
- Create the bucket: `odc-bucket` in MinIO (if not present)
- Upload `yellow_tripdata_2025-09.parquet` to `odc-bucket`
- Initialize `odc_db` in Postgres for DuckLake metadata
- Launch one Spark master and one Spark worker

### 3. Run the demo script (inside Spark master)
Open a shell in the Spark master container:

```bash
docker exec -it spark-master bash
```

Run the demo with spark-submit:
```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars/duckdb_jdbc-1.3.2.0.jar \
  /opt/app/src/spark_ducklake_demo.py
```
- The demo source is mounted at /opt/app/src.
- --deploy-mode client is used for easier debugging.

### 4. Verify results
The script will:
- Convert the parquet file into a DuckLake table (stored in MinIO)
- Query the table using SparkSQL and perform analytics
- Write analytics results back into DuckLake as a new table
- Use PySpark DataFrame API to load and print sample results for verification

Note:
- Check the Spark master logs or terminal output to see the printed results and status messages.
- You can also open the MinIO Console to browse DuckLake storage at: http://localhost:9001/browser/odc-bucket
  - Here you can visually verify uploaded Parquet files and generated DuckLake tables.

### 5. Shutdown the Environment
```bash
docker compose down -v
```

## Reference
https://motherduck.com/blog/spark-ducklake-getting-started/
