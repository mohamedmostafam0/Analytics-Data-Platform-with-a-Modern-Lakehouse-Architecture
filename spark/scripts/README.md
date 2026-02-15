# Spark ETL Scripts

This directory contains the PySpark scripts that define the ETL pipeline. The pipeline follows a **Medallion Architecture** (Bronze -> Silver -> Gold).

## 📂 Scripts

| Script | Layer | Source | Destination | Description |
| :--- | :--- | :--- | :--- | :--- |
| `minio_loader.py` | Bronze | MinIO (JSON) | Iceberg (`bronze.pageviews`) | Ingests raw pageview events using **Structured Streaming** (AvailableNow trigger) for idempotency. |
| `postgres_loader.py` | Bronze | Postgres (JDBC) | Iceberg (`bronze.users`, etc.) | Ingests transactional data. Uses **JDBC Partitioning** for parallel reads and **Watermarking** for incremental loading. |
| `bronze_to_silver_transformer.py` | Silver | Bronze Tables | Iceberg Silver Tables | Cleans, deduplicates, and enriches data (e.g., joins purchases with user/item details). |
| `etl_utils.py` | - | - | - | Shared utility functions (Spark session creation, common writers). |

## 🏃 Usage

All scripts are executed inside the `spark-iceberg` container.

### 1. Ingest Bronze Data

```bash
# Ingest from MinIO
docker exec spark-iceberg spark-submit /home/iceberg/scripts/minio_loader.py

# Ingest from Postgres
docker exec spark-iceberg spark-submit /home/iceberg/scripts/postgres_loader.py
```

### 2. Transform to Silver

```bash
docker exec spark-iceberg spark-submit /home/iceberg/scripts/bronze_to_silver_transformer.py
```

## 🛠️ Development

- **Logging**: All scripts use Python's `logging` module. Avoid `print()` statements.
- **Credentials**: Credentials are secure and loaded from environment variables.
