# Spark ETL Scripts

This directory contains the PySpark scripts that define the ETL pipeline. The pipeline follows a **Medallion Architecture** (Bronze → Silver → Gold).

## 📂 Structure

```
scripts/
├── sql/                              # Iceberg DDL (per-layer)
│   ├── bronze_schema.sql
│   ├── silver_schema.sql
│   └── gold_schema.sql
├── config.py                         # Centralized configuration (dataclass)
├── etl_utils.py                      # Shared utilities (Spark session, writers)
├── minio_loader.py                   # Bronze: MinIO JSON → Iceberg
├── postgres_loader.py                # Bronze: Postgres JDBC → Iceberg
├── bronze_to_silver_transformer.py   # Silver: clean, enrich, deduplicate
├── silver_to_gold_transformer.py     # Gold: aggregate analytical tables
└── tests/                            # Automated tests
```

## 📋 Scripts

| Script | Layer | Source → Destination | Description |
| :--- | :--- | :--- | :--- |
| `minio_loader.py` | Bronze | MinIO (JSON) → `bronze.pageviews` | Ingests raw pageview events using Structured Streaming. Malformed records go to `bronze.pageviews_dlq`. |
| `postgres_loader.py` | Bronze | Postgres → `bronze.users`, `items`, `purchases` | JDBC partitioned reads with watermarking for incremental loading. |
| `bronze_to_silver_transformer.py` | Silver | Bronze → Silver tables | Cleans, deduplicates, and enriches data (e.g., joins purchases with user/item details). |
| `silver_to_gold_transformer.py` | Gold | Silver → Gold tables | Creates aggregated analytical tables (top sellers, conversion rates, channel metrics). |
| `config.py` | — | — | Centralized configuration via dataclass, reads from environment variables. |
| `etl_utils.py` | — | — | Shared Spark session creation and Iceberg write utilities. |

## 🏃 Usage

All scripts run inside the `spark-iceberg` container.

```bash
# Create schemas
docker compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/bronze_schema.sql
docker compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/silver_schema.sql
docker compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/gold_schema.sql

# Ingest Bronze
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/minio_loader.py
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/postgres_loader.py

# Transform Silver
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/bronze_to_silver_transformer.py

# Transform Gold
docker compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/silver_to_gold_transformer.py
```

Or run everything at once with the orchestration script:
```bash
./lakehouse-preparer.sh
```

## 🛠️ Development

- **Configuration**: All settings are centralized in `config.py`. Never use `os.getenv()` directly in scripts.
- **Logging**: All scripts use Python's `logging` module. Avoid `print()` statements.
- **Error Handling**: All loaders include try/except blocks. Malformed pageview records are routed to a Dead Letter Queue (`bronze.pageviews_dlq`).
