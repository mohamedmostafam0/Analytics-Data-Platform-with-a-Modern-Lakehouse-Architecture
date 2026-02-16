# Analytics Data Platform with a Modern Lakehouse Architecture

![Lakehouse Architecture](lakehouse%20architecture.png)

## 🚀 Overview

This project implements a **Modern Data Lakehouse** architecture, combining the best features of data lakes and data warehouses. It provides a robust, scalable, and open platform for data engineering and analytics workloads.

The platform follows the **Medallion Architecture** (Bronze → Silver → Gold) and is built on open standards, leveraging **Apache Iceberg** for table format, **Apache Spark** for compute, **Trino** for interactive queries, and **Apache Superset** for visualization.

## ✨ Key Features

-   **Open Table Format**: Apache Iceberg for ACID transactions, time travel, and schema evolution.
-   **Scalable Compute**: Apache Spark 3.5 for large-scale data processing (ETL).
-   **Interactive SQL**: Trino for low-latency, ad-hoc analytical queries.
-   **BI & Visualization**: Apache Superset dashboards connected via Trino.
-   **S3-Compatible Storage**: MinIO provides high-performance object storage.
-   **REST Catalog**: Centralized Iceberg metadata management.
-   **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated).

## 🛠️ Tech Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Compute (ETL)** | [Apache Spark](https://spark.apache.org/) 3.5 | Batch data processing and transformations. |
| **Query Engine** | [Trino](https://trino.io/) | Interactive SQL queries for analytics / BI. |
| **Table Format** | [Apache Iceberg](https://iceberg.apache.org/) | Open table format for huge analytic datasets. |
| **Storage** | [MinIO](https://min.io/) | S3-compatible object storage. |
| **Catalog** | Iceberg REST | Centralized metadata catalog. |
| **Visualization** | [Apache Superset](https://superset.apache.org/) | BI dashboards and data exploration. |
| **OLTP Database** | [PostgreSQL](https://www.postgresql.org/) 18 | Source transactional database. |
| **Data Generator** | Custom Python | Synthetic e-commerce data. |
| **Orchestration** | Docker Compose | Local container orchestration. |

## 📂 Project Structure

```
├── .env                            # Environment variables (single source of truth)
├── .env.example                    # Template for new setups
├── docker-compose.yaml             # All service definitions
├── lakehouse-preparer.sh           # End-to-end pipeline orchestration script
├── README.md
│
├── loadgen/                        # Synthetic data generator
│   ├── Dockerfile
│   ├── generate_load.py
│   └── requirements.txt
│
├── postgres/                       # Postgres initialization
│   └── postgres_bootstrap.sql
│
├── spark/                          # Spark image & ETL scripts
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── spark-defaults.conf
│   └── scripts/
│       ├── sql/                    # Iceberg DDL (per-layer)
│       │   ├── bronze_schema.sql
│       │   ├── silver_schema.sql
│       │   └── gold_schema.sql
│       ├── config.py               # Centralized configuration
│       ├── etl_utils.py            # Shared utilities
│       ├── minio_loader.py         # Bronze: MinIO → Iceberg
│       ├── postgres_loader.py      # Bronze: Postgres → Iceberg
│       ├── bronze_to_silver_transformer.py
│       ├── silver_to_gold_transformer.py
│       └── tests/
│
├── superset/                       # Superset image config
│   └── Dockerfile
│
└── trino/                          # Trino catalog config
    └── etc/catalog/
        └── iceberg.properties
```

## ⚡ Getting Started

### Prerequisites

-   [Docker](https://www.docker.com/)
-   [Docker Compose](https://docs.docker.com/compose/)

### Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Configure Environment**:
    ```bash
    cp .env.example .env
    ```
    > The defaults work out-of-the-box for local development.

3.  **Start the Services**:
    ```bash
    docker-compose up -d --build
    ```

4.  **Run the Data Pipeline**:
    ```bash
    # Generate synthetic data
    docker-compose run loadgen

    # Run full pipeline (schemas → ingest → transform)
    chmod +x lakehouse-preparer.sh
    ./lakehouse-preparer.sh
    ```

## 🖥️ Services

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Superset** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| **Trino** | `http://localhost:9090` | — |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **MinIO API** | `http://localhost:9000` | — |
| **Iceberg REST** | `http://localhost:8181` | — |
| **Spark UI** | [http://localhost:8080](http://localhost:8080) | — |
| **PostgreSQL** | `localhost:5432` | `admin` / `password` |

## 🏭 Data Pipeline

The pipeline follows the Medallion Architecture:

```
Sources                    Bronze              Silver                Gold
┌──────────┐          ┌────────────┐     ┌───────────────┐    ┌──────────────────┐
│ Postgres │──JDBC──▶ │ users      │──▶  │ users         │    │ top_selling_items │
│ (Users,  │          │ items      │     │ items         │──▶ │ sales_perf_24h   │
│  Items,  │          │ purchases  │──▶  │ purchases_    │    │ top_converting   │
│  Purch.) │          │            │     │   enriched    │    │ pageviews_by_ch  │
└──────────┘          └────────────┘     └───────────────┘    └──────────────────┘
┌──────────┐          ┌────────────┐     ┌───────────────┐
│ MinIO    │──S3────▶ │ pageviews  │──▶  │ pageviews_    │
│ (JSON)   │          │ (+ DLQ)    │     │   by_items    │
└──────────┘          └────────────┘     └───────────────┘
```

### Running Individual Steps

```bash
# 1. Generate data
docker-compose run loadgen

# 2. Create schemas
docker-compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/bronze_schema.sql
docker-compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/silver_schema.sql
docker-compose exec spark-iceberg /opt/spark/bin/spark-sql -f /home/iceberg/scripts/sql/gold_schema.sql

# 3. Ingest to Bronze
docker-compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/minio_loader.py
docker-compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/postgres_loader.py

# 4. Transform Bronze → Silver
docker-compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/bronze_to_silver_transformer.py

# 5. Transform Silver → Gold
docker-compose exec spark-iceberg /opt/spark/bin/spark-submit /home/iceberg/scripts/silver_to_gold_transformer.py
```

### 🧪 Running Tests

```bash
docker exec spark-iceberg pytest /home/iceberg/scripts/tests/
```

## 📸 Screenshots

### Superset Dashboard
<!-- TODO: Add screenshot of Superset dashboard with Gold layer charts -->
![Superset Dashboard](screenshots/superset-dashboard.png)

### MinIO Console
<!-- TODO: Add screenshot of MinIO console showing warehouse bucket -->
![MinIO Console](screenshots/minio-console.png)

### Trino Query Results
<!-- TODO: Add screenshot of Trino querying gold tables -->
![Trino Query](screenshots/trino-query.png)

### Spark UI
<!-- TODO: Add screenshot of Spark UI showing completed ETL jobs -->
![Spark UI](screenshots/spark-ui.png)

---

## 🔍 Querying Data

### Via Trino (CLI)
```bash
docker exec trino trino --execute "SELECT * FROM iceberg.gold.top_selling_items ORDER BY total_revenue DESC LIMIT 10"
```

### Via Superset
1. Open [http://localhost:8088](http://localhost:8088) and login with `admin` / `admin`.
2. Add a Trino database connection: `trino://trino@trino:8080/iceberg`.
3. Create charts and dashboards from the `gold` schema tables.
