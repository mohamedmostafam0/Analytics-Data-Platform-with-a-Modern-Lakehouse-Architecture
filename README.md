# Analytics Data Platform with a Modern Lakehouse Architecture

![Project Architecture](docs/architecture.svg)

### Data Flow
![Data Flow](docs/data_flow.svg)

## 🚀 Overview

This project implements a **Modern Data Lakehouse** architecture, combining the best features of data lakes and data warehouses. It provides a robust, scalable, and open platform for data engineering and analytics workloads.

The platform follows the **Medallion Architecture** (Bronze → Silver → Gold) and is built on open standards, leveraging **Apache Iceberg** for table format, **Apache Spark** for compute, **Trino** for interactive queries, and **Apache Superset** for visualization.

## ✨ Key Features

-   **Open Table Format**: Apache Iceberg for ACID transactions, time travel, and schema evolution.
-   **Scalable Compute**: Apache Spark 3.5 for large-scale data processing (ETL).
-   **Real-time Streaming**: Kafka + Debezium for Change Data Capture (CDC) from Postgres.
-   **Search & Analytics**: OpenSearch for full-text search and log analytics.
-   **Interactive SQL**: Trino for low-latency, ad-hoc analytical queries.
-   **BI & Visualization**: Apache Superset dashboards connected via Trino.
-   **S3-Compatible Storage**: MinIO provides high-performance object storage.
-   **REST Catalog**: Centralized Iceberg metadata management.
-   **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated).

## 🛠️ Tech Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Compute (ETL)** | [Apache Spark](https://spark.apache.org/) 3.5 | Batch data processing and transformations. |
| **Streaming** | [Apache Kafka](https://kafka.apache.org/) | Event streaming platform. |
| **Stream Processing** | [Apache Flink](https://flink.apache.org/) 1.17 | Stateful stream processing and bounded-state anomaly detection. |
| **CDC** | [Debezium](https://debezium.io/) | Change Data Capture for Postgres. |
| **Search Engine** | [OpenSearch](https://opensearch.org/) | Distributed search and analytics engine. |
| **Schema Registry** | [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) | Strict Avro schema enforcement and management. |
| **Query Engine** | [Trino](https://trino.io/) | Interactive SQL queries for analytics / BI. |
| **Table Format** | [Apache Iceberg](https://iceberg.apache.org/) | Open table format for huge analytic datasets. |
| **Storage** | [MinIO](https://min.io/) | S3-compatible object storage. |
| **Catalog** | Iceberg REST | Centralized metadata catalog. |
| **Visualization** | [Apache Superset](https://superset.apache.org/) | BI dashboards and data exploration. |
| **Orchestration** | [Apache Airflow](https://airflow.apache.org/) 2.8 | DAG-based workflow orchestration. |
| **OLTP Database** | [PostgreSQL](https://www.postgresql.org/) 18 | Source transactional database. |
| **Data Generator** | Custom Python | Multi-purpose synthetic data generators (E-commerce + Auth streaming). |
| **Email (Dev)** | [MailHog](https://github.com/mailhog/MailHog) | Local SMTP server for testing email alerts. |
| **Containerization** | Docker Compose | Local container orchestration. |

## 📂 Project Structure

```
├── .env                            # Environment variables (single source of truth)
├── .env.example                    # Template for new setups
├── docker-compose.yaml             # Core services (Spark, Trino, MinIO, Superset, Postgres)
├── airflow.yaml                    # Airflow services (webserver, scheduler, DB, MailHog)
├── scripts/                        # Utility scripts
│   └── lakehouse-preparer.sh       # End-to-end pipeline orchestrator
├── README.md
│
├── airflow/                        # Airflow orchestration
│   ├── dockerfile
│   └── dags/
│       ├── user_engagement_segments_dag.py
│       └── sql/
│           └── trino.sql           # Gold-layer segmentation query
│
├── load-generators/                # Data Generators
│   ├── sys-load/                   # System Load (CPU/Memory)
│   ├── items-load/                 # Product Seeder
│   ├── flashsale-load/             # Crash simulation (Purchases)
│   ├── login-load/                 # Real-time Auth Event Simulator (Avro)
│   └── README.md                   # Generator Documentation
│   └── postgres_bootstrap.sql
│
├── kafka-connect/                  # Streaming Pipeline Configs
│   ├── Dockerfile
│   ├── connector.json              # Debezium Source Config
│   ├── opensearch-sink.json        # OpenSearch Sink Config
│   └── register_connector.sh
│
├── flink/                          # Flink Resources
│   ├── sql/                        # Flink SQL Jobs
│   │   ├── create-tables.sql
│   │   └── insert-jobs.sql
│   └── lib/                        # Connectors (JARs)
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
│   ├── Dockerfile
│   └── init_connections.py         # Auto-creates Trino database connection
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

3.  **Start the Core Services**:
    ```bash
    docker compose up -d --build
    ```

4.  **Start Airflow** (optional):
    ```bash
    docker compose -f airflow.yaml up -d --build
    ```

5.  **Run the Data Pipeline**:
    ```bash
    # Generate synthetic data
    docker compose run loadgen

    # Run full pipeline (schemas → ingest → transform)
    chmod +x scripts/lakehouse-preparer.sh
    ./scripts/lakehouse-preparer.sh
    ```

## 🖥️ Services

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Superset** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| **Airflow** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` |
| **MailHog** | [http://localhost:8025](http://localhost:8025) | — |
| **Trino** | `http://localhost:9090` | — |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **MinIO API** | `http://localhost:9000` | — |
| **Iceberg REST** | `http://localhost:8181` | — |
| **Redpanda Console** | [http://localhost:8084](http://localhost:8084) | — |
| **Schema Registry** | [http://localhost:8081](http://localhost:8081) | — |
| **OpenSearch** | [http://localhost:9200](http://localhost:9200) | — |
| **Spark UI** | [http://localhost:8080](http://localhost:8080) | — |
| **PostgreSQL** | `localhost:5432` | See `.env` |

## 🏭 Data Pipeline

The pipeline follows the Medallion Architecture:

```
Sources                    Bronze              Silver                Gold
┌──────────┐          ┌────────────┐     ┌───────────────┐    ┌──────────────────────┐
│ Postgres │──JDBC──▶ │ users      │──▶  │ users         │    │ top_selling_items    │
│ (Users,  │          │ items      │     │ items         │──▶ │ sales_perf_24h       │
│  Items,  │          │ purchases  │──▶  │ purchases_    │    │ top_converting       │
│  Purch.) │          │            │     │   enriched    │    │ pageviews_by_ch      │
└──────────┘          └────────────┘     └───────────────┘    │ user_engagement_segs │
┌──────────┐          ┌────────────┐     ┌───────────────┐    └──────────────────────┘
│ MinIO    │──S3────▶ │ pageviews  │──▶  │ pageviews_    │
│ (JSON)   │          │ (+ DLQ)    │     │   by_items    │
└──────────┘          └────────────┘     └───────────────┘
```

> **Note:** `user_engagement_segments` is computed by the Airflow DAG (via Trino), not by Spark.

### Real-time Streaming Pipeline

```
┌──────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ Postgres │──CDC─▶ Kafka Topic  │──Sink─▶ OpenSearch │──API─▶ Architecture │
│ (Items)  │      │ (Avro)       │      │ (Items)    │      │ Diagram / UI │
└──────────┘      └──────────────┘      └──────────────┘      └──────────────┘
                               │
                               │
                          ┌─────────┐      ┌───────────────┐
                          │  Flink  │──SQL─▶ Login Anomalies│
                          │ (SQL)   │      │ (Kafka JSON)  │
                          └─────────┘      └───────────────┘
                               ▲
                               │ Avro Login Events
                               │
                        ┌──────────────┐      ┌──────────────┐
                        │ Login Loadgen│──Avro─▶ Schema      │
                        │ (Simulator)  │      │  Registry    │
                        └──────────────┘      └──────────────┘
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
![Superset Dashboard](docs/screenshots/superset-dashboard.png)

### MinIO Console
![MinIO Console](docs/screenshots/minio-console.png)

### Airflow DAG Graph
![Airflow DAG](docs/screenshots/airflow-dag-graph.png)

### Streamlit Real-Time Dashboard
<!-- TODO: Add screenshot of real-time ClickHouse metrics at http://localhost:8501 -->
![Streamlit Dashboard](docs/screenshots/streamlit-dashboard.png)

### Kafka Topics in Redpanda Console 
<!-- TODO: Add screenshot of Avro schemas or Topic UI at http://localhost:8084 -->
![Redpanda Console](docs/screenshots/redpanda-console.png)

### Flink Pipeline Topology
![Flink Topology - Login Events Enriched](docs/screenshots/login_events_enriched_flink.png)

![Flink Topology - Login Anomalies](docs/screenshots/login_anomalies_flink.png)

### Spark UI
<!-- TODO: Add screenshot of Spark UI showing completed ETL jobs -->
<!-- ![Spark UI](docs/screenshots/spark-ui.png) -->

---

## 🔍 Querying Data

### Via Trino (CLI)
```bash
docker exec trino trino --execute "SELECT * FROM iceberg.gold.top_selling_items ORDER BY total_revenue DESC LIMIT 10"
```

### Via Superset
1. Open [http://localhost:8088](http://localhost:8088) and login with `admin` / `admin`.
2. The Trino database connection (`trino://trino@trino:8080/iceberg`) is auto-created at startup.
3. Create charts and dashboards from the `gold` schema tables.

### Via Airflow
1. Open [http://localhost:8085](http://localhost:8085) and login with `admin` / `admin`.
2. Trino and MinIO connections are auto-created via `AIRFLOW_CONN_` env vars.
3. Enable the `user_engagement_segments_dag` to run the daily segmentation pipeline.
4. Check email alerts in [MailHog](http://localhost:8025).
