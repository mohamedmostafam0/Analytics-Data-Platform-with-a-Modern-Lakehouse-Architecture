# Analytics Data Platform with a Modern Lakehouse Architecture

![Lakehouse Architecture](lakehouse%20architecture.png)

## 🚀 Overview

This project implements a **Modern Data Lakehouse** architecture, combining the best features of data lakes and data warehouses. It provides a robust, scalable, and open platform for data engineering, analytics, and machine learning workloads.

The platform is built on open standards, leveraging **Apache Iceberg** for table format, **Apache Spark** for compute, and **MinIO** for S3-compatible object storage.

## ✨ Key Features

-   **Open Table Format**: Uses Apache Iceberg for ACID transactions, time travel, and schema evolution.
-   **Scalable Compute**: Apache Spark 3.5 for large-scale data processing.
-   **S3-Compatible Storage**: MinIO provides a high-performance object storage layer.
-   **Interactive Development**: Jupyter Notebooks with PySpark and Iceberg integration.
-   **REST Catalog**: Centralized metadata management via Iceberg REST Catalog.

## 🛠️ Tech Stack

| Component | Technology | Version | Description |
| :--- | :--- | :--- | :--- |
| **Compute Engine** | [Apache Spark](https://spark.apache.org/) | 3.5.6 | Distributed data processing engine. |
| **Table Format** | [Apache Iceberg](https://iceberg.apache.org/) | 1.9.0 | Open table format for huge analytic datasets. |
| **Storage** | [MinIO](https://min.io/) | Latest | High-performance, S3-compatible object storage. |
| **Catalog** | Iceberg REST | - | Lightweight catalog for Iceberg tables. |
| **Orchestration** | Docker Compose | - | Container orchestration for local development. |
| **IDE** | Jupyter | - | Interactive development environment. |

## 📂 Project Structure

```bash
.
├── docker-compose.yaml        # Docker Compose configuration for all services
├── .env                       # Environment variables (credentials, endpoints)
├── .env.example               # Example environment variables for new setups
├── lakehouse architecture.png # Architecture diagram
├── README.md                  # Project documentation
└── spark/                     # Spark image configuration
    ├── Dockerfile             # Custom Spark image with Iceberg & AWS dependencies
    ├── requirements.txt       # Python dependencies (PyIceberg, Jupyter, etc.)
    └── spark-defaults.conf    # Spark default configuration (catalogs, extensions)
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
    Copy the example environment file to `.env`:
    ```bash
    cp .env.example .env
    ```
    *Note: The default credentials in `.env.example` work out-of-the-box for local development.*

3.  **Start the Services**:
    Build and start the Docker containers:
    ```bash
    docker-compose up -d --build
    ```

4.  **Verify Installation**:
    Check if all containers are running:
    ```bash
    docker-compose ps
    ```

## 🖥️ Usage

Once the services are up and running, you can access them via the following interfaces:

| Service | access URL | Credentials (Default) |
| :--- | :--- | :--- |
| **Jupyter Notebook** | [http://localhost:8888](http://localhost:8888) | None (Token disabled) |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | User: `admin`, Pass: `password` |
| **MinIO API** | `http://localhost:9000` | - |
| **Iceberg REST** | `http://localhost:8181` | - |
| **Spark History** | `http://localhost:18080` | - |

### 📓 Running Notebooks

1.  Open your browser and navigate to [http://localhost:8888](http://localhost:8888).
2.  You will see the `notebooks/` directory (mapped from your local `./notebooks` folder).
3.  Create a new Python notebook.
4.  Spark is pre-configured with Iceberg. You can start running Spark code immediately:

    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Create an Iceberg table
    spark.sql("CREATE TABLE demo.nyc.taxis (vendor_id bigint, trip_id bigint) USING iceberg")
    ```

### 🗄️ Managing Data in MinIO

1.  Go to [http://localhost:9001](http://localhost:9001).
2.  Login with `admin` / `password`.
3.  You can view the `warehouse` bucket where Iceberg data (metadata, snapshots, data files) is stored.
