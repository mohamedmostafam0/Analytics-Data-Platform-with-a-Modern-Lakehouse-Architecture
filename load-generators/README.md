# Load Generators

This directory contains utility applications designed to simulate various real-world data engineering scenarios for the Modern Lakehouse Architecture platform. Each generator serves a distinct purpose, from steady-state background operations to high-throughput streaming and anomaly generation.

## Available Generators

### 1. System Load (`sys-load`)
**Purpose:** 
Continuously consumes CPU, Memory, and Disk bandwidth in the background. It mimics a live production environment to test system resilience, validate container health checks, and periodically export metrics to MinIO.
**Tech:** Python
**Execution:** Runs permanently in the background via Docker Compose (`loadgen` service).

### 2. Items Simulator (`items-load`)
**Purpose:** 
Initializes the PostgreSQL database with realistic E-Commerce seed data. It populates users and catalog items, driving the initial Change Data Capture (CDC) pipelines via Debezium and seeding the OpenSearch indexes.
**Tech:** Python, psycopg2
**Execution:** Runs automatically at startup (`items-loadgen` service).

### 3. Flashsale Simulator (`flashsale-load`)
**Purpose:** 
A high-throughput OLTP simulator designed to stress-test the PostgreSQL database and the Debezium CDC pipeline. It simulates rapid, concurrent purchase events resembling a "Flash Sale" or "Black Friday" scenario, allowing you to observe Kafka lag, Flink backpressure, and ClickHouse ingestion speed.
**Tech:** Python, ThreadPoolExecutor 
**Execution:** Runs continuously alongside the cluster (`flashsale-loadgen` service).

### 4. Real-time Login/Auth Simulator (`login-load`)
**Purpose:**
A streaming data simulator that bypasses the database and streams directly to Kafka. It generates synthetic user login events, producing strictly enforced binary serialization using **Confluent Avro** and the **Schema Registry**. This feeds the Apache Flink Pipeline to demonstrate real-time, stateful anomaly detection (e.g., detecting impossible travel/unseen countries).
**Tech:** Python, confluent-kafka, fastavro
**Execution:** Runs continuously alongside the cluster (`login-loadgen` service).

---

## Configuration

All simulators are configured centrally via the project's root `.env` file. To adjust the scale of the simulations:

```env
# E-commerce Scale
USERS_SEED_COUNT=10000
ITEM_SEED_COUNT=1000
PURCHASE_GEN_EVERY_MS=100  # Decrease to increase transaction rate

# Auth Streaming Scale
KAFKA_BROKERS=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

## Running Manually

If you scale down the services and wish to run them strictly on-demand:

```bash
# Seed the Database
docker compose run --rm items-loadgen

# Trigger a Flash Sale Event
docker compose run --rm flashsale-loadgen

# Stream Login Events to Kafka
docker compose run --rm login-loadgen
```
