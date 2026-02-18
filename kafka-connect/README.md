# Kafka Connect Configuration

This directory contains the configuration and automation scripts for the Real-time Streaming Pipeline using Kafka Connect.

## 📂 Contents

| File | Description |
| :--- | :--- |
| `Dockerfile` | Custom Connect image with Debezium 3.0 and Confluent Avro Converters. |
| `connector.json` | Debezium Postgres Source Connector configuration. |
| `opensearch-sink.json` | OpenSearch Sink Connector configuration. |
| `register_connector.sh` | Script to automatically register all `.json` connectors in this directory. |

## 🚀 Connectors

### 1. Postgres Source (`connector.json`)
Stream changes from the `public.items` table in Postgres to the `dbz.public.items` Kafka topic.
-   **Plugin**: `pgoutput` (Logical Replication)
-   **Format**: Avro (via Schema Registry)
-   **Transforms**: `ExtractNewRecordState` (flattens complex Debezium envelope to raw row data)

### 2. OpenSearch Sink (`opensearch-sink.json`)
Reads from `dbz.public.items` and writes to OpenSearch `items` index.
-   **Strategy**: "Soft Delete" (ignores tombstones, keeps records with `__deleted: true`).
-   **Format**: Avro (via Schema Registry).

## 🛠️ Usage

### Automated (Docker Compose)
The `connector-setup` service in the main `docker-compose.yaml` automatically runs `register_connector.sh` when the stack starts. No manual action is required.

### Manual Registration
If you need to register a connector manually:

```bash
# Register Source Connector
curl -i -X PUT http://localhost:8083/connectors/cdc-connector/config \
     -H "Content-Type: application/json" \
     -d @connector.json

# Register Sink Connector
curl -i -X PUT http://localhost:8083/connectors/opensearch-sink-connector/config \
     -H "Content-Type: application/json" \
     -d @opensearch-sink.json
```

### Checking Status
You can use the provided verification script to check the health of Connectors, Schema Registry, and OpenSearch indices in one go:

```bash
./kafka-connect/verify_pipeline.sh
```

Or manually:
```bash
curl http://localhost:8083/connectors?expand=status | python3 -m json.tool
```
