# Current platform architecture

## Scope and evidence

This inventory is based on `docker-compose.yaml`, `airflow.yaml`, both normalized
Compose models, all referenced Dockerfiles, scripts, SQL, connector definitions,
application source, environment examples, and existing architecture diagrams. No
container or platform service was started. The unreadable `airflow/db/18/docker`
path is generated PostgreSQL state, not deployment source.

## Runtime shape

The repository defines 29 services: 24 in the core Compose file and five in the
Airflow Compose file. They use a logical network named `iceberg_net`. With the
documented commands run from the repository and the same default Compose project
name, the two files resolve to the same project-scoped network. A different working
directory or `--project-name` can isolate them and break Airflow's references to
`postgres`, `trino`, and `minio`.

There are two named core volumes, `kafka_data` and `minio_data`. Airflow and
Superset metadata use host bind mounts. Several logically stateful services have
no explicit data mount, including both core PostgreSQL containers, ClickHouse, and
OpenSearch. Container removal can therefore lose their data.

## Components

| Area | Services | Current role |
| --- | --- | --- |
| Sources and generators | `debezium-postgres`, `postgres`, `items-loadgen`, `flashsale-loadgen`, `login-loadgen`, `loadgen` | OLTP data, pgvector reviews, pageviews, purchases, and login events |
| Messaging and CDC | `kafka`, `kafka-setup`, `schema-registry`, `connect`, `connector-setup`, `redpanda-console` | KRaft broker, topics, Avro schemas, Debezium sources, OpenSearch sink, topic UI |
| Stream processing | `jobmanager`, `taskmanager` | Flink SQL login enrichment and anomaly output |
| Lakehouse storage | `minio`, `mc`, `rest` | S3-compatible objects, bucket bootstrap, Iceberg REST catalog fixture |
| Batch processing | `spark-iceberg` | Iceberg schema creation and Bronze/Silver/Gold ETL |
| Serving and search | `trino`, `clickhouse`, `opensearch` | Iceberg SQL, Kafka-engine real-time OLAP, item search index |
| Visualization | `superset`, `superset-db`, `streamlit` | BI metadata/UI and real-time/semantic-search UI |
| Orchestration | `airflow-db`, `airflow-init`, `airflow-scheduler`, `airflow-webserver`, `mailhog` | Airflow metadata, initialization, scheduling, UI, development SMTP |

No Prometheus, Grafana, Alertmanager, log collector, OpenSearch Dashboards,
Kubernetes, Helm, Kustomize, or CI/CD definition existed at discovery time.
OpenSearch currently receives item documents; it is not a centralized log system.

## Data paths

### Batch lakehouse

`loadgen` writes relational records to `postgres` and pageview JSON to MinIO.
Spark reads PostgreSQL through JDBC and MinIO through S3A, then writes Iceberg
Bronze, Silver, and Gold tables through the REST catalog into MinIO. Trino reads
the same catalog and object store. Superset stores its own metadata in
`superset-db` and creates a Trino connection.

### Item CDC and search

`items-loadgen` and `flashsale-loadgen` write to `debezium-postgres`. Kafka Connect
registers Debezium connectors for items and purchases. Item changes go through
Kafka in Avro and the OpenSearch sink indexes them. Purchase changes are consumed
directly by ClickHouse's Kafka engine.

### Login stream processing

`login-loadgen` registers/uses an Avro schema and produces `login-events`. Flink
reads those events, joins a mounted CSV lookup, and writes enriched and anomaly
topics. The SQL job is not submitted by a Compose service; it requires a manual
Flink SQL action. The checked-in anomaly SQL contains a malformed timestamp
identifier and has not been runtime-validated.

### Analytics and orchestration

Streamlit queries ClickHouse for flash-sale metrics and PostgreSQL/pgvector for
review similarity. Airflow has one Trino-to-MinIO segmentation DAG and one
PostgreSQL embedding DAG. The repository diagram suggests Airflow triggers Spark,
but no inspected DAG implements a Spark trigger.

## Ports and exposure

All published ports bind using Compose defaults, with no loopback-only host IP.
Published endpoints include PostgreSQL 5432/5433, Kafka 9092, Schema Registry
8081, Connect 8083, Redpanda Console 8084, MinIO 9000/9001, OpenSearch 9200/9600,
Iceberg REST 8181, Spark 8080/10000/10001, Trino 9090, ClickHouse 8123/9002,
Streamlit 8501, Superset 8088, Airflow 8085, and MailHog 1025/8025.

## Security and reliability posture

The platform is explicitly development-oriented: Kafka is plaintext, OpenSearch
security is disabled, MinIO is HTTP and makes the warehouse bucket anonymous,
service APIs have no authentication in the inspected config, and several tracked
files contain development credential literals. Resource requests/limits are not
defined. Only eight core services have health checks and no Airflow service does.
See `migration-risks.md` for the prioritized remediation record.
