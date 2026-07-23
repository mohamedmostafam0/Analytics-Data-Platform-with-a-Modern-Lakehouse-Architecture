# Local resource strategy

## Interpretation

Resource classes are relative estimates from service type, image contents,
continuous behavior, JVM heap settings, and dependency count. They are not measured
CPU or memory claims because Compose defines no resource limits and no full stack
was started.

Base Helm values enable no workload. The `minimal` overlay enables only the
hibernatable Phase 2 slice. In Phase 3A, the `batch` overlay enables only its
storage foundation; the larger batch target remains deferred.

| Profile | Target enabled services | Required | Optional | Relative class | Purpose and validation |
| --- | --- | --- | --- | --- | --- |
| `minimal` | single-instance CloudNativePG source, one-shot `items-loadgen` | both | none | small to medium | Implemented in Phase 2; seed 100 rows, rerun without growth, then optionally hibernate PostgreSQL. |
| `ingestion` | `debezium-postgres`, `items-loadgen`, `kafka`, `schema-registry`, `connect`, `connector-setup`, `opensearch` | all for item CDC search | `flashsale-loadgen`, `redpanda-console` | large | Prove WAL-to-search CDC; validate connector/task status and indexed records. |
| `streaming` | `kafka`, `kafka-setup`, `schema-registry`, `login-loadgen`, `jobmanager`, `taskmanager` | all | `redpanda-console` | large | Prove Avro login enrichment/anomalies; validate topics, submitted Flink jobs, and bounded state. |
| `batch` | Current: main CloudNativePG, local MinIO, finite `mc` Job. Deferred: Polaris, Spark, `loadgen` | current storage trio | catalog/processing/Airflow later | medium now; very large at target | Phase 3A validates schema, restore, private buckets, object persistence, and hibernation. It does not run ETL. |
| `analytics` | `minio`, `rest`, `trino`, `superset`, `superset-db` | first three for queries | Superset UI; ClickHouse/Streamlit branch with its ingestion dependencies | large to very large | Query existing Iceberg data; avoid generating and processing data simultaneously. |
| `observability` | none currently | unknown | unknown | unknown | Reserved for Phase 6 after an actual metrics stack is selected; only static config validation is possible now. |
| `logging` | `opensearch` is the only existing backend | collector and dashboard are missing | none | large | Reserved for Phase 7; OpenSearch currently stores item search data, not logs. |
| `full` | all 29 services | all applicable dependencies | none | very large | Architecture/integration environment only; unsuitable for normal laptop use. |

## Component resource classification

- **Tiny:** one-shot setup Jobs (`kafka-setup`, `connector-setup`, `mc`,
  `airflow-init`), `items-loadgen`, MailHog.
- **Small:** login and flash-sale generators, Iceberg REST fixture, Redpanda
  Console.
- **Medium:** PostgreSQL instances, Schema Registry, Kafka Connect, MinIO,
  Airflow webserver/scheduler, Superset, Flink JobManager.
- **Large:** Kafka, OpenSearch, ClickHouse, Trino, Streamlit (ML/Torch image),
  Superset plus metadata database, Flink TaskManager.
- **Very large:** Spark image/ETL and any combined batch/full profile.

## Combination guidance

No hard port conflict was found in the declared Compose mappings. The constraints
are resource and data-safety based:

- Do not combine `batch`, `streaming`, and `analytics` on a normal laptop.
- Do not run `full` locally as a routine validation path.
- Do not run duplicate generators, connectors, schedulers, or setup Jobs against
  the same data stores.
- Stop one profile before starting another when it shares Kafka, PostgreSQL,
  MinIO, ClickHouse, or OpenSearch state.
- Use static rendering for every profile; use a single target slice for runtime
  smoke tests.

## Safe validation levels

1. Schema, shell, Compose normalization, Helm lint/render, and Kubernetes schema
   checks: always preferred.
2. Namespace-only client dry-run: safe and non-mutating.
3. One profile with reduced fixture counts: explicit approval required.
4. Combined or full runtime: remote/CI capacity and explicit approval required.

## Measured Phase 2 envelope

Before runtime work on 2026-07-22, the host exposed 20 CPUs, about 30 GiB RAM
(about 22 GiB available), and 36 GiB free disk. The Phase 2 chart requests 100m
CPU/256 MiB for PostgreSQL and 25m CPU/32 MiB for the finite Job; limits are 1
CPU/1 GiB and 250m CPU/128 MiB respectively. These are development guardrails,
not production sizing evidence.

## Phase 3A guardrails

The storage overlay adds 150m CPU/384 MiB requested for the main PostgreSQL and
100m CPU/256 MiB for MinIO; the finite client Job requests 20m CPU/32 MiB. It uses
two 2 GiB PVCs. Runtime testing keeps the Phase 2 database hibernated, does not
start messaging or processing, and hibernates Phase 3A after acceptance. These are
development guardrails rather than production sizing or availability evidence.
