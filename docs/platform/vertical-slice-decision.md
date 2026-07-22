# First vertical-slice decision

## Scoring

Scores are 1 (unfavorable) to 5 (favorable). Resource efficiency and dependency
simplicity score higher when they consume less.

| Candidate | Business value | Portfolio value | Resource efficiency | Operational simplicity | Testability | Dependency simplicity | End-to-end path | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Items seeder -> PostgreSQL | 3 | 4 | 5 | 4 | 5 | 5 | 3 | **29** |
| Login generator -> Kafka -> Flink -> Kafka | 4 | 5 | 2 | 2 | 3 | 2 | 5 | 23 |
| System generator -> PostgreSQL/MinIO -> Spark/Iceberg | 5 | 5 | 1 | 1 | 3 | 1 | 5 | 21 |
| Streamlit -> ClickHouse/PostgreSQL | 4 | 4 | 2 | 2 | 4 | 2 | 4 | 22 |
| Airflow metadata + DAG parsing | 3 | 4 | 3 | 2 | 3 | 3 | 2 | 20 |

## Accepted implementation

`items-loadgen` was migrated as a finite Kubernetes Job with one disposable
CloudNativePG dependency. This exercises a custom image, configuration, Secret
references, database readiness, Job completion, logs, retry policy, resources,
and rollback without Kafka, Spark, Flink, or analytics services.

Use the current application contract (`POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`) and do not embed credentials.
The source is deliberately separate from `debezium-postgres`: it uses PostgreSQL
18.4, a dedicated schema bootstrap, and no pgvector, WAL, CDC, or messaging setup.

## Explicit exclusions

Phase 2 should not add Kafka, Schema Registry, Connect, OpenSearch, ClickHouse,
MinIO, Spark, Flink, Airflow, Trino, Superset, Streamlit, monitoring, or logging.
It should not change Compose behavior or use production data.

## Phase 2 acceptance result

1. The database artifact and immutable version are documented and pinned.
2. A development-only database starts with a PVC and verified readiness.
3. The items Job runs as non-root where the existing image permits, has explicit
   requests/limits, no service-account token, and no plaintext secret.
4. The Job inserts the expected fixture count and reports no credential values.
5. Rerun semantics are explicitly chosen and tested rather than assumed idempotent.
6. Disabled flags render no database, Job, Service, PVC, RBAC, or Secret.
7. Invalid feature combinations fail before deployment.
8. Rollback removes workloads while retaining data unless deletion is explicitly
   approved.
9. Core and Airflow Compose files still pass static normalization.

All nine criteria passed on 2026-07-22. The Job seeded 100 rows and a second run
left 100 rows. Hibernation stopped PostgreSQL while retaining the bound 1 GiB PVC.
