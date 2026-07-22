# Kubernetes migration risks

| ID | Severity | Evidence and impact | Phase treatment |
| --- | --- | --- | --- |
| R01 | Critical | Tracked configs contain development credential literals; `.env` is gitignored but mode `0664` and contains default-like values. | Redact from new config; tighten local guidance; remove/rotate only with explicit approval. |
| R02 | Critical | Connector setup can print an environment-expanded connector payload, including database credentials, after failure. | Redesign before migrating Connect. |
| R03 | High | OpenSearch security is disabled, MinIO uses HTTP and grants anonymous warehouse access, Kafka is plaintext, and ports bind broadly. | Default-deny exposure and authenticate each migrated component. |
| R04 | High | Core PostgreSQL, CDC PostgreSQL, ClickHouse, and OpenSearch lack explicit data volumes. | Decide intended durability and backup before migration. |
| R05 | High | No service has Compose CPU/memory limits; full startup is laptop-unsafe. | Keep all Helm workloads off by default; add explicit resources per workload. |
| R06 | High | Existing images include implicit or explicit `latest` tags; several Dockerfiles and Python dependencies are unpinned. | Pin new artifacts; remediate existing artifacts in approved service phases. |
| R07 | High | Only eight core services have health checks; Airflow has none. Several dependencies wait only for process start or fixed sleep. | Verify actual probes and startup gates per service. |
| R08 | High | `postgres_bootstrap.sql` requests pgvector but is mounted into both a pgvector image and a Debezium PostgreSQL image. Compatibility is unverified. | Split/parameterize bootstrap only after a Phase 2 test and approval. |
| R09 | High | The Flink anomaly SQL contains a malformed timestamp identifier, and no Compose service submits the SQL jobs. | Fix and add bounded static/runtime tests in Phase 4, not now. |
| R10 | Medium | Two Compose files share cross-stack DNS only when the Compose project name resolves consistently. | Document and make mixed-mode endpoints explicit. |
| R11 | Medium | Broad `env_file` use injects unrelated credentials into several containers. | Replace with least-variable Secret/ConfigMap references per migration. |
| R12 | Medium | Items seeder was not idempotent; flash-sale/login generators run indefinitely. | Items seeder fixed and runtime-tested in Phase 2; define shutdown semantics for the remaining generators in Phase 3. |
| R13 | Medium | Spark entrypoint always creates schemas, sleeps a fixed ten seconds, then starts a Thrift server; Spark tests overwrite/drop source tables. | Separate Jobs from services and isolate destructive tests. |
| R14 | Medium | Existing docs claim Airflow triggers Spark and defaults work out of the box; inspected DAGs/config do not fully support those claims. | Keep discovery docs authoritative; correct user docs in a separately approved change. |
| R15 | Medium | Airflow image includes sentence-transformers and downloads/loads an ML model, increasing build/runtime cost. Streamlit includes Torch for one page. | Split optional ML capability or cache artifacts in later phases. |
| R16 | Medium | Superset and Airflow initialization perform migrations/admin creation at container start with weak readiness guarantees. | Use controlled Jobs and idempotent migrations. |
| R17 | Medium | Iceberg REST uses a fixture image with no tag in Compose; persistence and production support are unknown. | Evaluate production catalog choices in Phase 3 ADR. |
| R18 | Low | Existing architecture diagrams describe an embedding load generator that was replaced by an Airflow DAG. | Regenerate diagrams after component phases stabilize. |

No existing data or credentials were modified during discovery.

Phase 2 runtime testing used only a disposable kind cluster and a generated local
Secret. Two failed zero-row bootstrap PVCs were removed with exact targets during
debugging; no existing Compose volume, credential, container, or user data changed.
