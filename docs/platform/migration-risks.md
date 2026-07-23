# Kubernetes migration risks

| ID | Severity | Evidence and impact | Phase treatment |
| --- | --- | --- | --- |
| R01 | Critical | Tracked configs contain development credential literals; `.env` is gitignored but mode `0664` and contains default-like values. | Redact from new config; tighten local guidance; remove/rotate only with explicit approval. |
| R02 | Critical | Connector setup can print an environment-expanded connector payload, including database credentials, after failure. | Redesign before migrating Connect. |
| R03 | High | OpenSearch security is disabled, Compose MinIO uses HTTP and grants anonymous warehouse access, Kafka is plaintext, and ports bind broadly. | Phase 3A makes Kubernetes buckets private and ClusterIP-only but remains local HTTP; secure each later component before acceptance. |
| R04 | High | Core PostgreSQL, CDC PostgreSQL, ClickHouse, and OpenSearch lack explicit Compose data volumes. | Phase 3A gives main Kubernetes PostgreSQL a retained PVC and logical restore evidence; other components remain unresolved. |
| R05 | High | No service has Compose CPU/memory limits; full startup is laptop-unsafe. | Keep all Helm workloads off by default; add explicit resources per workload. |
| R06 | High | Existing images include implicit or explicit `latest` tags; several Dockerfiles and Python dependencies are unpinned. | Pin new artifacts; remediate existing artifacts in approved service phases. |
| R07 | High | Only eight core services have health checks; Airflow has none. Several dependencies wait only for process start or fixed sleep. | Verify actual probes and startup gates per service. |
| R08 | High | `postgres_bootstrap.sql` requests pgvector but is mounted into both a pgvector image and a Debezium PostgreSQL image. Compatibility is unverified. | Phase 3A uses a separate credential-free main bootstrap; select and test a CDC-specific bootstrap/image in Phase 3B. |
| R09 | High | The Flink anomaly SQL contains a malformed timestamp identifier, and no Compose service submits the SQL jobs. | Fix and add bounded static/runtime tests in Phase 4, not now. |
| R10 | Medium | Two Compose files share cross-stack DNS only when the Compose project name resolves consistently. | Document and make mixed-mode endpoints explicit. |
| R11 | Medium | Broad `env_file` use injects unrelated credentials into several containers. | Replace with least-variable Secret/ConfigMap references per migration. |
| R12 | Medium | Items seeder was not idempotent; flash-sale/login generators run indefinitely. | Items seeder fixed and runtime-tested in Phase 2; define shutdown semantics for the remaining generators in Phase 3. |
| R13 | Medium | Spark entrypoint always creates schemas, sleeps a fixed ten seconds, then starts a Thrift server; Spark tests overwrite/drop source tables. | Separate Jobs from services and isolate destructive tests. |
| R14 | Medium | Existing docs claim Airflow triggers Spark and defaults work out of the box; inspected DAGs/config do not fully support those claims. | Keep discovery docs authoritative; correct user docs in a separately approved change. |
| R15 | Medium | Airflow image includes sentence-transformers and downloads/loads an ML model, increasing build/runtime cost. Streamlit includes Torch for one page. | Split optional ML capability or cache artifacts in later phases. |
| R16 | Medium | Superset and Airflow initialization perform migrations/admin creation at container start with weak readiness guarantees. | Use controlled Jobs and idempotent migrations. |
| R17 | Medium | Iceberg REST uses a fixture image with no tag in Compose; persistence and production support are unknown. | ADR-008 selects Apache Polaris 1.6.0; authenticated JDBC deployment remains gated to a catalog integration sub-slice. |
| R18 | Low | Existing architecture diagrams describe an embedding load generator that was replaced by an Airflow DAG. | Regenerate diagrams after component phases stabilize. |
| R19 | High | MinIO server, client, and operator upstream repositories are archived; the final server security fix has no published community image. | Build the exact fixed source only for Phase 3A local compatibility; require a supported S3 service/distribution for production. |
| R20 | High | Phase 2/3A NetworkPolicy objects were API-accepted, but no policy-capable CNI enforcement or allowed/denied traffic matrix was tested. | Select an enforcing CNI for the target environment and make positive/negative connectivity tests an acceptance gate before production. |

No existing data or credentials were modified during discovery.

Phase 2 runtime testing used only a disposable kind cluster and a generated local
Secret. Two failed zero-row bootstrap PVCs were removed with exact targets during
debugging; no existing Compose volume, credential, container, or user data changed.

Phase 3A used separate generated Secrets, Helm release names, database, buckets,
and PVCs. Logical restore, object restart persistence, and hibernate/resume passed;
the Phase 2 PVC identity was unchanged. The local MinIO risk remains accepted only
for compatibility testing, and the final state is hibernated.
