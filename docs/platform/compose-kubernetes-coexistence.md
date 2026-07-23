# Compose and Kubernetes coexistence

## Authority by phase

| Phase | Compose authority | Kubernetes authority |
| --- | --- | --- |
| 0-1 | All 29 services | Namespace and feature policy only |
| 2 | All services except the isolated item-seeding test slice during its test window | Disposable source PostgreSQL and item Job only |
| 3A | All services except isolated storage compatibility tests during their test window | Main development PostgreSQL and local S3 compatibility only |
| 3B+ | Every service not yet accepted on Kubernetes | Only components with completed phase acceptance |

Compose remains the rollback source throughout migration. No Compose file is moved,
renamed, or generated from Helm.

## Operating rules

- Normalize both Compose files before and after infrastructure changes.
- Start only named services and their minimum dependencies; never use an
  unqualified full `up` for migration validation.
- Never run equivalent producers, connectors, setup Jobs, or Airflow schedulers in
  both orchestrators against the same state.
- Use distinct databases, buckets, Kafka consumer groups, connector names, and
  ports for parallel tests, or stop the Compose equivalent first.
- Preserve current environment-variable names where they are application contracts;
  map sensitive values from Kubernetes Secret references.
- Do not force Compose to consume Helm values or Kubernetes file formats.
- A rollback disables/uninstalls only the accepted Kubernetes slice, preserves its
  storage by default, and restarts the unchanged Compose service if needed.

## Network caveat

The two Compose files declare `iceberg_net` but do not set an external or fixed
network name. Their ability to communicate relies on using the same Compose project
name, as the documented same-directory commands do. Mixed-mode Kubernetes testing
must use an explicit reachable endpoint; Compose service DNS names are not valid in
Kubernetes.

## Compatibility record

Phase 0-2 made no changes to either Compose file, `.env`, Compose service names,
ports, volumes, or startup order. The item-seeder source now requires credentials
instead of unsafe application defaults and defaults to idempotent reruns in both
orchestrators; its existing environment-variable contract remains unchanged.

The Kubernetes source uses a separate database/PVC and generated local Secret. It
does not connect to `debezium-postgres` or any existing Compose state. After Phase 2
acceptance it is hibernated, with its 100-row PVC retained.

Phase 3A uses a separate `lakehouse-storage` Helm release, main database/PVC, and
bucket namespace. It does not upgrade or uninstall the Phase 2 release and the
runtime test asserts that the Phase 2 PVC UID is unchanged. No Kubernetes writer
targets Compose PostgreSQL or MinIO. The source-built MinIO image preserves the
S3 API contract for local testing only; Compose remains authoritative until a
supported production object-storage target and catalog integration are accepted.
