# ADR-008: Phase 3A storage and Iceberg catalog strategy

## Status

Accepted

## Context

The Compose batch path uses an ephemeral main `pgvector/pgvector:pg18` database,
an unpinned MinIO server/client pair, and the unpinned
`apache/iceberg-rest-fixture`. Kubernetes needs durable, authenticated storage,
but a laptop cannot safely absorb the storage, messaging, and processing stacks
in one test.

Artifact maintenance also changed before this phase. The MinIO server and operator
repositories are archived, and the last security release
`RELEASE.2025-10-15T17-29-55Z` has source but no published community container.
The Compose Iceberg REST fixture is not a production catalog. Apache Polaris is a
maintained Iceberg REST catalog with an official Helm chart; version 1.6.0 was
current at this decision on 2026-07-22.

## Decision

Split Phase 3 into separately approved storage and messaging slices.

Phase 3A uses:

- the already installed CloudNativePG operator `v1.29.2` for the main PostgreSQL
  database;
- `ghcr.io/cloudnative-pg/postgresql:18.4-standard-trixie` pinned to OCI index
  `sha256:4e4ac3fb2c914cfb44f80f0b8be8aa550e83b80bf5220df49c3a8780c1f79bc8`;
- a one-instance, 2 GiB PostgreSQL development Cluster with an external
  `kubernetes.io/basic-auth` Secret, pgvector, a split credential-free bootstrap,
  logical backup/restore testing, and hibernation with PVC retention;
- a single-node, 2 GiB MinIO compatibility StatefulSet only for local S3 contract
  testing. Its image is built from exact commit
  `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a` using a checksum-pinned source
  archive and pinned Go/runtime images;
- a finite `mc` Job from `RELEASE.2025-08-13T08-35-41Z` that creates private
  `warehouse`, `pageviews`, and `postgres-backups` buckets idempotently and checks
  a persistent marker;
- Apache Polaris 1.6.0 as the production-oriented Iceberg REST catalog direction.
  Polaris deployment is deferred until its authentication, JDBC persistence,
  credential vending, and Spark/Trino integration can be tested as one bounded
  catalog sub-slice.

The Helm `batch` overlay enables only Phase 3A storage. Kafka and every processing
or query component remain implementation-gated. The Phase 2 source stays in its
separate Helm release and retained PVC.

## Alternatives considered

- Use the last published MinIO container: rejected because it predates the final
  privilege-escalation security fix.
- Treat the source-built MinIO image as production: rejected because upstream is
  archived and there is no supported upgrade/operator lifecycle.
- Replace the repository's S3 contract immediately: rejected because it would
  combine application compatibility changes with the Kubernetes migration.
- Deploy the Iceberg REST fixture: rejected because it is explicitly a fixture,
  unpinned in Compose, and has unclear persistence/upgrade behavior.
- Deploy Polaris in the first storage runtime: deferred to avoid adding another
  database consumer and catalog security boundary before storage recovery passes.
- Deploy PostgreSQL as a handwritten StatefulSet: rejected in favor of the
  already proven CloudNativePG lifecycle.

## Consequences

The local profile can validate the existing S3 API contract without claiming that
archived MinIO is production-ready. Production must use a supported S3 service or
maintained distribution with TLS, encryption, identity, replication, backup, and
vendor lifecycle commitments. Logical PostgreSQL dump/restore is evidence for this
development slice, not a production PITR design; CloudNativePG's Barman Cloud
plugin remains the production backup direction.

Polaris creates an explicit catalog migration target but no Kubernetes workload
until the catalog sub-slice is approved. Existing Compose files and behavior are
unchanged.

## Risks

Source builds are slower and depend on external Go modules. The checksum and
commit prevent source drift, but rebuilds still need dependency availability.
Single-replica local storage has no high availability. Local HTTP and root MinIO
credentials are constrained to ClusterIP/Secret references but are not a
production security model.

## Validation

Phase 3A requires chart/schema/feature tests, image version checks, private and
idempotent bucket setup, object/PVC persistence across pod recreation, pgvector
and schema assertions, a logical restore with row-count comparison, hibernate and
resume retention, and proof that the Phase 2 PVC identity did not change.

All listed Phase 3A checks passed on 2026-07-22. The final runtime state is
hibernated with the Phase 2 and both Phase 3A PVCs retained. Polaris and messaging
were not deployed.
