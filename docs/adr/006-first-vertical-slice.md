# ADR-006: First Kubernetes vertical slice

## Status

Accepted

## Context

The first slice must demonstrate real work without pulling in the messaging,
processing, analytics, or observability stacks. The existing `items-loadgen` is a
small custom Python program with one PostgreSQL dependency and a finite execution
model suitable for a Kubernetes Job.

## Decision

Migrate `items-loadgen` first as a finite Kubernetes Job with one single-instance
CloudNativePG development Cluster. Pin CloudNativePG to `v1.29.2`, PostgreSQL to
the immutable `18.4-system-trixie` OCI index, allocate 1 GiB storage, and seed 100
rows in the `minimal` profile. The database is a Phase 2 source dependency named
`sourcePostgresql`; it is intentionally separate from the later CDC database.

The default rerun policy is `skip-if-present`. A transaction-scoped advisory lock
serializes concurrent seeders; explicit `append` remains available for intentional
fixture growth. Do not include Kafka Connect, Kafka, Schema Registry, Debezium, or
OpenSearch in Phase 2.

## Alternatives considered

- Login generator through Kafka and Flink: stronger streaming demonstration but
  substantially more resource-intensive.
- Streamlit: its image includes ML dependencies and its pages require ClickHouse
  and PostgreSQL.
- Airflow: multiple services, a metadata database, and cross-stack DAG dependencies.
- System load generator: requires both PostgreSQL and MinIO and creates much more
  synthetic data.

## Consequences

The slice proves image configuration, secret references, readiness gating,
one-shot execution, logs, idempotency expectations, and rollback with low resource
cost. It does not yet prove CDC or event streaming.

## Risks

This is a singleton development database, not a highly available production
design. Hibernation is the safe local rollback because it stops the instance while
retaining its PVC. The locally built Job image is loaded directly into kind with
an immutable phase tag and `imagePullPolicy: Never`; publishing and registry digest
pinning remain Phase 8 work.

## Validation

Phase 2 acceptance requires a successful Job, exactly 100-row evidence, an
unchanged count on a second run, no credential output, explicit resource controls,
NetworkPolicies, hardened Job security, and tested hibernation without PVC deletion.
