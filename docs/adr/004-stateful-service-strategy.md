# ADR-004: Stateful-service deployment strategy

## Status

Accepted

## Context

Kafka, PostgreSQL, MinIO, Airflow, Flink, ClickHouse, and OpenSearch have lifecycle,
upgrade, backup, and recovery needs that exceed a basic StatefulSet template.

## Decision

Do not hand-copy these systems into the platform chart. For each migration phase,
evaluate a maintained operator, a maintained pinned Helm chart, an external
service, and a development-only substitute. Record the chosen artifact, version,
upgrade path, backup behavior, security posture, and laptop footprint before
adding it. The platform chart owns feature policy and integration contracts, not
vendor internals.

## Alternatives considered

- Hand-written StatefulSets for every service: simple initially, costly and risky
  over upgrades and failure recovery.
- Operators for every local component: operationally rich but too heavy for the
  default laptop profile.
- External managed services only: unsuitable for offline portfolio development.

## Consequences

Stateful migration needs explicit selection gates. Local and production strategies
may differ, but their data and configuration contracts must be documented.

## Risks

Third-party charts can change ownership, licensing, repositories, or defaults.
Operators add CRDs and cluster-scoped permissions. These facts require verification
at the time of selection.

## Validation

Each later ADR must include restore testing, persistent-volume ownership, upgrade
testing, and an uninstall path that does not delete data by default.
