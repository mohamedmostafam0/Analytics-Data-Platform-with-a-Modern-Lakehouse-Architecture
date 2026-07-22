# ADR-003: Compose and Kubernetes coexistence

## Status

Accepted

## Context

Docker Compose is the only working deployment and carries local state. A full
cutover is neither safe nor possible on the target laptop.

## Decision

Keep `docker-compose.yaml` and `airflow.yaml` authoritative for every component
until that component passes its Kubernetes phase acceptance criteria. Kubernetes
files live under `infrastructure/` and do not change Compose paths or formats.
Mixed-mode testing must be explicit and use non-conflicting endpoints and data.

## Alternatives considered

- Immediate replacement: unacceptable risk to a working development platform.
- Generate Compose and Kubernetes from one template: would change current behavior
  and hide environment-specific semantics.
- Run both complete platforms concurrently: exceeds laptop resources and risks
  duplicate writers.

## Consequences

Rollback is disabling or uninstalling the Kubernetes slice and returning to the
unchanged Compose service. Configuration may temporarily be duplicated where the
orchestrators require different formats.

## Risks

Running duplicate producers, CDC connectors, schedulers, or seed jobs can corrupt
tests or create duplicate data. Compose project naming also controls whether the
two Compose files share their logical `iceberg_net` network.

## Validation

Run static Compose normalization before and after every migration change and
record any compatibility change in the coexistence document.
