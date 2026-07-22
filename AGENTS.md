# Repository guidance

This repository is a laptop-oriented modern lakehouse demonstration. Its current
runtime is Docker Compose; Kubernetes migration is incremental and must preserve
that implementation. Read `docs/plans/kubernetes-migration.md` before changing
deployment code.

## Important paths

- `docker-compose.yaml`: 24-service core platform definition.
- `airflow.yaml`: separate five-service Airflow definition.
- `docs/platform/`: discovered architecture, inventory, profiles, and risks.
- `infrastructure/helm/lakehouse-platform/`: Kubernetes feature flags and chart.
- `infrastructure/kind/`: pinned disposable local-cluster configuration.
- `infrastructure/kubernetes/`: non-Helm bootstrap manifests and guidance.
- `infrastructure/operators/`: pinned operator decisions and checksums.
- `infrastructure/scripts/`: static checks and explicit Phase 2 lifecycle helpers.

## Safe laptop commands

```bash
docker compose config -q
docker compose -f airflow.yaml config -q
infrastructure/scripts/check-local-prerequisites.sh
infrastructure/scripts/validate-platform.sh
infrastructure/scripts/render-profile.sh minimal
```

Static validation is the default. Do not run the full Compose stack, build every
image, deploy all components, pull large stacks, or run integration tests unless
the user explicitly approves the resource cost. Starting Kafka, Spark, Flink,
Airflow, Trino, ClickHouse, MinIO, OpenSearch, and Superset together requires
explicit approval.

The Phase 2 runtime helpers start only kind, CloudNativePG, and `items-loadgen`.
They require explicit runtime approval and must not be generalized to later phases.
Hibernation retains the PostgreSQL PVC; cluster/image/PVC deletion is never an
automatic validation step.

## Safety and migration rules

- Never delete volumes, `airflow/db`, `warehouse`, Superset state, or user data.
- Never print, commit, rotate, or copy values from `.env` or other credentials.
- Do not move, rename, or alter Compose behavior for Kubernetes convenience.
- Work one migration phase at a time and stop at its approval gate.
- Keep the default Helm profile foundation-only and laptop-safe.
- Centralize component switches under `features` in the chart values.
- A disabled feature must render no workload, service, storage, RBAC, or secret.
- Fail early for unmet dependencies and for features not yet migrated.
- Pin every newly introduced chart, container image, and downloaded artifact;
  never introduce `latest`.
- Put secrets in references to Kubernetes Secrets, never in committed values.
- Add standard `app.kubernetes.io/*` labels to every Kubernetes resource.
- Do not add a liveness probe or security setting without verifying compatibility.

## Documentation and tests

Update the service inventory, dependency map, feature matrix, risks, ADRs, and
execution plan when deployment behavior or a migration decision changes. Run
`infrastructure/scripts/validate-platform.sh` after infrastructure changes. If an
optional validator is unavailable, record the skipped check; do not install tools
silently. Phase acceptance criteria and the next approved action live in
`docs/plans/kubernetes-migration.md`.
