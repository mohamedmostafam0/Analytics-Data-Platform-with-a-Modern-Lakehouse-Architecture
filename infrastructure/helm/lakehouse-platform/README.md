# Lakehouse platform chart

This Phase 2 chart keeps base values foundation-only. The explicit `minimal`
overlay enables one disposable CloudNativePG source and the finite item-seeding
Job. Every larger component still fails closed at the implementation gate.

## Profiles

Available overlays are `minimal`, `ingestion`, `streaming`, `batch`, `analytics`,
`observability`, `logging`, and `full`. Only `minimal` currently enables workloads.
Its existing Secret prerequisite is `lakehouse-items-db-app` with `username` and
`password` keys and type `kubernetes.io/basic-auth`; values created from files must
not contain trailing newlines. The chart never creates or stores those values.

```bash
helm lint . -f values-minimal.yaml
helm template lakehouse-platform . -f values-minimal.yaml
../../scripts/render-profile.sh minimal
```

The Job image must be built and loaded into kind under
`lakehouse/items-loadgen:phase2-0.1.0`; `imagePullPolicy: Never` prevents an
unexpected registry fallback. The PostgreSQL image uses an immutable OCI digest.
CNPG `v1.29.2` and its CRDs must be installed first.

The local lifecycle helper creates the raw namespace before the Secret, so its
Helm command uses `--set namespace.create=false`. Direct renders keep namespace
creation enabled to verify the chart's self-contained profile behavior.

No liveness/readiness probe is added to the finite Job because it has no service
endpoint; completion is its health signal. CloudNativePG manages database startup,
readiness, and instance security. `enablePDB: false` is intentional for the
single-instance disposable development Cluster.

## Change contract

When migrating a component, update together:

1. `values.yaml` and `values.schema.json`;
2. `templates/feature-validation.yaml`;
3. the relevant profile overlays;
4. the feature matrix, inventory, ADR, and migration plan;
5. render/invalid-combination tests.
