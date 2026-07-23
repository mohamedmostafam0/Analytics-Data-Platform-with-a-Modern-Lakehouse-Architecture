# Lakehouse platform chart

This chart keeps base values foundation-only. The explicit `minimal` overlay
enables the accepted Phase 2 CloudNativePG source and finite item-seeding Job. The
`batch` overlay currently enables only the approved Phase 3A main PostgreSQL and
local object-storage foundation. Every later component fails closed at the
implementation gate.

| Overlay | Implemented workloads | Release contract |
| --- | --- | --- |
| Base and deferred profiles | None | Namespace only when `namespace.create=true` |
| `minimal` | Source CloudNativePG and finite item seeder | Install as `lakehouse-platform` |
| `batch` | Main CloudNativePG, local S3 StatefulSet, finite private-bucket bootstrap | Install as `lakehouse-storage` |

Do not upgrade one live release from `minimal` to `batch` or vice versa. The
overlays represent separate accepted slices with independent resource ownership
and retained storage, not interchangeable runtime presets.

## Profiles

Available overlays are `minimal`, `ingestion`, `streaming`, `batch`, `analytics`,
`observability`, `logging`, and `full`. `minimal` requires
`lakehouse-items-db-app` with `username` and `password` keys and type
`kubernetes.io/basic-auth`. `batch` requires `lakehouse-main-db-app` with the same
type/key contract and `lakehouse-minio-root` type `Opaque` with `rootUser` and
`rootPassword`. Values created from files must not contain trailing newlines. The
chart never creates or stores those values.

```bash
helm lint . -f values-minimal.yaml
helm template lakehouse-platform . -f values-minimal.yaml
../../scripts/render-profile.sh minimal
../../scripts/render-profile.sh batch
```

The Job image must be built and loaded into kind under
`lakehouse/items-loadgen:phase2-0.1.0`; `imagePullPolicy: Never` prevents an
unexpected registry fallback. The PostgreSQL image uses an immutable OCI digest.
CNPG `v1.29.2` and its CRDs must be installed first.

Phase 3A uses a separate `lakehouse-storage` release so it cannot remove or adopt
the retained Phase 2 objects. Build/load the local images with
`../../scripts/build-phase3a-images.sh`, create/reuse Secret references with
`../../scripts/create-phase3a-secrets.sh`, and run
`../../scripts/phase3a-smoke-test.sh`. The MinIO server image is built from the
checksum-pinned final fixed source because upstream stopped publishing community
containers; it is a local compatibility target, not a production object-storage
recommendation. See
[ADR-008](../../../docs/adr/008-phase3a-storage-and-catalog-strategy.md).

The Phase 3A runtime order is:

```bash
../../scripts/build-phase3a-images.sh
../../scripts/create-phase3a-secrets.sh
../../scripts/phase3a-smoke-test.sh
```

The smoke helper resumes retained state when necessary, tests it, and returns the
stateful workloads to hibernation. `resume-phase3a.sh` is an explicit operational
action; a subsequent Helm upgrade may restore the StatefulSet replica count and
must be followed by `hibernate-phase3a.sh` when the test window ends.

The local lifecycle helper creates the raw namespace before the Secret, so its
Helm command uses `--set namespace.create=false`. Direct renders keep namespace
creation enabled to verify the chart's self-contained profile behavior.

No liveness/readiness probe is added to the finite Job because it has no service
endpoint; completion is its health signal. CloudNativePG manages database startup,
readiness, and instance security. `enablePDB: false` is intentional for the
single-instance disposable development Cluster.

The MinIO StatefulSet uses its verified `/minio/health/ready` and
`/minio/health/live` endpoints, non-root/read-only security, ClusterIP API-only
exposure, a retained PVC, and no service-account token. Its client is a finite,
idempotent Job: all buckets are explicitly private and a marker proves object
persistence after pod recreation.

## Production boundary

The chart structure, validation, feature isolation, security contexts, resource
guardrails, Secret references, and retention tests are production-minded. The
current values are not production values: both databases are single-instance,
PDBs are intentionally disabled, storage classes are local defaults, no TLS or
external secret controller is configured, PostgreSQL has only a logical restore
drill, the S3 target is archived/local-only, and NetworkPolicy enforcement depends
on a capable cluster CNI. No production deployment should use
`values-minimal.yaml` or `values-batch.yaml` directly.

## Change contract

When migrating a component, update together:

1. `values.yaml` and `values.schema.json`;
2. `templates/feature-validation.yaml`;
3. the relevant profile overlays;
4. the feature matrix, inventory, ADR, and migration plan;
5. render/invalid-combination tests.
