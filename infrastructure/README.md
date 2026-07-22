# Platform infrastructure

This directory is the Kubernetes migration boundary. Docker Compose remains
unchanged and authoritative outside the explicitly migrated Phase 2 slice.

| Path | Purpose |
| --- | --- |
| `helm/lakehouse-platform/` | Central feature flags, dependency validation, profiles, and namespace template |
| `kind/` | Pinned disposable Phase 2 cluster configuration |
| `kubernetes/` | Bootstrap manifest and Kubernetes-specific operating guidance |
| `operators/cloudnative-pg/` | Operator/version/checksum decision record |
| `scripts/` | Static validation plus explicit Phase 2 lifecycle helpers |

Base values contain no application workload. The `minimal` overlay creates one
CloudNativePG Cluster, schema ConfigMap, item-seeding Job, and NetworkPolicies. It
references an externally created Secret and never commits credential material.

```bash
infrastructure/scripts/check-local-prerequisites.sh
infrastructure/scripts/validate-platform.sh
infrastructure/scripts/render-profile.sh minimal
```

Runtime scripts are intentionally separate from static validation. Run them only
for the approved Phase 2 test, in this order: tool download, kind creation, CNPG
installation, image build/load, Secret creation, smoke test, and optional
hibernation. `resume-phase2.sh` reverses hibernation without recreating storage.
No cleanup script is supplied because PVC/image deletion requires an explicit
decision.

See `docs/plans/kubernetes-migration.md` for the phase gate and
`docs/platform/compose-kubernetes-coexistence.md` before mixed-mode testing.
