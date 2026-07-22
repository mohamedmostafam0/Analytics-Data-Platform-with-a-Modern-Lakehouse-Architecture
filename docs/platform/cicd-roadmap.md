# CI/CD roadmap

## Boundary

Phase 2 adds local build, deployment, smoke, and hibernation helpers for one accepted
slice. It does not add a CI/CD workflow, publish images, or claim that the singleton
development database is production-ready.

## Continuous integration sequence

1. Lint shell, Markdown, YAML, Python, Dockerfiles, and application unit tests.
2. Normalize both Compose files with non-secret validation placeholders.
3. Validate Helm values against JSON Schema.
4. Run Helm lint and render every supported profile.
5. Assert disabled-feature behavior and invalid dependency failures.
6. Validate rendered Kubernetes schemas with `kubeconform` using pinned schemas.
7. Build only changed custom images with immutable source-revision tags.
8. Generate SBOMs and scan dependencies, images, IaC, and committed secrets.
9. Publish signed images only after tests and severity policy pass.

CI must use small fixtures and must not start the full platform. Integration suites
should be split by profile and scheduled on appropriately sized remote runners.

## Continuous delivery sequence

1. Manual development deployment of one accepted slice.
2. Readiness and application smoke checks.
3. Data-contract and replay/idempotency checks.
4. Automated development deployment with concurrency controls.
5. Backup/restore evidence for stateful changes.
6. Staging promotion by immutable artifact digest.
7. Manual production approval until rollback and SLO evidence is mature.
8. Optional GitOps adoption after release boundaries stabilize.

## Release controls

- CI and CD use separate permissions and credentials.
- Deployments use environment protection and least-privilege service accounts.
- Promotion reuses artifacts; it never rebuilds per environment.
- Rollback restores the prior chart/config revision and does not delete PVCs.
- Database/schema changes require forward and backward compatibility plans.
- Feature profiles and chart schema are versioned together.
