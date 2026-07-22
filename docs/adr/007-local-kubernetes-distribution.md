# ADR-007: Local Kubernetes distribution

## Status

Accepted

## Context

Phase 2 needs a disposable, low-overhead Kubernetes cluster on a Docker-capable
developer laptop. No kind, k3d, minikube, kubectl, or system Helm installation was
present. Docker already has enough measured CPU and memory, and existing user
images and volumes must not be cleaned up.

## Decision

Use kind `v0.31.0` with a single control-plane node running Kubernetes `v1.35.0`.
Pin the node OCI digest in `infrastructure/kind/phase2.yaml`. Download kind,
kubectl `v1.35.0`, and Helm `v3.21.1` into `/tmp` with committed SHA-256 checks;
do not install them system-wide.

## Alternatives considered

- k3d: also lightweight, but introduces a k3s-specific runtime and was absent.
- minikube: broader driver/add-on surface and unnecessary for this one-node test.
- Docker Desktop Kubernetes: no enabled cluster was found and it is less disposable.

## Consequences

The Phase 2 cluster is isolated, reproducible, and removable independently of
Compose. Local images must be loaded into the kind node. The single node does not
validate high availability, disruption behavior, or multi-zone scheduling.

## Risks

The node image and PostgreSQL/operator images consume disk space. No automated
script deletes the cluster, images, volumes, or PVCs; cleanup remains an explicit
user decision.

## Validation

Verify checksums, client versions, node readiness, server version, operator
rollout, and the end-to-end Phase 2 smoke test. Record observed runtime image IDs.
