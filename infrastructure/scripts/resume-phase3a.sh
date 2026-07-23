#!/usr/bin/env bash
set -Eeuo pipefail

phase3a_namespace="lakehouse-platform"
phase3a_cluster="platform-postgresql"
phase3a_release="lakehouse-storage"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is required." >&2
  exit 2
fi

phase3a_minio_statefulset="$(kubectl get statefulset \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio" \
  --output jsonpath='{.items[0].metadata.name}')"
if [[ -z "$phase3a_minio_statefulset" ]]; then
  echo "ERROR: the Phase 3A MinIO StatefulSet was not found." >&2
  exit 1
fi

phase3a_hibernation="$(kubectl get cluster.postgresql.cnpg.io/"$phase3a_cluster" \
  --namespace "$phase3a_namespace" \
  --output jsonpath='{.metadata.annotations.cnpg\.io/hibernation}')"
if [[ "$phase3a_hibernation" == "on" ]]; then
  kubectl annotate cluster.postgresql.cnpg.io/"$phase3a_cluster" \
    --namespace "$phase3a_namespace" \
    cnpg.io/hibernation- >/dev/null
fi
kubectl scale statefulset "$phase3a_minio_statefulset" \
  --namespace "$phase3a_namespace" \
  --replicas=1 >/dev/null

kubectl rollout status statefulset/"$phase3a_minio_statefulset" \
  --namespace "$phase3a_namespace" \
  --timeout=5m

phase3a_primary_pod=""
for phase3a_attempt in {1..60}; do
  phase3a_primary_pod="$(kubectl get pods \
    --namespace "$phase3a_namespace" \
    --selector "cnpg.io/cluster=$phase3a_cluster,role=primary" \
    --output jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$phase3a_primary_pod" ]]; then
    break
  fi
  sleep 2
done
if [[ -z "$phase3a_primary_pod" ]]; then
  echo "ERROR: the Phase 3A primary PostgreSQL pod did not appear within 120 seconds." >&2
  exit 1
fi
kubectl wait pod/"$phase3a_primary_pod" \
  --namespace "$phase3a_namespace" \
  --for=condition=Ready \
  --timeout=5m
kubectl wait cluster.postgresql.cnpg.io/"$phase3a_cluster" \
  --namespace "$phase3a_namespace" \
  --for=condition=Ready \
  --timeout=5m

echo "Phase 3A storage resumed from retained PVCs."
