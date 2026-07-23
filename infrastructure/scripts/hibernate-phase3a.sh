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

kubectl annotate cluster.postgresql.cnpg.io/"$phase3a_cluster" \
  --namespace "$phase3a_namespace" \
  cnpg.io/hibernation=on \
  --overwrite >/dev/null
kubectl scale statefulset "$phase3a_minio_statefulset" \
  --namespace "$phase3a_namespace" \
  --replicas=0 >/dev/null

for phase3a_attempt in {1..60}; do
  phase3a_pg_pods="$(kubectl get pods --namespace "$phase3a_namespace" \
    --selector "cnpg.io/cluster=$phase3a_cluster" \
    --output jsonpath='{.items[*].metadata.name}')"
  phase3a_minio_pods="$(kubectl get pods --namespace "$phase3a_namespace" \
    --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio" \
    --output jsonpath='{.items[*].metadata.name}')"
  if [[ -z "$phase3a_pg_pods" && -z "$phase3a_minio_pods" ]]; then
    echo "Phase 3A storage is hibernated; PostgreSQL and MinIO PVCs remain present."
    kubectl get pvc --namespace "$phase3a_namespace" \
      --selector 'app.kubernetes.io/part-of=lakehouse-platform'
    exit 0
  fi
  sleep 2
done

echo "ERROR: Phase 3A storage pods did not stop within 120 seconds." >&2
exit 1
