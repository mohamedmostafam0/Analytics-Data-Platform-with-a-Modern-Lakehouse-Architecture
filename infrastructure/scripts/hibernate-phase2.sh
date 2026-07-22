#!/usr/bin/env bash
set -Eeuo pipefail

phase2_namespace="lakehouse-platform"
phase2_cluster="items-postgresql"

for phase2_command in kubectl jq; do
  if ! command -v "$phase2_command" >/dev/null 2>&1; then
    echo "ERROR: $phase2_command is required." >&2
    exit 2
  fi
done

kubectl annotate cluster.postgresql.cnpg.io/"$phase2_cluster" \
  --namespace "$phase2_namespace" \
  cnpg.io/hibernation=on \
  --overwrite

for phase2_attempt in {1..60}; do
  phase2_pod_count="$(kubectl get pods \
    --namespace "$phase2_namespace" \
    --selector "cnpg.io/cluster=$phase2_cluster" \
    --output json | jq '.items | length')"
  if [[ "$phase2_pod_count" -eq 0 ]]; then
    echo "Phase 2 PostgreSQL is hibernated; its PVC remains present."
    kubectl get pvc --namespace "$phase2_namespace" --selector "cnpg.io/cluster=$phase2_cluster"
    exit 0
  fi
  sleep 2
done

echo "ERROR: PostgreSQL pods did not stop within 120 seconds." >&2
exit 1
