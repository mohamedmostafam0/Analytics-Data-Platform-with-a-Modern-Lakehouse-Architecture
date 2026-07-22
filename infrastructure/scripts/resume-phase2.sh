#!/usr/bin/env bash
set -Eeuo pipefail

phase2_namespace="lakehouse-platform"
phase2_cluster="items-postgresql"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is required." >&2
  exit 2
fi

phase2_hibernation="$(kubectl get cluster.postgresql.cnpg.io/"$phase2_cluster" \
  --namespace "$phase2_namespace" \
  --output jsonpath='{.metadata.annotations.cnpg\.io/hibernation}')"
if [[ "$phase2_hibernation" == "on" ]]; then
  kubectl annotate cluster.postgresql.cnpg.io/"$phase2_cluster" \
    --namespace "$phase2_namespace" \
    cnpg.io/hibernation-
fi

phase2_primary_pod=""
for phase2_attempt in {1..60}; do
  phase2_primary_pod="$(kubectl get pods \
    --namespace "$phase2_namespace" \
    --selector "cnpg.io/cluster=$phase2_cluster,role=primary" \
    --output jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$phase2_primary_pod" ]]; then
    break
  fi
  sleep 2
done
if [[ -z "$phase2_primary_pod" ]]; then
  echo "ERROR: a primary PostgreSQL pod did not appear within 120 seconds." >&2
  exit 1
fi

kubectl wait pod/"$phase2_primary_pod" \
  --namespace "$phase2_namespace" \
  --for=condition=Ready \
  --timeout=5m
kubectl wait cluster.postgresql.cnpg.io/"$phase2_cluster" \
  --namespace "$phase2_namespace" \
  --for=condition=Ready \
  --timeout=5m

echo "Phase 2 PostgreSQL resumed from its retained PVC."
