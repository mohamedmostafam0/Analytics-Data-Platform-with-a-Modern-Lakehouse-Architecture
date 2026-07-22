#!/usr/bin/env bash
set -Eeuo pipefail

phase2_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase2_repo_root="$(cd -- "$phase2_script_dir/../.." && pwd)"
phase2_chart="$phase2_repo_root/infrastructure/helm/lakehouse-platform"
phase2_namespace="lakehouse-platform"
phase2_cluster="items-postgresql"
phase2_release="lakehouse-platform"
phase2_expected_count=100

for phase2_command in kubectl helm; do
  if ! command -v "$phase2_command" >/dev/null 2>&1; then
    echo "ERROR: $phase2_command is required." >&2
    exit 2
  fi
done

helm upgrade --install "$phase2_release" "$phase2_chart" \
  --namespace "$phase2_namespace" \
  --values "$phase2_chart/values-minimal.yaml" \
  --set namespace.create=false \
  --wait \
  --timeout 10m

kubectl wait cluster.postgresql.cnpg.io/"$phase2_cluster" \
  --namespace "$phase2_namespace" \
  --for=condition=Ready \
  --timeout=10m
kubectl wait job \
  --namespace "$phase2_namespace" \
  --selector app.kubernetes.io/component=items-load-generator \
  --for=condition=Complete \
  --timeout=10m

phase2_primary_pod="$(kubectl get pod \
  --namespace "$phase2_namespace" \
  --selector "cnpg.io/cluster=$phase2_cluster,role=primary" \
  --output jsonpath='{.items[0].metadata.name}')"
phase2_first_count="$(kubectl exec --namespace "$phase2_namespace" "$phase2_primary_pod" -- \
  psql --dbname oneshop --tuples-only --no-align --command 'SELECT count(*) FROM items;')"

if [[ "$phase2_first_count" != "$phase2_expected_count" ]]; then
  echo "ERROR: expected $phase2_expected_count items; found $phase2_first_count." >&2
  exit 1
fi

kubectl delete job \
  --namespace "$phase2_namespace" \
  --selector app.kubernetes.io/component=items-load-generator \
  --wait=true >/dev/null
helm upgrade "$phase2_release" "$phase2_chart" \
  --namespace "$phase2_namespace" \
  --values "$phase2_chart/values-minimal.yaml" \
  --set namespace.create=false \
  --wait \
  --timeout 10m
kubectl wait job \
  --namespace "$phase2_namespace" \
  --selector app.kubernetes.io/component=items-load-generator \
  --for=condition=Complete \
  --timeout=10m

phase2_second_count="$(kubectl exec --namespace "$phase2_namespace" "$phase2_primary_pod" -- \
  psql --dbname oneshop --tuples-only --no-align --command 'SELECT count(*) FROM items;')"
if [[ "$phase2_second_count" != "$phase2_first_count" ]]; then
  echo "ERROR: rerun changed item count from $phase2_first_count to $phase2_second_count." >&2
  exit 1
fi

if ! kubectl logs \
  --namespace "$phase2_namespace" \
  --selector app.kubernetes.io/component=items-load-generator \
  --tail=50 | grep -Fq 'skip-if-present mode made no changes'; then
  echo "ERROR: second Job did not report the expected idempotent skip." >&2
  exit 1
fi
echo "Phase 2 smoke test passed: $phase2_first_count items; rerun remained $phase2_second_count."
