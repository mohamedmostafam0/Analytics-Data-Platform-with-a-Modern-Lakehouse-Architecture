#!/usr/bin/env bash
set -Eeuo pipefail

phase3a_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase3a_repo_root="$(cd -- "$phase3a_script_dir/../.." && pwd)"
phase3a_chart="$phase3a_repo_root/infrastructure/helm/lakehouse-platform"
phase3a_namespace="lakehouse-platform"
phase3a_cluster="platform-postgresql"
phase3a_release="lakehouse-storage"
phase3a_backup_tmpdir="$(mktemp -d /tmp/lakehouse-phase3a-backup.XXXXXX)"
phase3a_primary_pod=""
phase3a_restore_created=false
phase3a_should_hibernate=false

phase3a_cleanup() {
  local phase3a_exit_code="$?"
  trap - EXIT
  if [[ "$phase3a_restore_created" == true && -n "$phase3a_primary_pod" ]]; then
    kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
      psql --dbname postgres --command \
      'DROP DATABASE IF EXISTS phase3a_restore WITH (FORCE);' >/dev/null 2>&1 || true
  fi
  if [[ "$phase3a_should_hibernate" == true ]]; then
    "$phase3a_script_dir/hibernate-phase3a.sh" >/dev/null 2>&1 || \
      echo "WARNING: automatic Phase 3A hibernation failed; inspect the local cluster." >&2
  fi
  rm -rf -- "$phase3a_backup_tmpdir"
  exit "$phase3a_exit_code"
}
trap phase3a_cleanup EXIT

for phase3a_command in kubectl helm; do
  if ! command -v "$phase3a_command" >/dev/null 2>&1; then
    echo "ERROR: $phase3a_command is required." >&2
    exit 2
  fi
done

phase2_pvc_before="$(kubectl get pvc --namespace "$phase3a_namespace" \
  --selector 'cnpg.io/cluster=items-postgresql' \
  --output jsonpath='{.items[*].metadata.uid}')"

helm upgrade --install "$phase3a_release" "$phase3a_chart" \
  --namespace "$phase3a_namespace" \
  --values "$phase3a_chart/values-batch.yaml" \
  --set namespace.create=false \
  --wait \
  --wait-for-jobs \
  --timeout 15m
phase3a_should_hibernate=true
"$phase3a_script_dir/resume-phase3a.sh"

kubectl wait cluster.postgresql.cnpg.io/"$phase3a_cluster" \
  --namespace "$phase3a_namespace" \
  --for=condition=Ready \
  --timeout=10m
kubectl wait job \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio-client" \
  --for=condition=Complete \
  --timeout=5m

phase3a_primary_pod="$(kubectl get pod \
  --namespace "$phase3a_namespace" \
  --selector "cnpg.io/cluster=$phase3a_cluster,role=primary" \
  --output jsonpath='{.items[0].metadata.name}')"
phase3a_schema_result="$(kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname oneshop --tuples-only --no-align --command \
  "SELECT extname FROM pg_extension WHERE extname='vector'; SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname='public' AND tablename IN ('users','items','purchases','reviews'); SELECT count(*) FROM reviews;")"
if [[ "$phase3a_schema_result" != $'vector\nitems,purchases,reviews,users\n13' ]]; then
  echo "ERROR: the main PostgreSQL schema or fixtures do not match the Phase 3A contract." >&2
  exit 1
fi

phase3a_counts_before="$(kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname oneshop --tuples-only --no-align --command \
  "SELECT (SELECT count(*) FROM users) || ',' || (SELECT count(*) FROM items) || ',' || (SELECT count(*) FROM purchases) || ',' || (SELECT count(*) FROM reviews);")"

kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  pg_dump --dbname oneshop --format=custom >"$phase3a_backup_tmpdir/oneshop.dump"
if [[ ! -s "$phase3a_backup_tmpdir/oneshop.dump" ]]; then
  echo "ERROR: logical PostgreSQL backup is empty." >&2
  exit 1
fi
kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname postgres --set ON_ERROR_STOP=1 --command \
  'DROP DATABASE IF EXISTS phase3a_restore WITH (FORCE);' >/dev/null
kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname postgres --set ON_ERROR_STOP=1 --command \
  'CREATE DATABASE phase3a_restore OWNER platform_app;' >/dev/null
phase3a_restore_created=true
kubectl exec --stdin --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  pg_restore --dbname phase3a_restore --no-owner <"$phase3a_backup_tmpdir/oneshop.dump"
phase3a_counts_restored="$(kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname phase3a_restore --tuples-only --no-align --command \
  "SELECT (SELECT count(*) FROM users) || ',' || (SELECT count(*) FROM items) || ',' || (SELECT count(*) FROM purchases) || ',' || (SELECT count(*) FROM reviews);")"
if [[ "$phase3a_counts_restored" != "$phase3a_counts_before" ]]; then
  echo "ERROR: restored PostgreSQL row counts do not match the source database." >&2
  exit 1
fi
kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname postgres --set ON_ERROR_STOP=1 --command \
  'DROP DATABASE phase3a_restore WITH (FORCE);' >/dev/null
phase3a_restore_created=false

phase3a_minio_statefulset="$(kubectl get statefulset \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio" \
  --output jsonpath='{.items[0].metadata.name}')"
phase3a_minio_pvc="$(kubectl get pvc \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio" \
  --output jsonpath='{.items[0].metadata.uid}')"
kubectl delete pod \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio" \
  --wait >/dev/null
kubectl rollout status statefulset/"$phase3a_minio_statefulset" \
  --namespace "$phase3a_namespace" \
  --timeout=5m

kubectl delete job \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio-client" \
  --wait >/dev/null
helm upgrade "$phase3a_release" "$phase3a_chart" \
  --namespace "$phase3a_namespace" \
  --values "$phase3a_chart/values-batch.yaml" \
  --set namespace.create=false \
  --wait \
  --wait-for-jobs \
  --timeout 10m
if ! kubectl logs \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio-client" \
  --tail=50 | grep -Fq 'Retained existing Phase 3A persistence marker.'; then
  echo "ERROR: object-storage marker was not retained across the pod restart." >&2
  exit 1
fi
phase3a_minio_pvc_after="$(kubectl get pvc \
  --namespace "$phase3a_namespace" \
  --selector "app.kubernetes.io/instance=$phase3a_release,app.kubernetes.io/component=minio" \
  --output jsonpath='{.items[0].metadata.uid}')"
if [[ -z "$phase3a_minio_pvc" || "$phase3a_minio_pvc_after" != "$phase3a_minio_pvc" ]]; then
  echo "ERROR: MinIO did not retain the same PVC across restart." >&2
  exit 1
fi

"$phase3a_script_dir/hibernate-phase3a.sh"
"$phase3a_script_dir/resume-phase3a.sh"
phase3a_primary_pod="$(kubectl get pod \
  --namespace "$phase3a_namespace" \
  --selector "cnpg.io/cluster=$phase3a_cluster,role=primary" \
  --output jsonpath='{.items[0].metadata.name}')"
phase3a_counts_after_resume="$(kubectl exec --namespace "$phase3a_namespace" "$phase3a_primary_pod" -- \
  psql --dbname oneshop --tuples-only --no-align --command \
  "SELECT (SELECT count(*) FROM users) || ',' || (SELECT count(*) FROM items) || ',' || (SELECT count(*) FROM purchases) || ',' || (SELECT count(*) FROM reviews);")"
if [[ "$phase3a_counts_after_resume" != "$phase3a_counts_before" ]]; then
  echo "ERROR: PostgreSQL row counts changed across hibernation." >&2
  exit 1
fi

phase2_pvc_after="$(kubectl get pvc --namespace "$phase3a_namespace" \
  --selector 'cnpg.io/cluster=items-postgresql' \
  --output jsonpath='{.items[*].metadata.uid}')"
if [[ "$phase2_pvc_after" != "$phase2_pvc_before" ]]; then
  echo "ERROR: the retained Phase 2 PostgreSQL PVC changed during Phase 3A." >&2
  exit 1
fi

"$phase3a_script_dir/hibernate-phase3a.sh"
phase3a_should_hibernate=false
echo "Phase 3A smoke test passed: schema, 13 fixtures, logical restore, object persistence, hibernation, and Phase 2 PVC isolation."
