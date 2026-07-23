#!/usr/bin/env bash
set -Eeuo pipefail

phase3a_namespace="lakehouse-platform"
phase3a_db_secret="lakehouse-main-db-app"
phase3a_minio_secret="lakehouse-minio-root"
phase3a_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase3a_repo_root="$(cd -- "$phase3a_script_dir/../.." && pwd)"
phase3a_secret_tmpdir="$(mktemp -d /tmp/lakehouse-phase3a-secret.XXXXXX)"
trap 'rm -rf -- "$phase3a_secret_tmpdir"' EXIT
umask 077

for phase3a_command in kubectl openssl; do
  if ! command -v "$phase3a_command" >/dev/null 2>&1; then
    echo "ERROR: $phase3a_command is required." >&2
    exit 2
  fi
done

kubectl apply --filename \
  "$phase3a_repo_root/infrastructure/kubernetes/namespaces/lakehouse-platform.yaml" >/dev/null

validate_existing_secret() {
  local secret_name="$1"
  local expected_type="$2"
  shift 2

  if ! kubectl get secret "$secret_name" --namespace "$phase3a_namespace" >/dev/null 2>&1; then
    return 1
  fi

  local actual_type
  actual_type="$(kubectl get secret "$secret_name" --namespace "$phase3a_namespace" \
    --output jsonpath='{.type}')"
  if [[ "$actual_type" != "$expected_type" ]]; then
    echo "ERROR: existing Secret '$secret_name' has type '$actual_type', expected '$expected_type'; it was not changed." >&2
    exit 1
  fi

  local key
  local encoded_value
  for key in "$@"; do
    encoded_value="$(kubectl get secret "$secret_name" --namespace "$phase3a_namespace" \
      --output "jsonpath={.data.$key}")"
    if [[ -z "$encoded_value" ]]; then
      echo "ERROR: existing Secret '$secret_name' is missing '$key'; it was not changed." >&2
      exit 1
    fi
  done
  echo "Reusing '$phase3a_namespace/$secret_name'; no credential was changed."
  return 0
}

if ! validate_existing_secret "$phase3a_db_secret" kubernetes.io/basic-auth username password; then
  printf '%s' 'platform_app' >"$phase3a_secret_tmpdir/db-username"
  openssl rand -hex 32 | tr -d '\n' >"$phase3a_secret_tmpdir/db-password"
  kubectl create secret generic "$phase3a_db_secret" \
    --namespace "$phase3a_namespace" \
    --type kubernetes.io/basic-auth \
    --from-file="username=$phase3a_secret_tmpdir/db-username" \
    --from-file="password=$phase3a_secret_tmpdir/db-password" \
    --dry-run=client \
    --output yaml | kubectl apply --filename - >/dev/null
  echo "Created '$phase3a_namespace/$phase3a_db_secret' without printing credential data."
fi

if ! validate_existing_secret "$phase3a_minio_secret" Opaque rootUser rootPassword; then
  printf '%s' 'lakehouseadmin' >"$phase3a_secret_tmpdir/minio-username"
  openssl rand -hex 32 | tr -d '\n' >"$phase3a_secret_tmpdir/minio-password"
  kubectl create secret generic "$phase3a_minio_secret" \
    --namespace "$phase3a_namespace" \
    --type Opaque \
    --from-file="rootUser=$phase3a_secret_tmpdir/minio-username" \
    --from-file="rootPassword=$phase3a_secret_tmpdir/minio-password" \
    --dry-run=client \
    --output yaml | kubectl apply --filename - >/dev/null
  echo "Created '$phase3a_namespace/$phase3a_minio_secret' without printing credential data."
fi
