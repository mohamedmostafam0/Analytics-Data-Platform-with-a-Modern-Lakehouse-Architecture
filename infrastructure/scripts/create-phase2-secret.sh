#!/usr/bin/env bash
set -Eeuo pipefail

phase2_namespace="lakehouse-platform"
phase2_secret_name="lakehouse-items-db-app"
phase2_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase2_repo_root="$(cd -- "$phase2_script_dir/../.." && pwd)"
phase2_secret_tmpdir="$(mktemp -d /tmp/lakehouse-phase2-secret.XXXXXX)"
trap 'rm -rf -- "$phase2_secret_tmpdir"' EXIT
umask 077

for phase2_command in kubectl openssl; do
  if ! command -v "$phase2_command" >/dev/null 2>&1; then
    echo "ERROR: $phase2_command is required." >&2
    exit 2
  fi
done

kubectl apply --filename "$phase2_repo_root/infrastructure/kubernetes/namespaces/lakehouse-platform.yaml" >/dev/null

if kubectl get secret "$phase2_secret_name" --namespace "$phase2_namespace" >/dev/null 2>&1; then
  phase2_secret_type="$(kubectl get secret "$phase2_secret_name" \
    --namespace "$phase2_namespace" \
    --output jsonpath='{.type}')"
  phase2_username_data="$(kubectl get secret "$phase2_secret_name" \
    --namespace "$phase2_namespace" \
    --output jsonpath='{.data.username}')"
  phase2_password_data="$(kubectl get secret "$phase2_secret_name" \
    --namespace "$phase2_namespace" \
    --output jsonpath='{.data.password}')"
  if [[ "$phase2_secret_type" != "kubernetes.io/basic-auth" || -z "$phase2_username_data" || -z "$phase2_password_data" ]]; then
    echo "ERROR: existing Secret must be type kubernetes.io/basic-auth with username and password keys; it was not changed." >&2
    exit 1
  fi
  echo "Reusing existing Secret reference '$phase2_namespace/$phase2_secret_name'; no credential was changed."
  exit 0
fi

printf '%s' 'items_app' >"$phase2_secret_tmpdir/username"
openssl rand -hex 32 | tr -d '\n' >"$phase2_secret_tmpdir/password"
kubectl create secret generic "$phase2_secret_name" \
  --namespace "$phase2_namespace" \
  --type kubernetes.io/basic-auth \
  --from-file="username=$phase2_secret_tmpdir/username" \
  --from-file="password=$phase2_secret_tmpdir/password" \
  --dry-run=client \
  --output yaml |
  kubectl apply --filename - >/dev/null

echo "Created '$phase2_namespace/$phase2_secret_name' without printing credential data."
