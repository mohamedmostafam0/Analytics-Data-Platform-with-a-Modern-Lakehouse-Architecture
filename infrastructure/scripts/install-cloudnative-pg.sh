#!/usr/bin/env bash
set -Eeuo pipefail

cnpg_version="1.29.2"
cnpg_sha256="856312bb13e64c5b03861092eac9045e45f7b7d601aafe75a9016260a762ac8a"
cnpg_manifest="/tmp/lakehouse-cnpg-$cnpg_version.yaml"
cnpg_url="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v$cnpg_version/releases/cnpg-$cnpg_version.yaml"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is required." >&2
  exit 2
fi

curl -fsSLo "$cnpg_manifest" "$cnpg_url"
printf '%s  %s\n' "$cnpg_sha256" "$cnpg_manifest" | sha256sum --check
kubectl apply --server-side --filename "$cnpg_manifest"
kubectl rollout status deployment/cnpg-controller-manager \
  --namespace cnpg-system \
  --timeout 5m
