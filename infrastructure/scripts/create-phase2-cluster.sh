#!/usr/bin/env bash
set -Eeuo pipefail

phase2_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase2_repo_root="$(cd -- "$phase2_script_dir/../.." && pwd)"
phase2_cluster_name="${PHASE2_CLUSTER_NAME:-lakehouse-phase2}"

for phase2_command in kind docker; do
  if ! command -v "$phase2_command" >/dev/null 2>&1; then
    echo "ERROR: $phase2_command is required." >&2
    exit 2
  fi
done

if kind get clusters | grep -Fxq "$phase2_cluster_name"; then
  echo "Reusing existing kind cluster '$phase2_cluster_name'."
  kind export kubeconfig --name "$phase2_cluster_name"
  exit 0
fi

kind create cluster \
  --name "$phase2_cluster_name" \
  --config "$phase2_repo_root/infrastructure/kind/phase2.yaml" \
  --wait 5m
