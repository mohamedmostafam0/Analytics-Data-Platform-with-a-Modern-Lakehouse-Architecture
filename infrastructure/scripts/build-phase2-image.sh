#!/usr/bin/env bash
set -Eeuo pipefail

phase2_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase2_repo_root="$(cd -- "$phase2_script_dir/../.." && pwd)"
phase2_cluster_name="${PHASE2_CLUSTER_NAME:-lakehouse-phase2}"
phase2_image="lakehouse/items-loadgen:phase2-0.1.0"

for phase2_command in docker kind; do
  if ! command -v "$phase2_command" >/dev/null 2>&1; then
    echo "ERROR: $phase2_command is required." >&2
    exit 2
  fi
done

docker build \
  --tag "$phase2_image" \
  "$phase2_repo_root/load-generators/items-load"

docker run --rm \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --entrypoint python \
  "$phase2_image" \
  -m unittest discover -s tests -v

kind load docker-image "$phase2_image" --name "$phase2_cluster_name"
docker image inspect "$phase2_image" --format 'Loaded image ID: {{.Id}}'
