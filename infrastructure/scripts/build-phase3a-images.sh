#!/usr/bin/env bash
set -Eeuo pipefail

phase3a_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase3a_repo_root="$(cd -- "$phase3a_script_dir/../.." && pwd)"
phase3a_cluster_name="${PHASE3A_CLUSTER_NAME:-lakehouse-phase2}"
phase3a_minio_image="lakehouse/minio:phase3a-RELEASE.2025-10-15T17-29-55Z"
phase3a_client_image="lakehouse/minio-client:phase3a-RELEASE.2025-08-13T08-35-41Z"

for phase3a_command in docker kind; do
  if ! command -v "$phase3a_command" >/dev/null 2>&1; then
    echo "ERROR: $phase3a_command is required." >&2
    exit 2
  fi
done

docker build \
  --tag "$phase3a_minio_image" \
  "$phase3a_repo_root/infrastructure/images/minio"
docker build \
  --tag "$phase3a_client_image" \
  "$phase3a_repo_root/infrastructure/images/minio-client"

docker run --rm --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  "$phase3a_minio_image" --version
docker run --rm --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  "$phase3a_client_image" --version

kind load docker-image \
  "$phase3a_minio_image" \
  "$phase3a_client_image" \
  --name "$phase3a_cluster_name"

docker image inspect "$phase3a_minio_image" --format 'MinIO image ID: {{.Id}}'
docker image inspect "$phase3a_client_image" --format 'Client image ID: {{.Id}}'
