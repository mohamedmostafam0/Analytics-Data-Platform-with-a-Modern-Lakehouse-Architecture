#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
chart_dir="$(cd -- "$script_dir/../helm/lakehouse-platform" && pwd)"
profile="${1:-minimal}"
profiles=(minimal ingestion streaming batch analytics observability logging full)
valid=0

for candidate in "${profiles[@]}"; do
  if [[ "$profile" == "$candidate" ]]; then
    valid=1
    break
  fi
done

if [[ "$valid" -ne 1 ]]; then
  echo "ERROR: unknown profile '$profile'. Expected: ${profiles[*]}" >&2
  exit 1
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "ERROR: helm is required to render profiles; install it explicitly and retry." >&2
  exit 2
fi

exec helm template lakehouse-platform "$chart_dir" \
  --values "$chart_dir/values-$profile.yaml"
