#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
chart_dir="$repo_root/infrastructure/helm/lakehouse-platform"
profiles=(minimal ingestion streaming batch analytics observability logging full)
validation_tmpdir="$(mktemp -d /tmp/lakehouse-validation.XXXXXX)"
trap 'rm -rf -- "$validation_tmpdir"' EXIT

stage() {
  printf '\n==> %s\n' "$1"
}

skip() {
  printf 'SKIP: %s\n' "$1"
}

cd "$repo_root"

stage "Prerequisite report"
"$script_dir/check-local-prerequisites.sh"

stage "Docker Compose static normalization"
env \
  CDC_POSTGRES_USER=validation-user \
  CDC_POSTGRES_PASSWORD=validation-placeholder \
  CDC_POSTGRES_DB=validation-db \
  CLICKHOUSE_USER=validation-user \
  CLICKHOUSE_PASSWORD=validation-placeholder \
  docker compose --env-file .env.example -f docker-compose.yaml config -q
docker compose --env-file .env.example -f airflow.yaml config -q
echo "Both Compose files normalized without starting services."

stage "YAML inventory and merged values schema"
python3 - "$repo_root" <<'PY'
import copy
import json
import pathlib
import subprocess
import sys

import jsonschema
import yaml

root = pathlib.Path(sys.argv[1])
chart = root / "infrastructure/helm/lakehouse-platform"
profiles = ["minimal", "ingestion", "streaming", "batch", "analytics", "observability", "logging", "full"]

def merge(base, overlay):
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result

base = yaml.safe_load((chart / "values.yaml").read_text())
schema = json.loads((chart / "values.schema.json").read_text())
jsonschema.validate(base, schema)
for profile in profiles:
    overlay = yaml.safe_load((chart / f"values-{profile}.yaml").read_text()) or {}
    merged = merge(base, overlay)
    jsonschema.validate(merged, schema)
    if merged["global"]["profile"] != profile:
        raise SystemExit(f"profile overlay mismatch: {profile}")

inventory_path = root / "docs/platform/service-inventory.yaml"
inventory = yaml.safe_load(inventory_path.read_text())
if inventory["service_count"] != len(inventory["services"]):
    raise SystemExit("service inventory count does not match service entries")

main_services = set(subprocess.check_output(
    ["docker", "compose", "-f", "docker-compose.yaml", "config", "--services"],
    cwd=root,
    text=True,
).split())
airflow_services = set(subprocess.check_output(
    ["docker", "compose", "-f", "airflow.yaml", "config", "--services"],
    cwd=root,
    text=True,
).split())
if set(inventory["services"]) != main_services | airflow_services:
    missing = (main_services | airflow_services) - set(inventory["services"])
    extra = set(inventory["services"]) - (main_services | airflow_services)
    raise SystemExit(f"inventory mismatch; missing={sorted(missing)}, extra={sorted(extra)}")

for path in [
    root / "infrastructure/kubernetes/namespaces/lakehouse-platform.yaml",
    root / "infrastructure/helm/lakehouse-platform/Chart.yaml",
]:
    yaml.safe_load(path.read_text())

print(f"Validated schema for base values and {len(profiles)} merged profiles.")
print(f"Inventory exactly matches {len(main_services)} core + {len(airflow_services)} Airflow services.")
PY

stage "Helm lint, render, and feature behavior"
if command -v helm >/dev/null 2>&1; then
  "$chart_dir/tests/test-feature-flags.sh"
  for profile in "${profiles[@]}"; do
    helm template lakehouse-platform "$chart_dir" \
      --values "$chart_dir/values-$profile.yaml" \
      >"$validation_tmpdir/$profile.yaml"
  done
else
  skip "helm is not installed; dynamic chart rendering and feature-failure tests"
fi

stage "Kubernetes client/schema validation"
if command -v kubectl >/dev/null 2>&1; then
  kubectl apply --dry-run=client --validate=false \
    --filename infrastructure/kubernetes/namespaces/lakehouse-platform.yaml >/dev/null
  echo "kubectl client dry-run passed for the raw namespace."
else
  skip "kubectl is not installed"
fi

if command -v kubeconform >/dev/null 2>&1; then
  kubeconform -strict infrastructure/kubernetes/namespaces/lakehouse-platform.yaml
  if command -v helm >/dev/null 2>&1; then
    for profile in "${profiles[@]}"; do
      kubeconform -strict -ignore-missing-schemas "$validation_tmpdir/$profile.yaml"
    done
  fi
else
  skip "kubeconform is not installed"
fi

stage "Lint checks"
mapfile -t shell_files < <(find infrastructure -type f -name '*.sh' -print | sort)
bash -n "${shell_files[@]}"
echo "bash syntax validation passed for infrastructure shell scripts."

python3 - "$repo_root/load-generators/items-load/item_seeder.py" <<'PY'
import pathlib
import sys

source_path = pathlib.Path(sys.argv[1])
compile(source_path.read_text(), str(source_path), "exec")
print("Python syntax validation passed for the Phase 2 item seeder.")
PY

if command -v yamllint >/dev/null 2>&1; then
  mapfile -t yaml_files < <(find infrastructure docs/platform -type f \( -name '*.yaml' -o -name '*.yml' \) -print | sort)
  yamllint "${yaml_files[@]}"
else
  skip "yamllint is not installed"
fi

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck "${shell_files[@]}"
else
  skip "shellcheck is not installed"
fi

if command -v markdownlint >/dev/null 2>&1; then
  mapfile -t markdown_files < <(find infrastructure docs/adr docs/platform docs/plans -type f -name '*.md' -print | sort)
  markdownlint "${markdown_files[@]}"
else
  skip "markdownlint is not installed"
fi

stage "Migration safety assertions"
if grep -R -n -E '^[[:space:]]*kind:[[:space:]]*Secret[[:space:]]*$' infrastructure; then
  echo "ERROR: migration infrastructure must not commit Kubernetes Secret objects." >&2
  exit 1
fi
if grep -R -n -E '^[[:space:]]*image:[^#]*(latest|:[[:space:]]*$)' infrastructure; then
  echo "ERROR: unpinned images are forbidden in new infrastructure." >&2
  exit 1
fi
if git ls-files --error-unmatch .env >/dev/null 2>&1; then
  echo "ERROR: .env must not be tracked." >&2
  exit 1
fi
if ! git check-ignore -q .env; then
  echo "ERROR: .env must remain ignored." >&2
  exit 1
fi
fixture_password='etl''password'
if grep -R -n -F "$fixture_password" infrastructure/helm infrastructure/kubernetes infrastructure/scripts; then
  echo "ERROR: the Compose-only fixture password must not appear in Kubernetes assets." >&2
  exit 1
fi
echo "No Secret object, latest image, or Compose fixture credential was introduced; .env remains untracked and ignored."

stage "Validation complete"
echo "Static validation passed. No container, service, or Kubernetes resource was started or applied."
