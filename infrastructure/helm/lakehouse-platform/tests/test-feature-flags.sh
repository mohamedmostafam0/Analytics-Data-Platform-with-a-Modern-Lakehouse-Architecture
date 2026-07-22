#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
chart_dir="$(cd -- "$script_dir/.." && pwd)"
foundation_profiles=(ingestion streaming batch analytics observability logging full)
test_tmpdir="$(mktemp -d /tmp/lakehouse-feature-tests.XXXXXX)"
trap 'rm -rf -- "$test_tmpdir"' EXIT

if ! command -v helm >/dev/null 2>&1; then
  echo "ERROR: helm is required for feature behavior tests." >&2
  exit 2
fi

for profile in "${foundation_profiles[@]}"; do
  values_file="$chart_dir/values-$profile.yaml"
  output_file="$test_tmpdir/$profile.yaml"
  helm lint "$chart_dir" --values "$values_file" >/dev/null
  helm template lakehouse-platform "$chart_dir" --values "$values_file" >"$output_file"

  mapfile -t kinds < <(awk '$1 == "kind:" {print $2}' "$output_file")
  if [[ ${#kinds[@]} -ne 1 || "${kinds[0]}" != "Namespace" ]]; then
    echo "ERROR: profile '$profile' must remain foundation-only in Phase 2." >&2
    exit 1
  fi
done

minimal_output="$test_tmpdir/minimal.yaml"
helm lint "$chart_dir" --values "$chart_dir/values-minimal.yaml" >/dev/null
helm template lakehouse-platform "$chart_dir" \
  --values "$chart_dir/values-minimal.yaml" >"$minimal_output"

for expected_kind in Namespace ConfigMap Cluster Job NetworkPolicy; do
  if ! grep -Eq "^kind:[[:space:]]+$expected_kind$" "$minimal_output"; then
    echo "ERROR: minimal profile did not render $expected_kind." >&2
    exit 1
  fi
done
if [[ $(grep -Ec '^kind:[[:space:]]+NetworkPolicy$' "$minimal_output") -ne 2 ]]; then
  echo "ERROR: minimal profile must render exactly two NetworkPolicies." >&2
  exit 1
fi
if [[ $(grep -Ec '^kind:[[:space:]]+' "$minimal_output") -ne 6 ]]; then
  echo "ERROR: minimal profile must render exactly six Phase 2 resources." >&2
  exit 1
fi
grep -Fq 'automountServiceAccountToken: false' "$minimal_output"
grep -Fq 'readOnlyRootFilesystem: true' "$minimal_output"
grep -Fq 'runAsNonRoot: true' "$minimal_output"
grep -Fq 'ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie@sha256:9287ce030c6f3ce822e383b019ae4aaf1e8370bff3b39f9c51dc10d69dc97219' "$minimal_output"
grep -Fq 'ALTER TABLE public.items OWNER TO items_app' "$minimal_output"

no_namespace_output="$test_tmpdir/no-namespace.yaml"
helm template lakehouse-platform "$chart_dir" \
  --set namespace.create=false >"$no_namespace_output"
if grep -Eq '^kind:[[:space:]]+' "$no_namespace_output"; then
  echo "ERROR: disabling namespace creation must render no resources." >&2
  exit 1
fi

dependency_error="$test_tmpdir/dependency-error.txt"
if helm template lakehouse-platform "$chart_dir" \
  --set features.schemaRegistry.enabled=true >"$dependency_error" 2>&1; then
  echo "ERROR: Schema Registry without Kafka unexpectedly rendered." >&2
  exit 1
fi
grep -Fq 'features.schemaRegistry requires features.kafka' "$dependency_error"

items_dependency_error="$test_tmpdir/items-dependency-error.txt"
if helm template lakehouse-platform "$chart_dir" \
  --set features.itemsLoadGenerator.enabled=true >"$items_dependency_error" 2>&1; then
  echo "ERROR: item seeder without its source PostgreSQL unexpectedly rendered." >&2
  exit 1
fi
grep -Fq 'features.itemsLoadGenerator requires features.sourcePostgresql' "$items_dependency_error"

secret_error="$test_tmpdir/secret-error.txt"
if helm template lakehouse-platform "$chart_dir" \
  --set features.sourcePostgresql.enabled=true >"$secret_error" 2>&1; then
  echo "ERROR: source PostgreSQL without a Secret reference unexpectedly rendered." >&2
  exit 1
fi
grep -Fq 'components.sourcePostgresql.credentialsSecret.name is required' "$secret_error"

source_only_output="$test_tmpdir/source-only.yaml"
helm template lakehouse-platform "$chart_dir" \
  --set features.sourcePostgresql.enabled=true \
  --set components.sourcePostgresql.credentialsSecret.name=fixture-secret \
  >"$source_only_output"
if grep -Eq '^kind:[[:space:]]+(Job|NetworkPolicy)$' "$source_only_output"; then
  echo "ERROR: disabled item seeder rendered a Job or NetworkPolicy." >&2
  exit 1
fi

streamlit_error="$test_tmpdir/streamlit-error.txt"
if helm template lakehouse-platform "$chart_dir" \
  --set features.streamlit.enabled=true >"$streamlit_error" 2>&1; then
  echo "ERROR: Streamlit without its backends unexpectedly rendered." >&2
  exit 1
fi
grep -Fq 'features.streamlit requires features.clickhouse' "$streamlit_error"

phase_error="$test_tmpdir/phase-error.txt"
if helm template lakehouse-platform "$chart_dir" \
  --set features.kafka.enabled=true >"$phase_error" 2>&1; then
  echo "ERROR: a planned but unimplemented feature unexpectedly rendered." >&2
  exit 1
fi
grep -Fq 'features.kafka is planned but not implemented in Phase 2' "$phase_error"

schema_error="$test_tmpdir/schema-error.txt"
if helm lint "$chart_dir" \
  --set features.unknownFeature.enabled=true >"$schema_error" 2>&1; then
  echo "ERROR: an unknown feature unexpectedly passed values schema validation." >&2
  exit 1
fi

echo "Feature behavior tests passed for the Phase 2 slice and all profiles."
