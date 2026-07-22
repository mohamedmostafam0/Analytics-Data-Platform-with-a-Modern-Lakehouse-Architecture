#!/usr/bin/env bash
set -Eeuo pipefail

required=(docker python3)
optional=(helm kubectl kind openssl kubeconform yamllint shellcheck markdownlint d2 jq)
missing_required=0

echo "Required tools"
for tool in "${required[@]}"; do
  if command -v "$tool" >/dev/null 2>&1; then
    printf '  %-14s %s\n' "$tool" "available"
  else
    printf '  %-14s %s\n' "$tool" "MISSING"
    missing_required=1
  fi
done

echo "Optional validators"
for tool in "${optional[@]}"; do
  if command -v "$tool" >/dev/null 2>&1; then
    printf '  %-14s %s\n' "$tool" "available"
  else
    printf '  %-14s %s\n' "$tool" "not installed (validation will skip)"
  fi
done

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    echo "Docker Compose plugin: available"
  else
    echo "Docker Compose plugin: MISSING"
    missing_required=1
  fi
fi

if command -v python3 >/dev/null 2>&1; then
  python3 - <<'PY'
for module_name in ("yaml", "jsonschema"):
    try:
        __import__(module_name)
        print(f"Python module {module_name}: available")
    except ImportError:
        print(f"Python module {module_name}: not installed (schema fallback unavailable)")
PY
fi

exit "$missing_required"
