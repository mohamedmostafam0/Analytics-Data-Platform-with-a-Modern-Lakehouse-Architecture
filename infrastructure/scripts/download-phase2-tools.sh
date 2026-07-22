#!/usr/bin/env bash
set -Eeuo pipefail

phase2_tool_root="${PHASE2_TOOL_ROOT:-/tmp/lakehouse-phase2-tools}"
phase2_bin_dir="$phase2_tool_root/bin"
phase2_download_dir="$phase2_tool_root/downloads"

kind_version="v0.31.0"
kind_sha256="eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa"
kubectl_version="v1.35.0"
kubectl_sha256="a2e984a18a0c063279d692533031c1eff93a262afcc0afdc517375432d060989"
helm_version="v3.21.1"
helm_sha256="a349c62d6ab2d5d11f044fc0d3afa6deed7d27cc7d5c351f536b169d9fc2cc1a"

install -d -m 0755 "$phase2_bin_dir" "$phase2_download_dir"

curl -fsSLo "$phase2_download_dir/kind-linux-amd64" \
  "https://github.com/kubernetes-sigs/kind/releases/download/$kind_version/kind-linux-amd64"
printf '%s  %s\n' "$kind_sha256" "$phase2_download_dir/kind-linux-amd64" | sha256sum --check --status
install -m 0755 "$phase2_download_dir/kind-linux-amd64" "$phase2_bin_dir/kind"

curl -fsSLo "$phase2_download_dir/kubectl" \
  "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl"
printf '%s  %s\n' "$kubectl_sha256" "$phase2_download_dir/kubectl" | sha256sum --check --status
install -m 0755 "$phase2_download_dir/kubectl" "$phase2_bin_dir/kubectl"

helm_archive="helm-$helm_version-linux-amd64.tar.gz"
curl -fsSLo "$phase2_download_dir/$helm_archive" "https://get.helm.sh/$helm_archive"
printf '%s  %s\n' "$helm_sha256" "$phase2_download_dir/$helm_archive" | sha256sum --check --status
tar -xzf "$phase2_download_dir/$helm_archive" -C "$phase2_download_dir"
install -m 0755 "$phase2_download_dir/linux-amd64/helm" "$phase2_bin_dir/helm"

"$phase2_bin_dir/kind" version
"$phase2_bin_dir/kubectl" version --client
"$phase2_bin_dir/helm" version --short
echo "Verified Phase 2 tools are in $phase2_bin_dir"
echo "Add that directory to PATH for the current shell before running Phase 2 scripts."
