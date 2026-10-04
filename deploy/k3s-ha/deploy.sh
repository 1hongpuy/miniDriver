#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
manifest_dir="${repo_root}/deploy/k3s-ha"
: "${MINIKV_V2_CLUSTER_SECRET:?set MINIKV_V2_CLUSTER_SECRET before deployment}"

kubectl_cmd=(sudo k3s kubectl)

"${kubectl_cmd[@]}" apply -f "${manifest_dir}/namespace.yaml"
"${kubectl_cmd[@]}" -n minidriver create secret generic cluster-secret \
  --from-literal="cluster-secret=${MINIKV_V2_CLUSTER_SECRET}" \
  --dry-run=client -o yaml | "${kubectl_cmd[@]}" apply -f -
"${kubectl_cmd[@]}" apply -k "${manifest_dir}"

for deployment in metadata-1 metadata-2 metadata-3 datanode-1 datanode-2 datanode-3; do
  "${kubectl_cmd[@]}" -n minidriver rollout status \
    "deployment/${deployment}" --timeout=300s
done
"${kubectl_cmd[@]}" -n minidriver rollout status daemonset/gateway --timeout=300s

"${kubectl_cmd[@]}" -n minidriver get pods -o wide
"${kubectl_cmd[@]}" -n minidriver get service gateway -o wide

