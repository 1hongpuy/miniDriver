#!/usr/bin/env bash
set -euo pipefail

root_dir="${MINIKV_V3_ROOT:-/tmp/minidriver-v3-lite}"

mkdir -p \
    "${root_dir}/gateway" \
    "${root_dir}/metadata/meta-1" \
    "${root_dir}/metadata/meta-2" \
    "${root_dir}/metadata/meta-3" \
    "${root_dir}/datanode/dn-1" \
    "${root_dir}/datanode/dn-2" \
    "${root_dir}/datanode/dn-3" \
    "${root_dir}/logs" \
    "${root_dir}/faults"

echo "V3-Lite local directories are ready"
echo "  root:      ${root_dir}"
echo "  metadata:  ${root_dir}/metadata/meta-{1,2,3}"
echo "  datanode:  ${root_dir}/datanode/dn-{1,2,3}"
echo "  logs:      ${root_dir}/logs"
echo "  config:    configs/v3-lite.local.yaml"
echo "Metadata/Raft processes are intentionally not started in Phase 0."
