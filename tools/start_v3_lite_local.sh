#!/usr/bin/env bash
set -euo pipefail

root_dir="${MINIKV_V3_ROOT:-/tmp/minidriver-v3-lite}"
bin_dir="${MINIKV_V3_BIN_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../build/bin" && pwd)}"
metadata_bin="${MINIKV_V3_METADATA_BIN:-${bin_dir}/minidriver_v3_metadata}"
pid_file="${root_dir}/metadata.pids"

if [[ ! -x "${metadata_bin}" ]]; then
    echo "missing V3 Metadata executable: ${metadata_bin}" >&2
    echo "implement Phase 2 RaftAdapter and build minidriver_v3_metadata first" >&2
    exit 2
fi

if [[ -f "${pid_file}" ]]; then
    echo "existing V3 Metadata PID file: ${pid_file}" >&2
    echo "run tools/stop_v3_lite_local.sh first" >&2
    exit 2
fi

mkdir -p "${root_dir}/logs" "${root_dir}/metadata/meta-1" \
    "${root_dir}/metadata/meta-2" "${root_dir}/metadata/meta-3"

node_ids=(meta-1 meta-2 meta-3)
raft_ports=(18201 18202 18203)
pids=()

for index in "${!node_ids[@]}"; do
    node_id="${node_ids[index]}"
    node_dir="${root_dir}/metadata/${node_id}"
    log_file="${root_dir}/logs/${node_id}.log"
    nohup "${metadata_bin}" \
        --node-id "${node_id}" \
        --raft-port "${raft_ports[index]}" \
        --data-dir "${node_dir}" \
        --config "configs/v3-lite.local.yaml" \
        >"${log_file}" 2>&1 &
    pids+=("$!")
done

printf '%s\n' "${pids[@]}" >"${pid_file}"
echo "V3-Lite Metadata launch requested"
echo "  root: ${root_dir}"
echo "  pids: ${pid_file}"
