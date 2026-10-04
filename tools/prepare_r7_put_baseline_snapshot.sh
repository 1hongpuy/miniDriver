#!/usr/bin/env bash
set -euo pipefail

# Prepare one stopped, reproducible LevelDB/DataNode fixture for the formal
# global-vs-sharded A/C run.  The resulting directories are intentionally
# preserved; formal cases clone the snapshot and never mutate this source.

: "${MINIKV_V2_CLUSTER_SECRET:?MINIKV_V2_CLUSTER_SECRET is required}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_bin="${MINIKV_V2_BIN_DIR:-${repo_dir}/build/bin}"
root="${MINIKV_R7_BASELINE_ROOT:-/data/minikv-v2/r7-ac-baseline-$(date +%Y%m%d-%H%M%S)}"
snapshot="${MINIKV_R7_BASELINE_SNAPSHOT:-${root}-snapshot}"
prefill_objects="${MINIKV_R7_BASELINE_OBJECTS:-10000}"
prefill_concurrency="${MINIKV_R7_BASELINE_CONCURRENCY:-16}"
gateway_port="${MINIKV_R7_BASELINE_GATEWAY_PORT:-23080}"
node_a_port="${MINIKV_R7_BASELINE_NODE_A_PORT:-24080}"
node_c_port="${MINIKV_R7_BASELINE_NODE_C_PORT:-25080}"
node_b_port="${MINIKV_R7_BASELINE_NODE_B_PORT:-26080}"
settle_seconds="${MINIKV_R7_BASELINE_SETTLE_SECONDS:-10}"

[[ -x "${cluster_bin}/minikv_v2_gateway" && -x "${cluster_bin}/minikv_v2_datanode" &&
   -x "${cluster_bin}/minikv_v2_bench" ]] || {
    echo "missing benchmark cluster binaries under ${cluster_bin}" >&2
    exit 2
}
for tool in curl find du stat; do
    command -v "${tool}" >/dev/null 2>&1 || { echo "required tool missing: ${tool}" >&2; exit 3; }
done
[[ "${prefill_objects}" =~ ^[0-9]+$ && "${prefill_objects}" -gt 0 ]] || exit 2
[[ "${prefill_concurrency}" =~ ^[0-9]+$ && "${prefill_concurrency}" -gt 0 ]] || exit 2
[[ "${settle_seconds}" =~ ^[0-9]+$ ]] || exit 2
if [[ -e "${root}" ]] && [[ -n "$(find "${root}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "baseline root is not empty: ${root}" >&2
    exit 2
fi
if [[ -e "${snapshot}" ]] && [[ -n "$(find "${snapshot}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "snapshot path is not empty: ${snapshot}" >&2
    exit 2
fi

mkdir -p "${root}" "${snapshot}"
cluster="${root}/cluster"
prefill="${root}/prefill"
export MINIKV_V2_BENCH_CLUSTER_DIR="${cluster}"
export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
export MINIKV_V2_BENCH_NODE_A_PORT="${node_a_port}"
export MINIKV_V2_BENCH_NODE_C_PORT="${node_c_port}"
export MINIKV_V2_BENCH_NODE_B_PORT="${node_b_port}"
export MINIKV_V2_BENCH_NODE_COUNT=3
export MINIKV_GATEWAY_LOCK_MODE=global
export MINIKV_V3_REPLICATION_FACTOR=2
export MINIKV_V4_MAX_ACTIVE_UPLOADS=32
export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=32
export MINIKV_GATEWAY_MUTEX_DIAGNOSTICS=0

cleanup() {
    bash "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

bash "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh" > "${root}/cluster-start.log" 2>&1
rounds=$(( (prefill_objects + prefill_concurrency - 1) / prefill_concurrency ))
actual_objects=$(( rounds * prefill_concurrency ))
"${cluster_bin}/minikv_v2_bench" local \
    --gateway "127.0.0.1:${gateway_port}" \
    --work-dir "${prefill}" --sizes 64KiB --runs "${rounds}" \
    --concurrency "${prefill_concurrency}" --mode upload \
    --chunk-window 2 --global-chunk-budget 64 --upload-checksum crc32c \
    --connection-mode keep-alive --remote-dir /r7-ac-baseline-prefill \
    > "${root}/prefill.log" 2>&1
sleep "${settle_seconds}"

# Stop before copying so the snapshot is a stable, restartable state and does
# not contain a live PID file.
cleanup
trap - EXIT INT TERM

cp -a "${cluster}/." "${snapshot}/"
cp "${prefill}/runs.csv" "${snapshot}/fixture-manifest.csv"
metadata_dir="${snapshot}/gateway/metadata"
metadata_bytes="$(du -sb "${metadata_dir}" | awk '{print $1}')"
metadata_files="$(find "${metadata_dir}" -type f | wc -l)"
sst_files="$(find "${metadata_dir}" -type f \( -name '*.ldb' -o -name '*.sst' \) | wc -l)"
wal_files="$(find "${metadata_dir}" -type f \( -name '*.log' -o -name '*.wal' \) | wc -l)"
data_bytes="$(du -sb "${snapshot}/node-a" "${snapshot}/node-b" "${snapshot}/node-c" 2>/dev/null |
    awk '{sum += $1} END {print sum + 0}')"
snapshot_bytes="$(du -sb "${snapshot}" | awk '{print $1}')"
cat > "${snapshot}/snapshot-manifest.json" <<EOF
{
  "format": 1,
  "lock_mode": "global",
  "replication_factor": 2,
  "object_size_bytes": 65536,
  "prefill_requested_objects": ${prefill_objects},
  "prefill_actual_objects": ${actual_objects},
  "prefill_concurrency": ${prefill_concurrency},
  "metadata_relative_path": "gateway/metadata",
  "fixture_manifest": "fixture-manifest.csv",
  "metadata_bytes": ${metadata_bytes},
  "metadata_file_count": ${metadata_files},
  "metadata_sst_file_count": ${sst_files},
  "metadata_wal_file_count": ${wal_files},
  "data_root_bytes": ${data_bytes},
  "snapshot_bytes": ${snapshot_bytes},
  "created_at": "$(date -Is)"
}
EOF
printf 'status=ready\ncreated_at=%s\nobjects=%s\nsnapshot=%s\n' \
    "$(date -Is)" "${actual_objects}" "${snapshot}" > "${snapshot}/snapshot-status.txt"
echo "baseline snapshot ready: ${snapshot}"
