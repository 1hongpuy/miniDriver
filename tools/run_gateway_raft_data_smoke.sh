#!/usr/bin/env bash
set -euo pipefail

# End-to-end local validation for Phase 3: Metadata Raft + remote Gateway
# control plane + two real DataNodes.  It intentionally runs one small upload
# and strict read; it is not a capacity benchmark.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
metadata_bin="${1:-${repo_dir}/build-ha/bin/minikv_metadata_raft_service}"
bin_dir="${repo_dir}/build-ha/bin"
run_dir="$(mktemp -d /tmp/minidriver-gateway-raft-data-smoke.XXXXXX)"
secret="gateway-data-smoke-secret"
meta_pids=()
cluster_started=0
cleanup() {
  if [[ "${cluster_started}" == 1 ]]; then
    MINIKV_V2_BENCH_CLUSTER_DIR="${run_dir}/cluster" \
    MINIKV_V2_BIN_DIR="${bin_dir}" \
    bash "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh" >/dev/null 2>&1 || true
  fi
  for pid in "${meta_pids[@]:-}"; do kill -TERM "${pid}" 2>/dev/null || true; done
  for pid in "${meta_pids[@]:-}"; do wait "${pid}" 2>/dev/null || true; done
}
trap cleanup EXIT

members="1@127.0.0.1:28801,2@127.0.0.1:28802,3@127.0.0.1:28803"
for member in 1 2 3; do
  MINIKV_METADATA_MEMBER_ID="${member}" \
  MINIKV_METADATA_RAFT_PORT="$((28800 + member))" \
  MINIKV_METADATA_RAFT_MEMBERS="${members}" \
  MINIKV_METADATA_PORT="$((28900 + member))" \
  MINIKV_METADATA_DIR="${run_dir}/meta-${member}" \
  MINIKV_V2_CLUSTER_SECRET="${secret}" \
  "${metadata_bin}" >"${run_dir}/meta-${member}.log" 2>&1 &
  meta_pids+=("$!")
done

leader_ready=0
for attempt in $(seq 1 100); do
  for member in 1 2 3; do
    if curl --silent --max-time 1 "http://127.0.0.1:$((28900 + member))/readyz" |
        grep -q '"role":"leader"'; then leader_ready=1; break 2; fi
  done
  sleep 0.1
done
[[ "${leader_ready}" == 1 ]] || { echo "metadata election failed: ${run_dir}" >&2; exit 1; }

export MINIKV_V2_CLUSTER_SECRET="${secret}"
export MINIKV_METADATA_MODE=raft
export MINIKV_METADATA_ENDPOINTS=127.0.0.1:28901,127.0.0.1:28902,127.0.0.1:28903
export MINIKV_DATANODE_METADATA_ENDPOINTS="${MINIKV_DATANODE_METADATA_ENDPOINTS:-${MINIKV_METADATA_ENDPOINTS}}"
export MINIKV_V2_BIN_DIR="${bin_dir}"
export MINIKV_V2_BENCH_CLUSTER_DIR="${run_dir}/cluster"
export MINIKV_V2_BENCH_GATEWAY_PORT=28701
export MINIKV_V2_BENCH_NODE_A_PORT=29701
export MINIKV_V2_BENCH_NODE_C_PORT=29702
export MINIKV_V2_BENCH_NODE_COUNT=2
export MINIKV_V2_BENCH_ADVERTISE_ADDRESS=127.0.0.1
export MINIKV_V3_REPLICATION_FACTOR=2
export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
export MINIKV_V3_CHECKSUM_TYPE=crc32c
export MINIKV_V3_DURABILITY_MODE=group_commit

bash "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
cluster_started=1

bench_dir="${run_dir}/bench"
mkdir -p "${bench_dir}"
"${bin_dir}/minikv_v2_bench" local \
  --gateway 127.0.0.1:28701 --work-dir "${bench_dir}" \
  --sizes 64KiB --runs 1 --mode end-to-end --concurrency 1 \
  --requests-per-worker 1 --chunk-window 1 --global-chunk-budget 2 \
  --upload-checksum crc32c --connection-mode keep-alive \
  --download-verification strict --sdk-read-plan true \
  --cluster-internal-token "${secret}" \
  --service-principal phase3-smoke --remote-dir /phase3

echo "Gateway Raft data smoke passed: RF2 real DataNode upload/strict-read artifacts=${run_dir}"
