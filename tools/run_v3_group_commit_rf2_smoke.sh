#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_V3_GROUP_COMMIT_RF2_ROOT:-}" ]]; then
    echo "MINIKV_V3_GROUP_COMMIT_RF2_ROOT is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_GROUP_COMMIT_RF2_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-p4-rf2-*) ;;
    *)
        echo "RF=2 smoke root must match /data-ssd/minidriver-v3-p4-rf2-*" >&2
        exit 2
        ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "RF=2 smoke root already exists; choose a fresh directory" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_GROUP_COMMIT_RF2_GATEWAY_PORT:-58281}"
node_a_port="${MINIKV_V3_GROUP_COMMIT_RF2_NODE_A_PORT:-59201}"
node_c_port="${MINIKV_V3_GROUP_COMMIT_RF2_NODE_C_PORT:-59202}"
work_dir="/tmp/minikv-v3-p4-rf2-smoke"
cluster_running=false

stop_cluster()
{
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

export MINIKV_V2_BENCH_CLUSTER_DIR="${result_root}/cluster"
export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
export MINIKV_V2_BENCH_NODE_A_PORT="${node_a_port}"
export MINIKV_V2_BENCH_NODE_C_PORT="${node_c_port}"
export MINIKV_V2_BENCH_NODE_COUNT=2
export MINIKV_V3_DURABILITY_MODE=group_commit
export MINIKV_V3_WRITE_BATCH_MODE=pwritev
export MINIKV_V3_WRITE_BATCH_BYTES=262144
export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
export MINIKV_V3_GROUP_COMMIT_BYTES=8388608
export MINIKV_V3_GROUP_COMMIT_ITEMS=8
export MINIKV_V3_GROUP_COMMIT_DELAY_US=2000
export MINIKV_V3_GROUP_COMMIT_MAX_PENDING_BYTES=67108864
export MINIKV_V3_GROUP_COMMIT_MAX_PENDING_ITEMS=64
export MINIKV_V4_IO_THREADS=2
export MINIKV_V4_MAX_ACTIVE_UPLOADS=16
export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=16
export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=16
export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=16
export MINIKV_V4_DISK_WRITE_WORKERS=2
export MINIKV_V4_DISK_WRITE_BLOCKS=128

mkdir -p "${result_root}/results" "${work_dir}"
"${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
cluster_running=true

"${repo_dir}/build/bin/minikv_v2_bench" local \
    --gateway "127.0.0.1:${gateway_port}" \
    --work-dir "${work_dir}" \
    --sizes 64KiB,4MiB,16MiB,64MiB --runs 2 --concurrency 4 \
    --chunk-window 2 --global-chunk-budget 8 --mode end-to-end \
    --remote-dir /perf/v3-p4/rf2/e2e \
    | tee "${result_root}/results/console.log"

cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
    "${result_root}/results/"

echo "P4 RF=2 Group Commit smoke completed: ${result_root}"
