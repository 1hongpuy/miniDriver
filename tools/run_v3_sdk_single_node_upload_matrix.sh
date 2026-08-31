#!/usr/bin/env bash
set -euo pipefail

# Reproducible single-DataNode SDK upload matrix.  This is intentionally
# separate from RF=2 capacity tests: it is the closest MiniDriver-side
# baseline for a single-process, single-directory MinIO/Warp PUT run.

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_V3_SDK_SINGLE_ROOT:-}" ]]; then
    echo "MINIKV_V3_SDK_SINGLE_ROOT is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_SDK_SINGLE_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-sdk-single-*) ;;
    *) echo "MINIKV_V3_SDK_SINGLE_ROOT must match /data-ssd/minidriver-v3-sdk-single-*" >&2; exit 2 ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "benchmark root already exists: ${result_root}" >&2
    exit 2
fi
if ! findmnt -T /data-ssd -n -o OPTIONS | tr ',' '\n' | rg -qx 'rw'; then
    echo "/data-ssd is not mounted read-write" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_SDK_SINGLE_GATEWAY_PORT:-18411}"
node_port="${MINIKV_V3_SDK_SINGLE_NODE_PORT:-19411}"
runs="${MINIKV_V3_SDK_SINGLE_RUNS:-3}"
concurrencies="${MINIKV_V3_SDK_SINGLE_CONCURRENCIES:-1 4 8 16}"
size="${MINIKV_V3_SDK_SINGLE_SIZE:-16MiB}"
cluster_running=false

stop_cluster() {
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
export MINIKV_V2_BENCH_NODE_A_PORT="${node_port}"
export MINIKV_V2_BENCH_NODE_COUNT=1
export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
export MINIKV_V3_CHECKSUM_TYPE=crc32c
export MINIKV_V3_DURABILITY_MODE=buffered
export MINIKV_V3_WRITE_BATCH_MODE=pwritev
export MINIKV_V3_WRITE_BATCH_BYTES=262144
export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
export MINIKV_V4_IO_THREADS=2
export MINIKV_V4_MAX_ACTIVE_UPLOADS=64
export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=64
export MINIKV_V4_DISK_WRITE_WORKERS=2
export MINIKV_V4_DISK_WRITE_BLOCKS=128

mkdir -p "${result_root}"
findmnt -T /data-ssd -o TARGET,SOURCE,FSTYPE,OPTIONS >"${result_root}/mount.txt"
lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINTS,ROTA,RO >"${result_root}/lsblk.txt"

for concurrency in ${concurrencies}; do
    if ! [[ "${concurrency}" =~ ^[1-9][0-9]*$ ]]; then
        echo "invalid concurrency: ${concurrency}" >&2
        exit 2
    fi
    case_root="${result_root}/c${concurrency}"
    work_dir="/tmp/minidriver-v3-sdk-single-c${concurrency}"
    export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
    mkdir -p "${case_root}/results" "${work_dir}"
    "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
    cluster_running=true
    cp /proc/diskstats "${case_root}/results/diskstats.before"
    "${repo_dir}/build/bin/minikv_v2_bench" local \
        --gateway "127.0.0.1:${gateway_port}" \
        --work-dir "${work_dir}" --sizes "${size}" --runs "${runs}" \
        --concurrency "${concurrency}" --chunk-window 2 \
        --global-chunk-budget 16 --mode upload --fixture-settle-ms 1000 \
        --remote-dir "/perf/sdk-single/c${concurrency}" \
        | tee "${case_root}/results/console.log"
    cp /proc/diskstats "${case_root}/results/diskstats.after"
    cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
        "${case_root}/results/"
    python3 "${repo_dir}/tools/summarize_v4_observability.py" \
        --logs "${case_root}/cluster/node-*/logs/datanode-*.log" \
        --output "${case_root}/results/observability.json"
    stop_cluster
done

echo "SDK single-DataNode upload matrix completed: ${result_root}"
