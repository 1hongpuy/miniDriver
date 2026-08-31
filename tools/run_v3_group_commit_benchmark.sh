#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_V3_GROUP_COMMIT_BENCH_ROOT:-}" ]]; then
    echo "MINIKV_V3_GROUP_COMMIT_BENCH_ROOT is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_GROUP_COMMIT_BENCH_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-p4-*) ;;
    *)
        echo "benchmark root must match /data-ssd/minidriver-v3-p4-*" >&2
        exit 2
        ;;
esac

gateway_port="${MINIKV_V3_GROUP_COMMIT_GATEWAY_PORT:-58181}"
node_port="${MINIKV_V3_GROUP_COMMIT_NODE_PORT:-59102}"
runs="${MINIKV_V3_GROUP_COMMIT_RUNS:-5}"
sustained_runs="${MINIKV_V3_GROUP_COMMIT_SUSTAINED_RUNS:-10}"
cases="${MINIKV_V3_GROUP_COMMIT_CASES:-buffered:buffered:8388608:8:2000 chunk-sync:chunk_sync:8388608:8:2000 group-4m-i4-d2:group_commit:4194304:4:2000 group-8m-i8-d1:group_commit:8388608:8:1000 group-8m-i8-d2:group_commit:8388608:8:2000 group-8m-i8-d5:group_commit:8388608:8:5000 group-16m-i16-d2:group_commit:16777216:16:2000}"
cluster_running=false

stop_cluster()
{
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

if [[ -e "${result_root}" ]]; then
    echo "benchmark root already exists; choose a fresh P4 directory" >&2
    exit 2
fi
mkdir -p "${result_root}"

for definition in ${cases}; do
    IFS=: read -r case_name durability_mode group_bytes group_items delay_us <<<"${definition}"
    case "${durability_mode}" in buffered|chunk_sync|group_commit) ;; *) exit 2 ;; esac

    case_root="${result_root}/${case_name}"
    export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
    export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
    export MINIKV_V2_BENCH_NODE_A_PORT="${node_port}"
    export MINIKV_V2_BENCH_NODE_COUNT=1
    export MINIKV_V3_DURABILITY_MODE="${durability_mode}"
    export MINIKV_V3_WRITE_BATCH_MODE=pwritev
    export MINIKV_V3_WRITE_BATCH_BYTES=262144
    export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
    export MINIKV_V3_GROUP_COMMIT_BYTES="${group_bytes}"
    export MINIKV_V3_GROUP_COMMIT_ITEMS="${group_items}"
    export MINIKV_V3_GROUP_COMMIT_DELAY_US="${delay_us}"
    export MINIKV_V3_GROUP_COMMIT_MAX_PENDING_BYTES=67108864
    export MINIKV_V3_GROUP_COMMIT_MAX_PENDING_ITEMS=64
    export MINIKV_V4_IO_THREADS=2
    export MINIKV_V4_MAX_ACTIVE_UPLOADS=64
    export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=64
    export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=8
    export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=8
    export MINIKV_V4_DISK_WRITE_WORKERS=2
    export MINIKV_V4_DISK_WRITE_BLOCKS=128

    result_dir="${case_root}/results/16m-c8"
    work_dir="/tmp/minikv-v3-p4-${case_name}-16m-c8"
    mkdir -p "${work_dir}" "${result_dir}"
    "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
    cluster_running=true
    "${repo_dir}/build/bin/minikv_v2_bench" local \
        --gateway "127.0.0.1:${gateway_port}" \
        --work-dir "${work_dir}" \
        --sizes 16MiB --runs "${runs}" --concurrency 8 \
        --chunk-window 2 --global-chunk-budget 16 --mode upload \
        --remote-dir "/perf/v3-p4/${case_name}/16m-c8" \
        | tee "${result_dir}/console.log"
    cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
        "${result_dir}/"

    if [[ "${case_name}" == "buffered" || "${case_name}" == "chunk-sync" ||
          "${case_name}" == "group-8m-i8-d2" ||
          "${case_name}" == "group-16m-i16-d2" ]]; then
        result_dir="${case_root}/results/64m-c4-sustained"
        work_dir="/tmp/minikv-v3-p4-${case_name}-64m-c4"
        mkdir -p "${work_dir}" "${result_dir}"
        "${repo_dir}/build/bin/minikv_v2_bench" local \
            --gateway "127.0.0.1:${gateway_port}" \
            --work-dir "${work_dir}" \
            --sizes 64MiB --runs "${sustained_runs}" --concurrency 4 \
            --chunk-window 2 --global-chunk-budget 8 --mode upload \
            --remote-dir "/perf/v3-p4/${case_name}/64m-c4-sustained" \
            | tee "${result_dir}/console.log"
        cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
            "${result_dir}/"
    fi

    stop_cluster
done

echo "P4 group commit benchmark completed: ${result_root}"
