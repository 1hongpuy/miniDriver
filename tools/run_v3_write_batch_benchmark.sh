#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_V3_WRITE_BATCH_BENCH_ROOT:-}" ]]; then
    echo "MINIKV_V3_WRITE_BATCH_BENCH_ROOT is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_WRITE_BATCH_BENCH_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-p3-*) ;;
    *)
        echo "benchmark root must match /data-ssd/minidriver-v3-p3-*" >&2
        exit 2
        ;;
esac

gateway_port="${MINIKV_V3_WRITE_BATCH_GATEWAY_PORT:-48181}"
node_port="${MINIKV_V3_WRITE_BATCH_NODE_PORT:-49102}"
runs="${MINIKV_V3_WRITE_BATCH_RUNS:-5}"
sustained_runs="${MINIKV_V3_WRITE_BATCH_SUSTAINED_RUNS:-10}"
cases="${MINIKV_V3_WRITE_BATCH_CASES:-buffered-single:buffered:single:262144:0 buffered-pwritev-64k:buffered:pwritev:65536:1000 buffered-pwritev-128k:buffered:pwritev:131072:1000 buffered-pwritev-256k:buffered:pwritev:262144:1000 buffered-pwritev-512k:buffered:pwritev:524288:1000 buffered-pwritev-256k-d0:buffered:pwritev:262144:0 buffered-pwritev-256k-d2:buffered:pwritev:262144:2000 chunk-sync-single:chunk_sync:single:262144:0 chunk-sync-pwritev-256k:chunk_sync:pwritev:262144:1000}"
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
    echo "benchmark root already exists; choose a fresh P3 directory" >&2
    exit 2
fi
mkdir -p "${result_root}"

for definition in ${cases}; do
    IFS=: read -r case_name durability_mode write_mode batch_bytes delay_us <<<"${definition}"
    case "${durability_mode}" in buffered|chunk_sync) ;; *) exit 2 ;; esac
    case "${write_mode}" in single|pwritev) ;; *) exit 2 ;; esac

    case_root="${result_root}/${case_name}"
    export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
    export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
    export MINIKV_V2_BENCH_NODE_A_PORT="${node_port}"
    export MINIKV_V2_BENCH_NODE_COUNT=1
    export MINIKV_V3_DURABILITY_MODE="${durability_mode}"
    export MINIKV_V3_WRITE_BATCH_MODE="${write_mode}"
    export MINIKV_V3_WRITE_BATCH_BYTES="${batch_bytes}"
    export MINIKV_V3_WRITE_BATCH_DELAY_US="${delay_us}"
    export MINIKV_V4_IO_THREADS=2
    export MINIKV_V4_MAX_ACTIVE_UPLOADS=64
    export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=64
    export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=8
    export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=8
    export MINIKV_V4_DISK_WRITE_WORKERS=2
    export MINIKV_V4_DISK_WRITE_BLOCKS=128

    mkdir -p "${case_root}/results/16m-c8"
    "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
    cluster_running=true

    work_dir="/tmp/minikv-v3-p3-${case_name}-16m-c8"
    mkdir -p "${work_dir}"
    "${repo_dir}/build/bin/minikv_v2_bench" local \
        --gateway "127.0.0.1:${gateway_port}" \
        --work-dir "${work_dir}" \
        --sizes 16MiB --runs "${runs}" --concurrency 8 \
        --chunk-window 2 --global-chunk-budget 16 --mode upload \
        --remote-dir "/perf/v3-p3/${case_name}/16m-c8" \
        | tee "${case_root}/results/16m-c8/console.log"
    cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
        "${case_root}/results/16m-c8/"

    if [[ "${case_name}" == "buffered-single" ||
          "${case_name}" == "buffered-pwritev-256k" ||
          "${case_name}" == "chunk-sync-single" ||
          "${case_name}" == "chunk-sync-pwritev-256k" ]]; then
        result_dir="${case_root}/results/64m-c4-sustained"
        work_dir="/tmp/minikv-v3-p3-${case_name}-64m-c4"
        mkdir -p "${work_dir}" "${result_dir}"
        "${repo_dir}/build/bin/minikv_v2_bench" local \
            --gateway "127.0.0.1:${gateway_port}" \
            --work-dir "${work_dir}" \
            --sizes 64MiB --runs "${sustained_runs}" --concurrency 4 \
            --chunk-window 2 --global-chunk-budget 8 --mode upload \
            --remote-dir "/perf/v3-p3/${case_name}/64m-c4-sustained" \
            | tee "${result_dir}/console.log"
        cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
            "${result_dir}/"
    fi

    stop_cluster
done

echo "P3 write batch benchmark completed: ${result_root}"
