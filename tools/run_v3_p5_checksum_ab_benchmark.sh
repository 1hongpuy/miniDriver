#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_V3_P5_AB_ROOT:-}" ]]; then
    echo "MINIKV_V3_P5_AB_ROOT is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_P5_AB_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-p5-ab-*) ;;
    *)
        echo "benchmark root must match /data-ssd/minidriver-v3-p5-ab-*" >&2
        exit 2
        ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "benchmark root already exists; choose a fresh P5 A/B directory" >&2
    exit 2
fi
if ! findmnt -T /data-ssd -n -o OPTIONS | tr ',' '\n' | rg -qx 'rw'; then
    echo "/data-ssd is not mounted read-write" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_P5_AB_GATEWAY_PORT:-58481}"
node_a_port="${MINIKV_V3_P5_AB_NODE_A_PORT:-59401}"
node_c_port="${MINIKV_V3_P5_AB_NODE_C_PORT:-59402}"
protocols="${MINIKV_V3_P5_AB_PROTOCOLS:-sha256 crc32c}"
workloads="${MINIKV_V3_P5_AB_WORKLOADS:-upload-c4:upload:16MiB:4:5:8 upload-c8:upload:16MiB:8:5:8 upload-c16:upload:16MiB:16:5:8 sustained-c4:upload:64MiB:4:8:8 mixed-c8:mixed:16MiB:8:5:8}"
cluster_running=false
monitor_pid=""

stop_monitor()
{
    if [[ -n "${monitor_pid}" ]] && kill -0 "${monitor_pid}" 2>/dev/null; then
        kill "${monitor_pid}" 2>/dev/null || true
        wait "${monitor_pid}" 2>/dev/null || true
    fi
    monitor_pid=""
}

stop_cluster()
{
    stop_monitor
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
export MINIKV_V2_BENCH_NODE_A_PORT="${node_a_port}"
export MINIKV_V2_BENCH_NODE_C_PORT="${node_c_port}"
export MINIKV_V2_BENCH_NODE_COUNT=2
export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
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

mkdir -p "${result_root}"
findmnt -T /data-ssd -o TARGET,SOURCE,FSTYPE,OPTIONS >"${result_root}/mount.txt"
lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINTS,ROTA,RO >"${result_root}/lsblk.txt"

for workload_definition in ${workloads}; do
    IFS=: read -r workload_name mode size concurrency runs budget <<<"${workload_definition}"
    case "${mode}" in upload|mixed) ;; *) echo "invalid mode ${mode}" >&2; exit 2 ;; esac
    for checksum_type in ${protocols}; do
        case "${checksum_type}" in sha256|crc32c) ;; *) echo "invalid checksum ${checksum_type}" >&2; exit 2 ;; esac

        case_root="${result_root}/${workload_name}/${checksum_type}"
        result_dir="${case_root}/results"
        work_dir="/tmp/minikv-v3-p5-ab-${workload_name}-${checksum_type}"
        export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
        export MINIKV_V3_CHECKSUM_TYPE="${checksum_type}"
        mkdir -p "${result_dir}" "${work_dir}"

        "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
        cluster_running=true
        cp /proc/diskstats "${result_dir}/diskstats.before"
        cp /proc/meminfo "${result_dir}/meminfo.before"
        vmstat 1 >"${result_dir}/vmstat.log" &
        monitor_pid=$!

        "${repo_dir}/build/bin/minikv_v2_bench" local \
            --gateway "127.0.0.1:${gateway_port}" \
            --work-dir "${work_dir}" \
            --sizes "${size}" --runs "${runs}" --concurrency "${concurrency}" \
            --chunk-window 2 --global-chunk-budget "${budget}" --mode "${mode}" \
            --upload-checksum "${checksum_type}" \
            --fixture-settle-ms 1000 \
            --remote-dir "/perf/v3-p5-ab/${workload_name}/${checksum_type}" \
            | tee "${result_dir}/console.log"

        stop_monitor
        cp /proc/diskstats "${result_dir}/diskstats.after"
        cp /proc/meminfo "${result_dir}/meminfo.after"
        cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
            "${result_dir}/"
        python3 "${repo_dir}/tools/summarize_v4_observability.py" \
            --logs "${case_root}/cluster/node-*/logs/datanode-*.log" \
            --output "${result_dir}/observability.json"

        if ! rg -q "identity_scheme=opaque-chunk-id checksum_type=${checksum_type}" \
            "${case_root}"/cluster/node-*/logs/datanode-*.log; then
            echo "protocol marker missing for ${workload_name}/${checksum_type}" >&2
            exit 1
        fi
        stop_cluster
    done
done

echo "P5 checksum A/B benchmark completed: ${result_root}"
