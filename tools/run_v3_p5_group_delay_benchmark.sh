#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_V3_P5_DELAY_ROOT:-}" ]]; then
    echo "MINIKV_V3_P5_DELAY_ROOT is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_P5_DELAY_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-p5-delay-*) ;;
    *)
        echo "benchmark root must match /data-ssd/minidriver-v3-p5-delay-*" >&2
        exit 2
        ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "benchmark root already exists; choose a fresh P5 delay directory" >&2
    exit 2
fi
if ! findmnt -T /data-ssd -n -o OPTIONS | tr ',' '\n' | rg -qx 'rw'; then
    echo "/data-ssd is not mounted read-write" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_P5_DELAY_GATEWAY_PORT:-58581}"
node_a_port="${MINIKV_V3_P5_DELAY_NODE_A_PORT:-59501}"
node_c_port="${MINIKV_V3_P5_DELAY_NODE_C_PORT:-59502}"
# Alternate short/long candidates so monotonically increasing disk pressure is
# less likely to be mistaken for a delay effect.
delays_us="${MINIKV_V3_P5_DELAYS_US:-2000 5000 3000 4000}"
workloads="${MINIKV_V3_P5_DELAY_WORKLOADS:-sustained-c4:upload:64MiB:4:6:8 mixed-c8:mixed:16MiB:8:6:8}"
cluster_running=false
monitor_pids=()

stop_monitor()
{
    for monitor_pid in "${monitor_pids[@]}"; do
        if kill -0 "${monitor_pid}" 2>/dev/null; then
            kill "${monitor_pid}" 2>/dev/null || true
            wait "${monitor_pid}" 2>/dev/null || true
        fi
    done
    monitor_pids=()
}

start_monitor()
{
    local result_dir="$1"
    local pid_file="$2"
    vmstat 1 >"${result_dir}/vmstat.log" &
    monitor_pids+=("$!")

    if command -v pidstat >/dev/null 2>&1; then
        local pids
        pids="$(paste -sd, "${pid_file}")"
        pidstat -dur -p "${pids}" 1 >"${result_dir}/pidstat.log" &
        monitor_pids+=("$!")
    else
        echo "pidstat unavailable; install sysstat for per-process CPU/RSS/I/O sampling" \
            >"${result_dir}/pidstat.unavailable.txt"
    fi

    if command -v iostat >/dev/null 2>&1; then
        iostat -x 1 >"${result_dir}/iostat.log" &
        monitor_pids+=("$!")
    else
        echo "iostat unavailable; install sysstat for device await/util sampling" \
            >"${result_dir}/iostat.unavailable.txt"
    fi
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
export MINIKV_V3_CHECKSUM_TYPE=crc32c
export MINIKV_V3_DURABILITY_MODE=group_commit
export MINIKV_V3_WRITE_BATCH_MODE=pwritev
export MINIKV_V3_WRITE_BATCH_BYTES=262144
export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
export MINIKV_V3_GROUP_COMMIT_BYTES=8388608
export MINIKV_V3_GROUP_COMMIT_ITEMS=8
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
    for delay_us in ${delays_us}; do
        if ! [[ "${delay_us}" =~ ^[0-9]+$ ]] || (( delay_us < 2000 || delay_us > 5000 )); then
            echo "delay must be an integer between 2000 and 5000 us: ${delay_us}" >&2
            exit 2
        fi

        delay_ms=$((delay_us / 1000))
        case_root="${result_root}/${workload_name}/${delay_ms}ms"
        result_dir="${case_root}/results"
        work_dir="/tmp/minikv-v3-p5-delay-${workload_name}-${delay_ms}ms"
        export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
        export MINIKV_V3_GROUP_COMMIT_DELAY_US="${delay_us}"
        mkdir -p "${result_dir}" "${work_dir}"

        "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
        cluster_running=true
        cp /proc/diskstats "${result_dir}/diskstats.before"
        cp /proc/meminfo "${result_dir}/meminfo.before"
        start_monitor "${result_dir}" "${case_root}/cluster/pids"

        "${repo_dir}/build/bin/minikv_v2_bench" local \
            --gateway "127.0.0.1:${gateway_port}" \
            --work-dir "${work_dir}" \
            --sizes "${size}" --runs "${runs}" --concurrency "${concurrency}" \
            --chunk-window 2 --global-chunk-budget "${budget}" --mode "${mode}" \
            --fixture-settle-ms 1000 \
            --remote-dir "/perf/v3-p5-delay/${workload_name}/${delay_ms}ms" \
            | tee "${result_dir}/console.log"

        stop_monitor
        cp /proc/diskstats "${result_dir}/diskstats.after"
        cp /proc/meminfo "${result_dir}/meminfo.after"
        cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
            "${result_dir}/"
        python3 "${repo_dir}/tools/summarize_v4_observability.py" \
            --logs "${case_root}/cluster/node-*/logs/datanode-*.log" \
            --output "${result_dir}/observability.json"

        if ! rg -q "identity_scheme=opaque-chunk-id checksum_type=crc32c" \
            "${case_root}"/cluster/node-*/logs/datanode-*.log; then
            echo "protocol marker missing for ${workload_name}/${delay_ms}ms" >&2
            exit 1
        fi
        if ! rg -q "group_commit_delay_us=${delay_us}" \
            "${case_root}"/cluster/node-*/logs/datanode-*.log; then
            echo "group commit delay marker missing for ${workload_name}/${delay_ms}ms" >&2
            exit 1
        fi
        stop_cluster
    done
done

echo "P5 group-delay benchmark completed: ${result_root}"
