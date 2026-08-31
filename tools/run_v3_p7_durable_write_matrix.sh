#!/usr/bin/env bash
set -euo pipefail

# P7 uses a fresh data root for every case.  It deliberately exercises one
# DataNode first: P7 is about a single shard's durable pipeline, not multi-disk
# scaling or cross-host capacity.
if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" || -z "${MINIKV_V3_P7_ROOT:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET and MINIKV_V3_P7_ROOT are required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_P7_ROOT}"
environment_scope="target-storage"
case "${result_root}" in
    /data-ssd/minidriver-v3-p7-durable-write-*) ;;
    /tmp/minidriver-v3-p7-durable-write-*)
        if [[ "${MINIKV_V3_P7_ALLOW_NON_RELEASE_ROOT:-}" != "1" ]]; then
            echo "set MINIKV_V3_P7_ALLOW_NON_RELEASE_ROOT=1 to use /tmp for a non-release smoke" >&2
            exit 2
        fi
        environment_scope="non-release-tmp"
        ;;
    *) echo "benchmark root must match /data-ssd/minidriver-v3-p7-durable-write-* (or the explicit /tmp non-release form)" >&2; exit 2 ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "benchmark root already exists; choose a fresh P7 directory" >&2
    exit 2
fi
mount_probe="$(dirname "${result_root}")"
if ! findmnt -T "${mount_probe}" -n -o OPTIONS | tr ',' '\n' | rg -qx 'rw'; then
    echo "benchmark filesystem is not mounted read-write: ${result_root}" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_P7_GATEWAY_PORT:-58681}"
node_port="${MINIKV_V3_P7_NODE_PORT:-59601}"
batch_bytes_list="${MINIKV_V3_P7_BATCH_BYTES:-8388608 16777216 33554432}"
workloads="${MINIKV_V3_P7_WORKLOADS:-sustained-c1:upload:64MiB:1:5:2 sustained-c2:upload:64MiB:2:5:4 sustained-c4:upload:64MiB:4:5:8 sustained-c8:upload:64MiB:8:5:16 mixed-c8:mixed:16MiB:8:5:16}"
disk_workers="${MINIKV_V3_P7_DISK_WORKERS:-2}"
cluster_running=false
monitor_pids=()

stop_monitor() {
    for monitor_pid in "${monitor_pids[@]}"; do
        if kill -0 "${monitor_pid}" 2>/dev/null; then
            kill "${monitor_pid}" 2>/dev/null || true
            wait "${monitor_pid}" 2>/dev/null || true
        fi
    done
    monitor_pids=()
}

start_monitor() {
    local result_dir="$1"
    local pid_file="$2"
    vmstat 1 >"${result_dir}/vmstat.log" &
    monitor_pids+=("$!")
    if command -v pidstat >/dev/null 2>&1; then
        pidstat -dur -p "$(paste -sd, "${pid_file}")" 1 >"${result_dir}/pidstat.log" &
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

stop_cluster() {
    stop_monitor
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

mkdir -p "${result_root}"
findmnt -T "${mount_probe}" -o TARGET,SOURCE,FSTYPE,OPTIONS >"${result_root}/mount.txt"
lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINTS,ROTA,RO >"${result_root}/lsblk.txt"
printf '%s\n' "environment_scope=${environment_scope}" >"${result_root}/environment_scope.txt"
if [[ "${environment_scope}" != "target-storage" ]]; then
    echo "This run uses /tmp and is only an instrumentation smoke; do not use it for P7 release capacity." \
        >"${result_root}/NON_RELEASE_WARNING.txt"
fi

for batch_bytes in ${batch_bytes_list}; do
    if ! [[ "${batch_bytes}" =~ ^[0-9]+$ ]] || (( batch_bytes < 8388608 || batch_bytes > 33554432 )); then
        echo "batch bytes must be an integer in [8388608, 33554432]: ${batch_bytes}" >&2
        exit 2
    fi
    case_name="batch-${batch_bytes}"
    for workload_definition in ${workloads}; do
        IFS=: read -r workload_name mode size concurrency runs budget <<<"${workload_definition}"
        case "${mode}" in upload|mixed) ;; *) echo "invalid mode ${mode}" >&2; exit 2 ;; esac
        case_root="${result_root}/${case_name}/${workload_name}"
        result_dir="${case_root}/results"
        work_dir="/tmp/minikv-v3-p7-${case_name}-${workload_name}"
        export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
        export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
        export MINIKV_V2_BENCH_NODE_A_PORT="${node_port}"
        export MINIKV_V2_BENCH_NODE_COUNT=1
        export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
        export MINIKV_V3_CHECKSUM_TYPE=crc32c
        export MINIKV_V3_DURABILITY_MODE=group_commit
        export MINIKV_V3_WRITE_BATCH_MODE=pwritev
        export MINIKV_V3_WRITE_BATCH_BYTES=262144
        export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
        export MINIKV_V3_GROUP_COMMIT_BYTES="${batch_bytes}"
        export MINIKV_V3_GROUP_COMMIT_ITEMS=8
        export MINIKV_V3_GROUP_COMMIT_DELAY_US=2000
        export MINIKV_V3_GROUP_COMMIT_MAX_PENDING_BYTES=67108864
        export MINIKV_V3_GROUP_COMMIT_MAX_PENDING_ITEMS=64
        export MINIKV_V4_IO_THREADS=2
        export MINIKV_V4_MAX_ACTIVE_UPLOADS=16
        export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=16
        export MINIKV_V4_DISK_WRITE_WORKERS="${disk_workers}"
        export MINIKV_V4_DISK_WRITE_BLOCKS=128
        mkdir -p "${result_dir}" "${work_dir}"
        printf '%s\n' "batch_bytes=${batch_bytes}" "disk_workers=${disk_workers}" \
            "workload=${workload_definition}" >"${result_dir}/config.txt"

        "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
        cluster_running=true
        cp /proc/diskstats "${result_dir}/diskstats.before"
        start_monitor "${result_dir}" "${case_root}/cluster/pids"
        "${repo_dir}/build/bin/minikv_v2_bench" local \
            --gateway "127.0.0.1:${gateway_port}" --work-dir "${work_dir}" \
            --sizes "${size}" --runs "${runs}" --concurrency "${concurrency}" \
            --chunk-window 2 --global-chunk-budget "${budget}" --mode "${mode}" \
            --fixture-settle-ms 1000 \
            --remote-dir "/perf/v3-p7/${case_name}/${workload_name}" \
            | tee "${result_dir}/console.log"
        stop_monitor
        cp /proc/diskstats "${result_dir}/diskstats.after"
        cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" "${result_dir}/"
        python3 "${repo_dir}/tools/summarize_v4_observability.py" \
            --logs "${case_root}/cluster/node-*/logs/datanode-*.log" \
            --output "${result_dir}/observability.json"
        rg -q "event=durability_batch_complete" "${case_root}"/cluster/node-*/logs/datanode-*.log
        stop_cluster
    done
done

echo "P7 durable write batch matrix completed: ${result_root}"
