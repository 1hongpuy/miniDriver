#!/usr/bin/env bash
set -euo pipefail

# Reproducible current-SDK warm-read matrix.  It is intentionally a
# single-DataNode counterpart to the single-directory MinIO/Warp GET baseline:
# fixtures are uploaded before timed reads, and every SDK result retains the
# benchmark's whole-object verification.  It is not a protocol-equivalent S3
# ranking.

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" || -z "${MINIKV_V3_SDK_READ_ROOT:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET and MINIKV_V3_SDK_READ_ROOT are required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_SDK_READ_ROOT}"
case "${result_root}" in
    /data-ssd/minidriver-v3-sdk-read-*) ;;
    *) echo "MINIKV_V3_SDK_READ_ROOT must match /data-ssd/minidriver-v3-sdk-read-*" >&2; exit 2 ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "benchmark root already exists: ${result_root}" >&2
    exit 2
fi
if ! findmnt -T /data-ssd -n -o OPTIONS | tr ',' '\n' | rg -qx 'rw'; then
    echo "/data-ssd is not mounted read-write" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_SDK_READ_GATEWAY_PORT:-18421}"
node_port="${MINIKV_V3_SDK_READ_NODE_PORT:-19421}"
runs="${MINIKV_V3_SDK_READ_RUNS:-5}"
concurrencies="${MINIKV_V3_SDK_READ_CONCURRENCIES:-1 4 8 16 32}"
size="${MINIKV_V3_SDK_READ_SIZE:-16MiB}"
requests_per_worker="${MINIKV_V3_SDK_READ_REQUESTS_PER_WORKER:-5}"
download_verification="${MINIKV_V3_SDK_READ_DOWNLOAD_VERIFICATION:-strict}"
cluster_running=false

stop_cluster() {
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

if ! [[ "${runs}" =~ ^[1-9][0-9]*$ && "${requests_per_worker}" =~ ^[1-9][0-9]*$ ]]; then
    echo "runs and requests-per-worker must be positive integers" >&2
    exit 2
fi
if [[ "${download_verification}" != "strict" && "${download_verification}" != "transport-only" ]]; then
    echo "MINIKV_V3_SDK_READ_DOWNLOAD_VERIFICATION must be strict or transport-only" >&2
    exit 2
fi

export MINIKV_V2_BENCH_NODE_COUNT=1
export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
export MINIKV_V3_CHECKSUM_TYPE=crc32c
export MINIKV_V3_DURABILITY_MODE=buffered
export MINIKV_V3_WRITE_BATCH_MODE=pwritev
export MINIKV_V3_WRITE_BATCH_BYTES=262144
export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
export MINIKV_V4_IO_THREADS="${MINIKV_V4_IO_THREADS:-2}"
export MINIKV_V4_SENDFILE_QUANTUM_BYTES="${MINIKV_V4_SENDFILE_QUANTUM_BYTES:-262144}"
export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=64
export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=64
export MINIKV_V4_DISK_WRITE_WORKERS=2
export MINIKV_V4_DISK_WRITE_BLOCKS=128

mkdir -p "${result_root}"
findmnt -T /data-ssd -o TARGET,SOURCE,FSTYPE,OPTIONS >"${result_root}/mount.txt"
lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINTS,ROTA,RO >"${result_root}/lsblk.txt"
printf '%s\n' \
    "cache_mode=warm" \
    "download_verification=${download_verification}" \
    "transport_only_means=whole-chunk CRC32C remains enabled; object output/final SHA-256 are skipped" \
    "read_profile=independent fixture per worker, repeated by same worker" \
    "connection_mode=keep-alive" \
    "not_a_protocol_equivalent_minio_ranking=true" >"${result_root}/README.txt"

for concurrency in ${concurrencies}; do
    if ! [[ "${concurrency}" =~ ^[1-9][0-9]*$ ]]; then
        echo "invalid concurrency: ${concurrency}" >&2
        exit 2
    fi
    case_root="${result_root}/c${concurrency}"
    work_dir="/tmp/minidriver-v3-sdk-read-c${concurrency}"
    # Reusing a just-closed listener can race the next fresh benchmark
    # cluster. Give each concurrency case its own ports so one result never
    # accidentally talks to a previous case's process.
    case_gateway_port=$((gateway_port + concurrency))
    case_node_port=$((node_port + concurrency))
    if (( case_gateway_port > 65535 || case_node_port > 65535 )); then
        echo "per-case benchmark port exceeds 65535" >&2
        exit 2
    fi
    export MINIKV_V2_BENCH_CLUSTER_DIR="${case_root}/cluster"
    export MINIKV_V2_BENCH_GATEWAY_PORT="${case_gateway_port}"
    export MINIKV_V2_BENCH_NODE_A_PORT="${case_node_port}"
    mkdir -p "${case_root}/results" "${work_dir}"
    printf '%s\n' "size=${size}" "runs=${runs}" "concurrency=${concurrency}" \
        "requests_per_worker=${requests_per_worker}" "connection_mode=keep-alive" \
        "read_profile=independent" "download_verification=${download_verification}" >"${case_root}/results/config.txt"
    "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
    cluster_running=true
    cp /proc/diskstats "${case_root}/results/diskstats.before"
    "${repo_dir}/build/bin/minikv_v2_bench" local \
        --gateway "127.0.0.1:${case_gateway_port}" \
        --work-dir "${work_dir}" --sizes "${size}" --runs "${runs}" \
        --concurrency "${concurrency}" --chunk-window 2 --global-chunk-budget 64 \
        --mode download --read-profile independent --connection-mode keep-alive \
        --requests-per-worker "${requests_per_worker}" --fixture-settle-ms 1000 \
        --download-verification "${download_verification}" \
        --sdk-read-plan true --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}" \
        --service-principal benchmark-sdk-minio-read \
        --remote-dir "/perf/sdk-read/c${concurrency}" \
        >"${case_root}/results/console.log" 2>&1 &
    bench_pid=$!
    {
        while kill -0 "${bench_pid}" 2>/dev/null; do
            date -u +'%Y-%m-%dT%H:%M:%SZ'
            ps -o pid,ppid,pcpu,pmem,rss,stat,comm -p "$(paste -sd, "${case_root}/cluster/pids"),${bench_pid}" || true
            for pid in $(cat "${case_root}/cluster/pids") "${bench_pid}"; do
                [[ -r "/proc/${pid}/status" ]] || continue
                awk -v pid="${pid}" '/^(Name|VmRSS|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):/ {print "pid=" pid, $0}' "/proc/${pid}/status"
            done
            sleep 1
        done
    } >"${case_root}/results/process-samples.log" 2>&1 &
    sampler_pid=$!
    vmstat 1 >"${case_root}/results/vmstat.log" &
    vmstat_pid=$!
    if ! wait "${bench_pid}"; then
        wait "${sampler_pid}" || true
        kill "${vmstat_pid}" 2>/dev/null || true
        wait "${vmstat_pid}" 2>/dev/null || true
        cat "${case_root}/results/console.log" >&2
        exit 1
    fi
    wait "${sampler_pid}" || true
    kill "${vmstat_pid}" 2>/dev/null || true
    wait "${vmstat_pid}" 2>/dev/null || true
    cat "${case_root}/results/console.log"
    cp /proc/diskstats "${case_root}/results/diskstats.after"
    cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
        "${case_root}/results/"
    python3 "${repo_dir}/tools/summarize_v4_observability.py" \
        --logs "${case_root}"/cluster/node-*/logs/datanode-*.log \
        --output "${case_root}/results/observability.json"
    stop_cluster
done

echo "SDK single-DataNode warm-read matrix completed: ${result_root}"
