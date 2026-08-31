#!/usr/bin/env bash
set -euo pipefail

# R5: normal readers and intentionally slow TCP consumers read one immutable
# hot object together. Run against a fresh isolated benchmark cluster when
# interpreting output-buffer peaks, because those metrics are process-lifetime.

if [[ $# -ne 2 ]]; then
    echo "usage: $0 GATEWAY_HOST:PORT ABSOLUTE_RESULT_DIR" >&2
    exit 2
fi

gateway="$1"
result_dir="$2"
if [[ "${result_dir}" != /* ]]; then
    echo "result directory must be absolute" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bench="${MINIKV_V2_BENCH_BIN:-${repo_dir}/build/bin/minikv_v2_bench}"
cluster_dir="${MINIKV_V2_BENCH_CLUSTER_DIR:-/tmp/minikv-v2-bench-cluster}"
runs="${MINIKV_V3_R5_RUNS:-5}"
rate="${MINIKV_V3_R5_SLOW_RATE:-2MiB}"
remote_dir="${MINIKV_V3_R5_REMOTE_DIR:-/p6-r5}"
budget="${MINIKV_V3_R5_GLOBAL_CHUNK_BUDGET:-8}"
sdk_read_plan="${MINIKV_V3_R5_SDK_READ_PLAN:-true}"
service_principal="${MINIKV_V3_R5_SERVICE_PRINCIPAL:-benchmark-p6-r5}"

if [[ ! -x "${bench}" ]]; then
    echo "missing benchmark executable: ${bench}" >&2
    exit 2
fi
if [[ ! -f "${cluster_dir}/pids" ]]; then
    echo "missing benchmark cluster PID file: ${cluster_dir}/pids" >&2
    echo "start a fresh cluster with tools/start_v2_local_benchmark_cluster.sh" >&2
    exit 2
fi
case "${sdk_read_plan}" in
    true|false) ;;
    *) echo "MINIKV_V3_R5_SDK_READ_PLAN must be true or false" >&2; exit 2 ;;
esac
sdk_args=()
if [[ "${sdk_read_plan}" == true ]]; then
    if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
        echo "MINIKV_V2_CLUSTER_SECRET is required when SDK ReadPlan is enabled" >&2
        exit 2
    fi
    sdk_args=(--sdk-read-plan true --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}"
              --service-principal "${service_principal}")
fi

mkdir -p "${result_dir}"
printf '%s\n' \
    "profile=R5 slow-reader isolation" \
    "gateway=${gateway}" \
    "runs=${runs}" \
    "slow_rate=${rate}" \
    "sdk_read_plan=${sdk_read_plan}" \
    "service_principal=${service_principal}" \
    "normal_and_slow_readers_share_one_hot_16MiB_object=true" \
    "resource_snapshot_source=${cluster_dir}/node-*/logs/datanode-*.log" \
    > "${result_dir}/README.txt"

# RSS is sampled independently of benchmark records. DataNode emits its own
# output-buffer and DownloadGovernor snapshots once per second; retain both.
rss_file="${result_dir}/process-rss.csv"
echo "epoch_ms,pid,rss_kb,command" > "${rss_file}"
sample_rss() {
    while :; do
        local now pid rss command
        now="$(date +%s%3N)"
        while read -r pid; do
            [[ -n "${pid}" ]] || continue
            if [[ -r "/proc/${pid}/status" ]]; then
                rss="$(awk '/^VmRSS:/ {print $2}' "/proc/${pid}/status")"
                command="$(tr '\0' ' ' < "/proc/${pid}/cmdline" | tr ',' '_')"
                echo "${now},${pid},${rss:-0},${command}" >> "${rss_file}"
            fi
        done < "${cluster_dir}/pids"
        sleep 0.25
    done
}
sample_rss &
sampler_pid=$!
cleanup() {
    kill "${sampler_pid}" 2>/dev/null || true
    wait "${sampler_pid}" 2>/dev/null || true
}
trap cleanup EXIT

run_case() {
    local concurrency="$1"
    local slow_workers=$((concurrency / 2))
    local name="r5-hot-object-c${concurrency}-slow${slow_workers}-${rate}"
    echo "=== ${name} ==="
    "${bench}" local --gateway "${gateway}" --work-dir "${result_dir}/${name}" \
        --sizes 16MiB --runs "${runs}" --concurrency "${concurrency}" \
        --chunk-window 2 --global-chunk-budget "${budget}" --remote-dir "${remote_dir}" \
        --mode download --read-profile hot-object --connection-mode close \
        --slow-reader-workers "${slow_workers}" --slow-reader-bytes-per-sec "${rate}" \
        "${sdk_args[@]}"
}

run_case 8
run_case 16

for log in "${cluster_dir}"/node-*/logs/datanode-*.log; do
    [[ -f "${log}" ]] || continue
    base="$(basename "${log}")"
    grep 'event=resource_snapshot' "${log}" > "${result_dir}/${base}.resource_snapshot.log" || true
done

cat <<'EOF'
R5 output:
  - each runs.csv has reader_class=normal|slow for separate percentile analysis;
  - process-rss.csv is an external process RSS trace;
  - node resource_snapshot logs contain active_downloads and output-buffer peaks.
Do not declare R5 passed solely because requests succeed: normal-reader P99 and
the resource peaks must be compared to a normal-only control run.
EOF
