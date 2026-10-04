#!/usr/bin/env bash
set -euo pipefail

# Fixed, low-risk diagnostic matrix for the 64 KiB PUT tail-latency plan.
# Services are intentionally managed outside this script so it cannot delete
# data or silently change Gateway/DataNode configuration.
if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2; exit 2
fi
root="${MINIKV_R7_PUT_DIAG_ROOT:-/tmp/minidriver-r7-put-diagnostic-$(date +%Y%m%d-%H%M%S)}"
bin="${MINIKV_R7_PUT_DIAG_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../build/bin" && pwd)/minikv_v2_bench}"
gateway="${MINIKV_R7_PUT_DIAG_GATEWAY:-127.0.0.1:8081}"
duration="${MINIKV_R7_PUT_DIAG_DURATION_SECONDS:-300}"
warmup="${MINIKV_R7_PUT_DIAG_WARMUP_SECONDS:-30}"
fixtures="${MINIKV_R7_PUT_DIAG_DOWNLOAD_FIXTURES:-10000}"

[[ -x "${bin}" ]] || { echo "benchmark binary is not executable: ${bin}" >&2; exit 2; }
if [[ -e "${root}" ]] && [[ -n "$(find "${root}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "diagnostic root is not empty: ${root}" >&2; exit 2
fi
mkdir -p "${root}"
missing=()
for tool in pidstat iostat sar; do
    command -v "${tool}" >/dev/null 2>&1 || missing+=("${tool}")
done
if (( ${#missing[@]} )); then
    printf 'status=incomplete-observability\nmissing=%s\n' "${missing[*]}" > "${root}/observability-status.txt"
    if [[ "${MINIKV_R7_ALLOW_INCOMPLETE:-0}" != "1" ]]; then
        echo "required sysstat tools missing: ${missing[*]}" >&2
        echo "install sysstat, or set MINIKV_R7_ALLOW_INCOMPLETE=1 for exploratory-only output" >&2
        exit 3
    fi
else
    printf 'status=complete\nmissing=none\n' > "${root}/observability-status.txt"
fi
printf 'mixed_ratio_policy=request-generation-by-workers\n' > "${root}/diagnostic-config.txt"

pids=()
stop_collectors() {
    for pid in "${pids[@]}"; do kill "${pid}" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || true; done
    pids=()
}
trap stop_collectors EXIT INT TERM
start_collectors() {
    local dir="$1"
    pidstat -dur 1 > "${dir}/pidstat.log" 2>&1 & pids+=("$!")
    iostat -x 1 > "${dir}/iostat.log" 2>&1 & pids+=("$!")
    vmstat 1 > "${dir}/vmstat.log" 2>&1 & pids+=("$!")
    sar -n DEV 1 > "${dir}/sar-net.log" 2>&1 & pids+=("$!")
}
run_case() {
    local name="$1"; shift
    local dir="${root}/${name}"
    mkdir -p "${dir}"
    start_collectors "${dir}"
    "${bin}" local --gateway "${gateway}" --work-dir "${dir}" --sizes 64KiB \
        --duration-seconds "${duration}" --warmup-seconds "${warmup}" \
        --chunk-window 2 --global-chunk-budget 64 --upload-checksum crc32c \
        --connection-mode keep-alive --remote-dir "/r7-put-diagnostic/${name}" "$@" \
        | tee "${dir}/console.log"
    stop_collectors
}

for c in 4 8 16; do
    run_case "pure-put-c${c}" --mode upload --concurrency "${c}"
done
# c16 mixed is explicitly 8 PUT workers + 8 GET workers (request-generation
# policy). GET uses a fixed random fixture set; PUT creates new ObjectRefs.
run_case "mixed-5x5-c16" --mode mixed --concurrency 16 --read-share 50 \
    --download-fixtures "${fixtures}" --read-profile independent \
    --sdk-read-plan true --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}" \
    --service-principal "${MINIKV_R7_PUT_DIAG_SERVICE_PRINCIPAL:-r7-put-diagnostic}"

echo "R7 PUT diagnostic complete: ${root}"
