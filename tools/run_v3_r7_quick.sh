#!/usr/bin/env bash
set -euo pipefail

# R7-Quick runner. It assumes Gateway/DataNode processes are already running
# and never deletes server-side objects or data directories.
if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
if [[ -z "${MINIKV_R7_QUICK_ROOT:-}" ]]; then
    echo "MINIKV_R7_QUICK_ROOT is required (use /tmp/minidriver-r7-quick-* or /data-ssd/minidriver-r7-quick-*)" >&2
    exit 2
fi
case "${MINIKV_R7_QUICK_ROOT}" in
    /tmp/minidriver-r7-quick-*|/data-ssd/minidriver-r7-quick-*|/data/minikv-v2/minidriver-r7-quick-*) ;;
    *) echo "MINIKV_R7_QUICK_ROOT must be a dedicated /tmp/minidriver-r7-quick-*, /data-ssd/minidriver-r7-quick-*, or /data/minikv-v2/minidriver-r7-quick-* path" >&2; exit 2 ;;
esac

bin="${MINIKV_R7_QUICK_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../build/bin" && pwd)/minikv_v2_bench}"
gateway="${MINIKV_R7_QUICK_GATEWAY:-127.0.0.1:8081}"
duration="${MINIKV_R7_QUICK_DURATION_SECONDS:-180}"
warmup="${MINIKV_R7_QUICK_WARMUP_SECONDS:-30}"
fixtures="${MINIKV_R7_QUICK_DOWNLOAD_FIXTURES:-10000}"
concurrencies="${MINIKV_R7_QUICK_CONCURRENCIES:-1 4 8 16 32}"
sizes="${MINIKV_R7_QUICK_SIZES:-64KiB 1MiB}"
service_principal="${MINIKV_R7_QUICK_SERVICE_PRINCIPAL:-r7-quick}"
fixture_manifest="${MINIKV_R7_QUICK_DOWNLOAD_FIXTURE_MANIFEST:-}"

if [[ ! -x "${bin}" ]]; then
    echo "benchmark binary is not executable: ${bin}" >&2
    exit 2
fi
if ! [[ "${duration}" =~ ^[1-9][0-9]*$ && "${warmup}" =~ ^[0-9]+$ && "${fixtures}" =~ ^[1-9][0-9]*$ ]]; then
    echo "duration, warmup, and fixtures must be non-negative numeric values (duration/fixtures positive)" >&2
    exit 2
fi
if (( warmup >= duration )); then
    echo "warmup must be less than duration" >&2
    exit 2
fi

if [[ -e "${MINIKV_R7_QUICK_ROOT}" ]] && [[ -n "$(find "${MINIKV_R7_QUICK_ROOT}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "benchmark root is not empty; choose a fresh MINIKV_R7_QUICK_ROOT" >&2
    exit 2
fi
mkdir -p "${MINIKV_R7_QUICK_ROOT}"
missing_observability=()
for required_tool in pidstat iostat sar; do
    if ! command -v "${required_tool}" >/dev/null 2>&1; then
        missing_observability+=("${required_tool}")
    fi
done
if (( ${#missing_observability[@]} > 0 )); then
    printf 'status=incomplete-observability\nmissing=%s\n' "${missing_observability[*]}" \
        > "${MINIKV_R7_QUICK_ROOT}/observability-status.txt"
    if [[ "${MINIKV_R7_ALLOW_INCOMPLETE:-0}" != "1" ]]; then
        echo "required sysstat tools missing: ${missing_observability[*]}" >&2
        echo "install sysstat, or set MINIKV_R7_ALLOW_INCOMPLETE=1 for exploratory-only output" >&2
        exit 3
    fi
else
    printf 'status=complete\nmissing=none\n' > "${MINIKV_R7_QUICK_ROOT}/observability-status.txt"
fi
printf 'gateway=%s duration_seconds=%s warmup_seconds=%s fixtures=%s fixture_manifest=%s sizes=%s concurrencies=%s max_active_uploads=%s max_uploads_per_client=%s max_active_downloads=%s max_downloads_per_client=%s\n' \
    "${gateway}" "${duration}" "${warmup}" "${fixtures}" "${fixture_manifest:-none}" "${sizes}" "${concurrencies}" \
    "${MINIKV_V4_MAX_ACTIVE_UPLOADS:-default}" "${MINIKV_V4_MAX_UPLOADS_PER_CLIENT:-default}" \
    "${MINIKV_V4_MAX_ACTIVE_DOWNLOADS:-default}" "${MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT:-default}" \
    > "${MINIKV_R7_QUICK_ROOT}/runner-config.txt"
printf 'mixed_ratio_policy=request-generation-by-workers\ngateway_mutex_diagnostics=%s\nhttp_diagnostics=%s\nobservability_status=%s\n' \
    "${MINIKV_GATEWAY_MUTEX_DIAGNOSTICS:-disabled}" "${MINIKV_HTTP_DIAGNOSTICS:-disabled}" \
    "${MINIKV_R7_QUICK_ROOT}/observability-status.txt" >> "${MINIKV_R7_QUICK_ROOT}/runner-config.txt"

collector_pids=()
stop_collectors() {
    for pid in "${collector_pids[@]}"; do
        kill "${pid}" 2>/dev/null || true
    done
    for pid in "${collector_pids[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done
    collector_pids=()
}
trap stop_collectors EXIT INT TERM

start_collector() {
    local command_name="$1"
    local output_path="$2"
    shift 2
    if command -v "${command_name}" >/dev/null 2>&1; then
        "${command_name}" "$@" > "${output_path}" 2>&1 &
        collector_pids+=("$!")
    else
        printf '%s is not installed on this host\n' "${command_name}" > "${output_path}.unavailable.txt"
    fi
}

capture_host_snapshot() {
    local case_root="$1"
    {
        date --iso-8601=seconds 2>/dev/null || date
        uname -a
        printf 'admission_max_active_uploads=%s\n' "${MINIKV_V4_MAX_ACTIVE_UPLOADS:-default}"
        printf 'admission_max_uploads_per_client=%s\n' "${MINIKV_V4_MAX_UPLOADS_PER_CLIENT:-default}"
        printf 'admission_max_active_downloads=%s\n' "${MINIKV_V4_MAX_ACTIVE_DOWNLOADS:-default}"
        printf 'admission_max_downloads_per_client=%s\n' "${MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT:-default}"
        findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS "${MINIKV_R7_QUICK_DATA_MOUNT:-/data-ssd}" 2>/dev/null || true
        df -h "${MINIKV_R7_QUICK_DATA_MOUNT:-/data-ssd}" 2>/dev/null || true
    } > "${case_root}/environment-snapshot.txt"
}

for size in ${sizes}; do
    for mode in upload download; do
        for concurrency in ${concurrencies}; do
            case_root="${MINIKV_R7_QUICK_ROOT}/${mode}-${size}-c${concurrency}"
            mkdir -p "${case_root}"
            capture_host_snapshot "${case_root}"
    start_collector pidstat "${case_root}/pidstat.log" -dur 1
    start_collector iostat "${case_root}/iostat.log" -x 1
            start_collector vmstat "${case_root}/vmstat.log" 1
            start_collector sar "${case_root}/sar-net.log" -n DEV 1
            common_args=(
                local --gateway "${gateway}" --work-dir "${case_root}"
                --sizes "${size}" --duration-seconds "${duration}"
                --warmup-seconds "${warmup}" --concurrency "${concurrency}"
                --chunk-window 2 --global-chunk-budget 64
                --upload-checksum crc32c --connection-mode keep-alive
                --remote-dir "/r7-quick/${mode}/${size}/c${concurrency}"
            )
            if [[ "${mode}" == "upload" ]]; then
                "${bin}" "${common_args[@]}" --mode upload | tee "${case_root}/console.log"
            else
                download_args=("${common_args[@]}" --mode download \
                    --read-profile independent --download-fixtures "${fixtures}" \
                    --sdk-read-plan true --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}" \
                    --service-principal "${service_principal}")
                if [[ -n "${fixture_manifest}" ]]; then
                    download_args+=(--download-fixture-manifest "${fixture_manifest}")
                fi
                "${bin}" "${download_args[@]}" | tee "${case_root}/console.log"
            fi
            stop_collectors
        done
    done
done

echo "R7-Quick complete: ${MINIKV_R7_QUICK_ROOT}"
