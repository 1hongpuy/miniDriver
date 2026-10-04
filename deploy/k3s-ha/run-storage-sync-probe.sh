#!/usr/bin/env bash
set -euo pipefail

# Run the standalone probe inside the existing hostPath-mounted pods. It never
# touches MiniDriver's files: the binary creates and deletes only
# .minidriver-sync-probe/probe-*.bin below each supplied directory.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
kubectl_cmd=(sudo k3s kubectl)
report_root="${MINIDRIVER_STORAGE_REPORT_ROOT:-/var/lib/minidriver-bench/storage-diagnostics}"
mode="${1:-both}" # single, concurrent, or both
case "${mode}" in single|concurrent|both) ;; *) echo "usage: $0 [single|concurrent|both]" >&2; exit 2;; esac
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
report_dir="${report_root}/sync-probe-${stamp}"
warmup="${MINIDRIVER_SYNC_PROBE_WARMUP:-30}"
samples="${MINIDRIVER_SYNC_PROBE_SAMPLES:-300}"
repeats="${MINIDRIVER_SYNC_PROBE_REPEATS:-3}"
monitor_seconds="${MINIDRIVER_SYNC_PROBE_MONITOR_SECONDS:-90}"

sudo mkdir -p "${report_dir}"
sudo chown "$(id -u):$(id -g)" "${report_dir}"
probe="/usr/local/bin/minikv_storage_sync_probe"

declare -a labels targets paths
labels=(data index wal)
targets=(datanode-1 datanode-1 metadata-1)
paths=(/var/lib/minidriver/datanode /var/lib/minidriver/datanode-index /var/lib/minidriver/metadata-wal)

target_for() {
  local kind="$1" node="$2"
  case "${kind}" in
    data|index) printf 'datanode-%s' "${node}" ;;
    wal) printf 'metadata-%s' "${node}" ;;
  esac
}
path_for() {
  case "$1" in
    data) printf '%s' /var/lib/minidriver/datanode ;;
    index) printf '%s' /var/lib/minidriver/datanode-index ;;
    wal) printf '%s' /var/lib/minidriver/metadata-wal ;;
  esac
}
run_one() {
  local kind="$1" node="$2" size="$3" repeat="$4" start_at="${5:-0}" scope="${6:-single}"
  local target path run_id output
  target="$(target_for "${kind}" "${node}")"
  path="$(path_for "${kind}")"
  run_id="${scope}-${kind}-n${node}-${size}-r${repeat}"
  output="${report_dir}/${run_id}.csv"
  local args=("${probe}" --directory "${path}" --label "${kind}" --run-id "${run_id}"
              --size "${size}" --warmup "${warmup}" --samples "${samples}")
  if [[ "${start_at}" != 0 ]]; then args+=(--start-at-unix-ms "${start_at}"); fi
  "${kubectl_cmd[@]}" -n minidriver exec "deployment/${target}" -- "${args[@]}" >"${output}" 2>"${output}.stderr"
}

if ! "${kubectl_cmd[@]}" -n minidriver exec deployment/datanode-1 -- test -x "${probe}"; then
  echo "${probe} is absent from the deployed image." >&2
  echo "Build/export/distribute the K3s image, then redeploy DataNode and Metadata pods." >&2
  exit 1
fi

echo "report_dir=${report_dir}"
printf 'mode=%s warmup=%s samples=%s repeats=%s\n' "${mode}" "${warmup}" "${samples}" "${repeats}" >"${report_dir}/manifest.txt"

for repeat in $(seq 1 "${repeats}"); do
  for kind in data index wal; do
    for size in 4096 65536; do
      if [[ "${mode}" == single || "${mode}" == both ]]; then
        for node in 1 2 3; do
          echo "single kind=${kind} node=${node} size=${size} repeat=${repeat}"
          run_one "${kind}" "${node}" "${size}" "${repeat}" 0 single
        done
      fi
      if [[ "${mode}" == concurrent || "${mode}" == both ]]; then
        echo "concurrent kind=${kind} size=${size} repeat=${repeat}"
        metric_dir="${report_dir}/host-metrics-${kind}-${size}-r${repeat}"
        "${repo_root}/deploy/k3s-ha/capture-host-storage-metrics.sh" "${monitor_seconds}" "${metric_dir}" &
        monitor_pid=$!
        start_at="$(( $(date +%s%3N) + 5000 ))"
        pids=()
        for node in 1 2 3; do
          run_one "${kind}" "${node}" "${size}" "${repeat}" "${start_at}" concurrent &
          pids+=("$!")
        done
        failed=0
        for pid in "${pids[@]}"; do wait "${pid}" || failed=1; done
        wait "${monitor_pid}" || true
        if (( failed )); then echo "concurrent probe failed" >&2; exit 1; fi
      fi
    done
  done
done

python3 "${repo_root}/deploy/k3s-ha/summarize-storage-sync-probe.py" "${report_dir}" | tee "${report_dir}/summary.txt"
echo "storage sync probe report: ${report_dir}"
