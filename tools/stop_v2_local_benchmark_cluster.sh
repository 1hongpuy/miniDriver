#!/usr/bin/env bash
set -euo pipefail

root_dir="${MINIKV_V2_BENCH_CLUSTER_DIR:-/tmp/minikv-v2-bench-cluster}"
pid_file="${root_dir}/pids"

if [[ ! -f "${pid_file}" ]]; then
    echo "no benchmark cluster PID file at ${pid_file}" >&2
    exit 0
fi

while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}"
    fi
done <"${pid_file}"

rm -f "${pid_file}"
echo "isolated benchmark cluster stopped"
