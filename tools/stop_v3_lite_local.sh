#!/usr/bin/env bash
set -euo pipefail

root_dir="${MINIKV_V3_ROOT:-/tmp/minidriver-v3-lite}"
pid_file="${root_dir}/metadata.pids"

if [[ ! -f "${pid_file}" ]]; then
    echo "no V3 Metadata PID file at ${pid_file}"
    exit 0
fi

while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}"
    fi
done <"${pid_file}"

rm -f "${pid_file}"
echo "V3-Lite Metadata stopped"
