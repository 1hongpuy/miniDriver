#!/usr/bin/env bash
set -euo pipefail

# This collects guest-side metrics over SSH. Windows host counters must be
# collected separately because Linux cannot observe Hyper-V's physical disks.
duration="${1:-90}"
report_dir="${2:?usage: $0 [duration_seconds] REPORT_DIR}"
ssh_user="${MINIDRIVER_STORAGE_SSH_USER:-$USER}"
read -r -a nodes <<< "${MINIDRIVER_STORAGE_NODES:-192.168.137.10 192.168.137.148 192.168.137.151}"

mkdir -p "${report_dir}"
read -r -d '' remote_script <<'REMOTE' || true
duration=__DURATION__
echo "# host=$(hostname) started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
collector_pids=()
collector_dir=$(mktemp -d)
trap 'rm -rf "$collector_dir"' EXIT
if command -v iostat >/dev/null; then
  timeout "$duration" iostat -x 1 >"$collector_dir/iostat.txt" 2>&1 &
  collector_pids+=("$!")
else
  printf '%s\n' 'iostat unavailable' >"$collector_dir/iostat.txt"
fi
if command -v vmstat >/dev/null; then
  timeout "$duration" vmstat -t 1 >"$collector_dir/vmstat.txt" 2>&1 &
  collector_pids+=("$!")
else
  printf '%s\n' 'vmstat unavailable' >"$collector_dir/vmstat.txt"
fi
for collector_pid in "${collector_pids[@]}"; do wait "${collector_pid}" || true; done
echo '## iostat'
cat "$collector_dir/iostat.txt"
echo '## vmstat'
cat "$collector_dir/vmstat.txt"
REMOTE
remote_script="${remote_script/__DURATION__/${duration}}"
pids=()
for node in "${nodes[@]}"; do
  safe_node="${node//[^A-Za-z0-9._-]/_}"
  # Feed a small script on stdin rather than composing a shell command from
  # nested quotes.  The previous form was accepted by ssh but reached bash
  # with unmatched quotes, silently losing all iostat/vmstat evidence.
  ssh -o BatchMode=no -o ConnectTimeout=10 "${ssh_user}@${node}" 'bash -s' \
    >"${report_dir}/${safe_node}.txt" 2>&1 <<<"${remote_script}" &
  pids+=("$!")
done
for pid in "${pids[@]}"; do wait "${pid}" || true; done
