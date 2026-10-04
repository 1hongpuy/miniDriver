#!/usr/bin/env bash
set -euo pipefail

binary="${1:-build-ha/bin/minikv_metadata_raft_service}"
command_tool="${2:-build-ha/bin/minikv_metadata_command_tool}"
run_dir="$(mktemp -d /tmp/minidriver-metadata-raft-failover.XXXXXX)"
members="1@127.0.0.1:28201,2@127.0.0.1:28202,3@127.0.0.1:28203"
pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill -TERM "${pid}" 2>/dev/null || true; done
  for pid in "${pids[@]:-}"; do wait "${pid}" 2>/dev/null || true; done
}
trap cleanup EXIT

for member in 1 2 3; do
  http_port=$((28300 + member)); raft_port=$((28200 + member))
  MINIKV_METADATA_MEMBER_ID="${member}" MINIKV_METADATA_RAFT_PORT="${raft_port}" \
  MINIKV_METADATA_RAFT_MEMBERS="${members}" MINIKV_METADATA_PORT="${http_port}" \
  MINIKV_METADATA_DIR="${run_dir}/meta-${member}" "${binary}" >"${run_dir}/meta-${member}.log" 2>&1 &
  pids+=("$!")
done

leader_member=""
for attempt in $(seq 1 80); do
  for member in 1 2 3; do
    if curl --silent --max-time 1 "http://127.0.0.1:$((28300 + member))/readyz" | grep -q '"role":"leader"'; then
      leader_member="${member}"; break 2
    fi
  done
  sleep 0.1
done
[[ -n "${leader_member}" ]] || { echo "initial leader election failed: ${run_dir}" >&2; exit 1; }

register="${run_dir}/register.bin"
"${command_tool}" register-node "${register}" failover-register-1 dn-failover boot-1 127.0.0.1 29301 1073741824 crc32c
curl --fail --silent --max-time 3 -X POST -H 'Content-Type: application/octet-stream' \
  --data-binary "@${register}" "http://127.0.0.1:$((28300 + leader_member))/internal/v4/metadata/commands" | grep -q '"status":"OK"'

old_pid="${pids[$((leader_member - 1))]}"
kill -KILL "${old_pid}"
wait "${old_pid}" 2>/dev/null || true
new_leader=""
for attempt in $(seq 1 100); do
  for member in 1 2 3; do
    [[ "${member}" == "${leader_member}" ]] && continue
    if curl --silent --max-time 1 "http://127.0.0.1:$((28300 + member))/readyz" | grep -q '"role":"leader"'; then
      new_leader="${member}"; break 2
    fi
  done
  sleep 0.1
done
[[ -n "${new_leader}" ]] || { echo "leader failover failed: ${run_dir}" >&2; exit 1; }

digest=""
for attempt in $(seq 1 40); do
  digest="$(curl --silent --max-time 1 "http://127.0.0.1:$((28300 + new_leader))/internal/v4/metadata/state-digest" || true)"
  [[ "${digest}" == *'"digest":"'* ]] && break
  sleep 0.1
done
[[ "${digest}" == *'"digest":"'* ]] || { echo "post-failover ReadIndex failed: ${run_dir}" >&2; exit 1; }
echo "metadata Raft failover smoke passed: old_leader=${leader_member} new_leader=${new_leader} read_index=OK artifacts=${run_dir}"
