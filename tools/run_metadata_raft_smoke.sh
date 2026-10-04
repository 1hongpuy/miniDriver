#!/usr/bin/env bash
set -euo pipefail

binary="${1:-build-ha/bin/minikv_metadata_raft_service}"
command_tool="${2:-build-ha/bin/minikv_metadata_command_tool}"
run_dir="$(mktemp -d /tmp/minidriver-metadata-raft-smoke.XXXXXX)"
members="1@127.0.0.1:28001,2@127.0.0.1:28002,3@127.0.0.1:28003"
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill -TERM "${pid}" 2>/dev/null || true
  done
  for pid in "${pids[@]:-}"; do
    wait "${pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT

for member in 1 2 3; do
  http_port=$((28100 + member))
  raft_port=$((28000 + member))
  MINIKV_METADATA_MEMBER_ID="${member}" \
  MINIKV_METADATA_RAFT_PORT="${raft_port}" \
  MINIKV_METADATA_RAFT_MEMBERS="${members}" \
  MINIKV_METADATA_PORT="${http_port}" \
  MINIKV_METADATA_DIR="${run_dir}/meta-${member}" \
  "${binary}" >"${run_dir}/meta-${member}.log" 2>&1 &
  pids+=("$!")
done

leader_port=""
for attempt in $(seq 1 60); do
  for port in 28101 28102 28103; do
    body="$(curl --silent --max-time 1 "http://127.0.0.1:${port}/readyz" || true)"
    if [[ "${body}" == *'"role":"leader"'* ]] && [[ "${body}" == *'"leaderResolved":true'* ]]; then
      leader_port="${port}"
      break 2
    fi
  done
  sleep 0.1
done

if [[ -z "${leader_port}" ]]; then
  echo "no leader elected; logs: ${run_dir}" >&2
  exit 1
fi

for port in 28101 28102 28103; do
  curl --fail --silent --max-time 2 "http://127.0.0.1:${port}/healthz" >/dev/null
done

command_file="${run_dir}/register-node.bin"
"${command_tool}" register-node "${command_file}" smoke-register-1 dn-smoke boot-1 127.0.0.1 29201 1073741824 crc32c,rf2
result="$(curl --fail --silent --max-time 3 -X POST \
  -H 'Content-Type: application/octet-stream' \
  --data-binary "@${command_file}" \
  "http://127.0.0.1:${leader_port}/internal/v4/metadata/commands")"
if [[ "${result}" != *'"status":"OK"'* ]] || [[ "${result}" != *'"nodeEpoch":1'* ]]; then
  echo "majority command failed: ${result}; logs: ${run_dir}" >&2
  exit 1
fi

digest="$(curl --fail --silent --max-time 3 \
  "http://127.0.0.1:${leader_port}/internal/v4/metadata/state-digest")"
if [[ "${digest}" != *'"digest":"'* ]]; then
  echo "linearizable digest read failed; logs: ${run_dir}" >&2
  exit 1
fi

before_status="$(curl --fail --silent --max-time 3 \
  "http://127.0.0.1:${leader_port}/internal/v4/metadata/status")"
before_commit="$(sed -n 's/.*"commitIndex":\([0-9][0-9]*\).*/\1/p' <<<"${before_status}")"
for _ in $(seq 1 10); do
  curl --fail --silent --max-time 3 \
    "http://127.0.0.1:${leader_port}/internal/v4/metadata/state-digest" >/dev/null
done
after_status="$(curl --fail --silent --max-time 3 \
  "http://127.0.0.1:${leader_port}/internal/v4/metadata/status")"
after_commit="$(sed -n 's/.*"commitIndex":\([0-9][0-9]*\).*/\1/p' <<<"${after_status}")"
if [[ -z "${before_commit}" || "${before_commit}" != "${after_commit}" ]]; then
  echo "ReadIndex read appended a log entry: before=${before_status} after=${after_status}" >&2
  exit 1
fi

echo "metadata Raft smoke passed: leader_http_port=${leader_port} majority_command=OK linearizable_read=OK readindex_no_log=OK artifacts=${run_dir}"
