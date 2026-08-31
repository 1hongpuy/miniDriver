#!/usr/bin/env bash
set -euo pipefail

binary="$1"
port="${MINIKV_TEST_REDIS_PORT:-16381}"
root="$(mktemp -d)"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cleanup() {
  if [[ -n "${redis_pid:-}" ]]; then kill "$redis_pid" 2>/dev/null || true; fi
  rm -rf "$root"
}
trap cleanup EXIT

redis-server --bind 127.0.0.1 --port "$port" --save '' --appendonly no \
  --daemonize no >"$root/redis.log" 2>&1 &
redis_pid=$!
for _ in $(seq 1 50); do
  if redis-cli -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then break; fi
  sleep 0.05
done
redis-cli -h 127.0.0.1 -p "$port" ping | grep -qx PONG
MINIKV_REDIS_TEST_PORT="$port" "$binary"
redis-cli -h 127.0.0.1 -p "$port" XRANGE minidrive:file-events:publisher-test - + | grep -qx event-one
AI_REDIS_TEST_PORT="$port" AI_REDIS_TEST_STREAM=minidrive:file-events:python-test \
  PYTHONPATH="$repo_root/ai-app-lite" \
  python3 -m unittest discover -s "$repo_root/ai-app-lite/tests" -p 'test_redis_stream_integration.py' -v
echo "PASS: C++ file event publisher and Python Redis Stream consumer interoperate"
