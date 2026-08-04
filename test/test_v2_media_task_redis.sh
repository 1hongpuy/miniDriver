#!/usr/bin/env bash

set -euo pipefail

binary="$1"
port="${MINIKV_TEST_REDIS_PORT:-16379}"
root="$(mktemp -d)"
pid=""

cleanup() {
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf "$root"
}
trap cleanup EXIT

redis-server --bind 127.0.0.1 --port "$port" --save '' --appendonly no \
    --daemonize no >"$root/redis.log" 2>&1 &
pid="$!"

for _ in $(seq 1 50); do
    if redis-cli -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

redis-cli -h 127.0.0.1 -p "$port" ping | grep -qx PONG
MINIKV_REDIS_TEST_PORT="$port" "$binary"
redis-cli -h 127.0.0.1 -p "$port" XRANGE media:thumbnail:test - + | grep -qx job-one

echo "PASS: Redis task publisher writes a bounded D2 thumbnail message"
