#!/usr/bin/env bash
set -euo pipefail

# Single-node MinIO/Warp baseline. This is a *separate protocol/client
# baseline*, not a request-by-request ranking against MiniDriver: Warp uses
# S3/SigV4 and a duration-based workload while MiniDriver's native benchmark
# validates each finite object via its manifest and SHA-256.

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data_dir="${MINIKV_V3_MINIO_DATA_DIR:-/data-ssd/minidriver-v3-minio-20260829}"
evidence_dir="${MINIKV_V3_MINIO_EVIDENCE_DIR:-/tmp/minidriver-v3-minio-results-20260829}"
port="${MINIKV_V3_MINIO_PORT:-29020}"
name="${MINIKV_V3_MINIO_CONTAINER:-minidriver-v3-minio-baseline}"
duration="${MINIKV_V3_MINIO_DURATION:-12s}"
access_key="${MINIKV_V3_MINIO_ACCESS_KEY:-minidriverbench}"
secret_key="${MINIKV_V3_MINIO_SECRET_KEY:-minidriverbench-temporary-local-only}"

if [[ "${data_dir}" != /data-ssd/* || "${evidence_dir}" != /* ]]; then
    echo "data must be below /data-ssd and evidence must be absolute" >&2
    exit 2
fi
if docker ps -a --format '{{.Names}}' | grep -Fxq "${name}"; then
    echo "container name already exists: ${name}" >&2
    exit 2
fi

mkdir -p "${data_dir}" "${evidence_dir}"
cleanup() {
    docker rm -f "${name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "${name}" --network host \
    -e "MINIO_ROOT_USER=${access_key}" -e "MINIO_ROOT_PASSWORD=${secret_key}" \
    -v "${data_dir}:/data" quay.io/minio/minio:latest \
    server /data --address ":${port}" --console-address ":$((port + 1))" \
    > "${evidence_dir}/container-id.txt"

for _ in $(seq 1 30); do
    if curl --noproxy '*' --silent --fail "http://127.0.0.1:${port}/minio/health/live" >/dev/null; then
        break
    fi
    sleep 1
done
curl --noproxy '*' --silent --fail "http://127.0.0.1:${port}/minio/health/live" >/dev/null
docker inspect "${name}" > "${evidence_dir}/container-inspect.json"

run_warp() {
    local name_suffix="$1"
    shift
    docker run --rm --network host -v "${evidence_dir}:/evidence" minio/warp:latest "$@" \
        --host "127.0.0.1:${port}" --access-key "${access_key}" --secret-key "${secret_key}" \
        --duration "${duration}" --benchdata "/evidence/${name_suffix}.csv.zst" \
        > "${evidence_dir}/${name_suffix}.console.log"
}

# Same logical 16 MiB write family as the existing MiniDriver single-node
# baseline. The report must preserve the different client/protocol semantics.
for concurrency in 1 4 8 16 32; do
    run_warp "put-16MiB-c${concurrency}" put --concurrent "${concurrency}" \
        --obj.size 16MiB --disable-multipart --disable-sha256-payload
done

# Keep a small finite object set in the named bucket, then run warm GET. Warp
# itself drives S3 GET; this is useful as a mature-object-store reference but
# does not replace MiniDriver's P6 hot-object test.
run_warp "seed-get-16MiB" put --concurrent 4 --obj.size 16MiB --disable-multipart \
    --disable-sha256-payload --duration 3s --noclear
for concurrency in 1 4 8 16 32; do
    run_warp "get-warm-16MiB-c${concurrency}" get --concurrent "${concurrency}" \
        --obj.size 16MiB --list-existing --noclear
done

docker logs "${name}" > "${evidence_dir}/server.log" 2>&1
docker inspect "${name}" > "${evidence_dir}/container-inspect-final.json"
printf '%s\n' \
    "MinIO image=quay.io/minio/minio:latest" \
    "Warp image=minio/warp:latest" \
    "network=host data_dir=${data_dir} duration=${duration}" \
    "PUT uses --disable-multipart --disable-sha256-payload" \
    "GET uses warm existing objects; page cache is not dropped" \
    "not a request-equivalent MiniDriver comparison" > "${evidence_dir}/README.txt"

echo "MinIO/Warp evidence: ${evidence_dir}"
