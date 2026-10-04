#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
image="${MINIDRIVER_K3S_IMAGE:-minidriver:4.0-ha-k3s}"
archive="${MINIDRIVER_K3S_IMAGE_ARCHIVE:-/tmp/minidriver-4.0-ha-k3s.tar}"

required=(
  minikv_metadata_raft_service
  minikv_v2_gateway
  minikv_v2_datanode
  minikv_v2_bench
  minikv_storage_sync_probe
)
for binary in "${required[@]}"; do
  if [[ ! -x "${repo_root}/build-ha/bin/${binary}" ]]; then
    echo "missing executable: ${repo_root}/build-ha/bin/${binary}" >&2
    exit 1
  fi
done

docker_cmd=(docker)
if ! docker info >/dev/null 2>&1; then
  docker_cmd=(sudo docker)
fi

build_args=()
if [[ -n "${MINIDRIVER_BUILD_PROXY:-}" ]]; then
  build_args+=(
    --build-arg "HTTP_PROXY=${MINIDRIVER_BUILD_PROXY}"
    --build-arg "HTTPS_PROXY=${MINIDRIVER_BUILD_PROXY}"
    --build-arg "http_proxy=${MINIDRIVER_BUILD_PROXY}"
    --build-arg "https_proxy=${MINIDRIVER_BUILD_PROXY}"
  )
fi

"${docker_cmd[@]}" build \
  --network=host \
  "${build_args[@]}" \
  -f "${repo_root}/deploy/k3s-ha/Dockerfile" \
  -t "${image}" \
  "${repo_root}"

"${docker_cmd[@]}" save -o "${archive}" "${image}"
if [[ ! -r "${archive}" ]]; then
  sudo chown "$(id -u):$(id -g)" "${archive}"
fi
sha256sum "${archive}"
echo "image=${image}"
echo "archive=${archive}"
