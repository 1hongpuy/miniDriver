#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
kubectl_cmd=(sudo k3s kubectl)
report_dir="/var/lib/minidriver-bench/placement-smoke"

show_reports() {
  echo "Placement smoke reports on minidriver-1: ${report_dir}"
  if sudo test -f "${report_dir}/summary.csv"; then
    echo "summary.csv:"
    sudo cat "${report_dir}/summary.csv"
  fi
  if sudo test -f "${report_dir}/upload-stages-summary.csv"; then
    echo "upload-stages-summary.csv:"
    sudo cat "${report_dir}/upload-stages-summary.csv"
  fi
}

"${kubectl_cmd[@]}" -n minidriver delete job placement-smoke \
  --ignore-not-found=true --wait=true
"${kubectl_cmd[@]}" apply \
  -f "${repo_root}/deploy/k3s-ha/benchmark-placement-job.yaml"
echo "Waiting for placement-smoke (5s warmup + 60s steady; reports are written at the end)..."
"${kubectl_cmd[@]}" -n minidriver wait \
  --for=condition=complete job/placement-smoke --timeout=180s || {
    "${kubectl_cmd[@]}" -n minidriver describe job placement-smoke
    "${kubectl_cmd[@]}" -n minidriver logs job/placement-smoke --all-containers=true
    show_reports
    exit 1
  }
"${kubectl_cmd[@]}" -n minidriver logs job/placement-smoke --all-containers=true
show_reports
