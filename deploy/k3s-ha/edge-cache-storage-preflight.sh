#!/usr/bin/env bash
set -euo pipefail

# Inspect the future EdgeCache directory without touching MiniDriver's
# authoritative DataNode data/index/WAL directories.  --check-write creates a
# uniquely named 4 KiB temporary file *only* in the supplied existing path and
# removes it before exiting.  It proves guest-side write permission, not that
# the virtual disk is backed by a physically independent SSD.

usage() {
  cat <<'EOF'
Usage: edge-cache-storage-preflight.sh --path EXISTING_DIRECTORY [--check-write]

Examples:
  deploy/k3s-ha/edge-cache-storage-preflight.sh --path /data/minidriver-edge-cache
  deploy/k3s-ha/edge-cache-storage-preflight.sh --path /data/minidriver-edge-cache --check-write

The target directory must already exist.  Do not pass a DataNode data/index,
Metadata WAL, repository, or root directory as the target.
EOF
}

cache_path=""
check_write=false
while (($#)); do
  case "$1" in
    --path)
      (($# >= 2)) || { usage >&2; exit 2; }
      cache_path="$2"
      shift 2
      ;;
    --check-write)
      check_write=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$cache_path" ]]; then
  usage >&2
  exit 2
fi
if [[ ! -d "$cache_path" ]]; then
  echo "error: --path must be an existing directory: $cache_path" >&2
  exit 2
fi

resolved_path="$(readlink -f -- "$cache_path")"
case "$resolved_path" in
  /|/var/lib/minidriver-data|/var/lib/minidriver-index|/var/lib/minidriver-metadata)
    echo "error: refusing a protected/non-dedicated target: $resolved_path" >&2
    exit 2
    ;;
esac

echo "edge_cache_path=$resolved_path"
echo "host=$(hostname)"
echo "collected_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo
echo "## mount"
findmnt -T "$resolved_path"
echo
echo "## filesystem capacity"
df -hT "$resolved_path"
echo
echo "## guest block devices"
lsblk -o NAME,MAJ:MIN,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL,ROTA
echo
echo "## directory ownership"
stat -c 'path=%n uid=%u gid=%g mode=%A (%a)' "$resolved_path"

if ! $check_write; then
  echo
  echo "write_check=SKIPPED (rerun with --check-write to create and remove one 4 KiB temporary file)"
  echo "physical_ssd_verdict=UNKNOWN (verify VM disk file → Windows volume → physical disk manually)"
  exit 0
fi

probe_file="$(mktemp "$resolved_path/.edge-cache-preflight.XXXXXX")"
cleanup() {
  rm -f -- "$probe_file"
}
trap cleanup EXIT

started_ns="$(date +%s%N)"
dd if=/dev/zero of="$probe_file" bs=4096 count=1 conv=fsync status=none
finished_ns="$(date +%s%N)"
probe_size="$(stat -c %s "$probe_file")"

echo
echo "## temporary write check"
echo "write_check=PASS"
echo "probe_size_bytes=$probe_size"
echo "write_plus_fsync_us=$(((finished_ns - started_ns) / 1000))"
echo "probe_cleanup=scheduled"
echo "physical_ssd_verdict=UNKNOWN (guest write success does not prove host physical isolation)"
