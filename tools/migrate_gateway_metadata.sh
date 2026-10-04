#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 SOURCE_METADATA_DIR TARGET_METADATA_DIR" >&2
  exit 2
fi

source_dir=$1
target_dir=$2

if [[ ! -d "$source_dir" ]]; then
  echo "source metadata directory does not exist: $source_dir" >&2
  exit 1
fi
if [[ -e "$target_dir" ]]; then
  if [[ -n "$(find "$target_dir" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "refusing to overwrite non-empty target: $target_dir" >&2
    exit 1
  fi
else
  mkdir -p "$target_dir"
fi

mapfile -t source_files < <(find "$source_dir" -maxdepth 1 -type f -printf '%f\n' | sort)
if [[ ${#source_files[@]} -eq 0 ]]; then
  echo "source contains no LevelDB files: $source_dir" >&2
  exit 1
fi

for file in "${source_files[@]}"; do
  cp -p -- "$source_dir/$file" "$target_dir/$file"
done

source_bytes=$(du -cb "$source_dir"/* 2>/dev/null | awk 'END {print $1+0}')
target_bytes=$(du -cb "$target_dir"/* 2>/dev/null | awk 'END {print $1+0}')
if [[ "$source_bytes" != "$target_bytes" ]]; then
  echo "metadata migration size validation failed: source=$source_bytes target=$target_bytes" >&2
  exit 1
fi

printf 'source=%s\ntarget=%s\nfiles=%s\nbytes=%s\n' \
  "$source_dir" "$target_dir" "${#source_files[@]}" "$target_bytes" \
  > "$target_dir/.minikv-metadata-migrated"
echo "metadata copied and size-validated; source was preserved"
