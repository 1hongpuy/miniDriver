#!/usr/bin/env bash
set -euo pipefail

# P6-READ orchestration for an already running MiniDriver benchmark cluster.
# It intentionally does not start MinIO and it does not drop Linux page cache:
# cold-cache control requires host authority and must be recorded in the report.
# Use this tool for reproducible warm-cache/local-regression profiles, then run
# the same representative R1/R3/R4 cases on the real three-host topology.

if [[ $# -ne 2 ]]; then
    echo "usage: $0 GATEWAY_HOST:PORT ABSOLUTE_RESULT_DIR" >&2
    exit 2
fi

gateway="$1"
result_dir="$2"
if [[ "${result_dir}" != /* ]]; then
    echo "result directory must be absolute" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bench="${MINIKV_V2_BENCH_BIN:-${repo_dir}/build/bin/minikv_v2_bench}"
runs="${MINIKV_V3_P6_RUNS:-3}"
remote_dir="${MINIKV_V3_P6_REMOTE_DIR:-/p6-read}"
budget="${MINIKV_V3_P6_GLOBAL_CHUNK_BUDGET:-8}"
sdk_read_plan="${MINIKV_V3_P6_SDK_READ_PLAN:-true}"
service_principal="${MINIKV_V3_P6_SERVICE_PRINCIPAL:-benchmark-p6-read}"

if [[ ! -x "${bench}" ]]; then
    echo "missing benchmark executable: ${bench}" >&2
    exit 2
fi
if [[ ! "${runs}" =~ ^[1-9][0-9]*$ ]]; then
    echo "MINIKV_V3_P6_RUNS must be positive" >&2
    exit 2
fi
case "${sdk_read_plan}" in
    true|false) ;;
    *) echo "MINIKV_V3_P6_SDK_READ_PLAN must be true or false" >&2; exit 2 ;;
esac
sdk_args=()
if [[ "${sdk_read_plan}" == true ]]; then
    if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
        echo "MINIKV_V2_CLUSTER_SECRET is required when SDK ReadPlan is enabled" >&2
        exit 2
    fi
    sdk_args=(--sdk-read-plan true --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}"
              --service-principal "${service_principal}")
fi

mkdir -p "${result_dir}"
printf '%s\n' \
    "cache_mode=warm (fixtures are uploaded before the timed reads)" \
    "cold_cache=not run by this script; use a separately authorized host procedure" \
    "gateway=${gateway}" \
    "runs=${runs}" \
    "global_chunk_budget=${budget}" \
    "sdk_read_plan=${sdk_read_plan}" \
    "service_principal=${service_principal}" > "${result_dir}/README.txt"

run_bench() {
    local name="$1"
    shift
    echo "=== ${name} ==="
    "${bench}" local --gateway "${gateway}" --work-dir "${result_dir}/${name}" \
        --runs "${runs}" --chunk-window 2 --global-chunk-budget "${budget}" \
        --remote-dir "${remote_dir}" "${sdk_args[@]}" "$@"
}

# R1: same 100 MiB object for every reader.  The same command also covers
# the hot-object selection policy for different server/default admission caps.
for concurrency in 1 4 8 16 32 64; do
    run_bench "r1-hot-object-c${concurrency}" \
        --sizes 100MiB --concurrency "${concurrency}" --mode download \
        --read-profile hot-object --connection-mode close
done

# R2: a single storage Chunk. It detects connection/admission overhead that a
# large object can amortize away.
for concurrency in 1 4 8 16 32 64; do
    run_bench "r2-hot-chunk-c${concurrency}" \
        --sizes 4MiB --concurrency "${concurrency}" --mode download \
        --read-profile hot-object --connection-mode close
done

# R3: independent fixtures; this is intentionally not an object-placement or
# replica-aware scheduler benchmark.
for size in 64KiB 1MiB 16MiB; do
    for concurrency in 1 4 8 16 32; do
        run_bench "r3-random-${size}-c${concurrency}" \
            --sizes "${size}" --concurrency "${concurrency}" --mode download \
            --read-profile independent --connection-mode close
    done
done

# R4: all sizes are active in each timed round. This reports a row per size,
# so thumbnail P95/P99 can be inspected separately from original-file reads.
for concurrency in 4 8 16 32; do
    run_bench "r4-mixed-size-c${concurrency}" \
        --sizes 64KiB,150KiB,1MiB,16MiB,50MiB --concurrency "${concurrency}" \
        --mode download --read-profile mixed-size --connection-mode close
done

# R6: repeated reads on each worker make the 64/150 KiB connection-mode
# comparison meaningful. `runs.csv` carries direct DataNode connection opens,
# requests and reuses; Gateway manifest calls are deliberately excluded.
for mode in close keep-alive; do
    for size in 64KiB 150KiB; do
        for concurrency in 1 4 8 16 32; do
            run_bench "r6-${mode}-${size}-c${concurrency}" \
                --sizes "${size}" --concurrency "${concurrency}" --mode download \
                --read-profile hot-object --connection-mode "${mode}" \
                --requests-per-worker 50
        done
    done
done

cat <<'EOF'
R5 slow-reader isolation is intentionally not run here. It needs a separate
throttled client plus server RSS/output-watermark sampling; do not mistake a
normal read benchmark for that isolation test.
EOF
