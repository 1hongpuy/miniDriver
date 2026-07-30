# V2 Local Performance Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure MiniKVCine V2's local Gateway/DataNode data plane at controlled concurrency and produce reproducible throughput, system, and flame-graph evidence.

**Architecture:** Reuse the isolated local Gateway plus two DataNode cluster and extend the existing benchmark executable with a bounded number of independent file workers. Each worker keeps the existing upload, chain-replica, manifest, download, and checksum semantics. System metrics and `perf` are collected outside the benchmark so benchmark code remains focused on workload execution.

**Tech Stack:** C++17, existing `minikv_v2_bench`, pthread, Linux `perf`, `vmstat`, FlameGraph scripts.

## Global Constraints

- Do not modify runtime `data/`; all benchmark data belongs under `/tmp`.
- Run local tests through `build/bin` and keep existing V2 API semantics unchanged.
- Report local loopback results separately from cross-Tailscale results.
- Limit the 2 GiB node equivalent workload to bounded worker counts and clean temporary cluster data after capture.

---

### Task 1: Add a bounded concurrent benchmark workload

**Files:**
- Modify: `include/benchmark/BenchmarkTypes.hpp`
- Modify: `include/benchmark/BenchmarkCli.hpp`
- Modify: `src/benchmark/BenchmarkCli.cpp`
- Modify: `src/benchmark/BenchmarkRunner.cpp`
- Test: `test/test_benchmark_cli.cpp`

**Interfaces:**
- Produces: `BenchmarkOptions::concurrency` parsed from `--concurrency N`, with `N >= 1`.
- Produces: one independent remote object per worker and aggregate wall-clock throughput.

- [x] Add a failing CLI test for `--concurrency 4` and invalid zero concurrency.
- [x] Run `test_benchmark_cli` and verify the new assertions fail before implementation.
- [x] Add the option, parse and validate it, and execute bounded worker threads in `BenchmarkRunner`.
- [x] Run benchmark unit tests and existing V2 tests.

### Task 2: Capture the local performance matrix

**Files:**
- Create: `/tmp/minikv-v2-perf-<timestamp>/`

**Interfaces:**
- Consumes: `minikv_v2_bench local --concurrency N`.
- Produces: CSV/JSON benchmark output, `vmstat`, `perf stat`, `perf.data`, folded stacks and SVG flame graph.

- [x] Start the isolated Gateway plus two DataNode cluster.
- [x] Run 64 MiB workloads at concurrency 1, 2 and 4, with three repetitions.
- [x] Run one 1 GiB single-worker workload as the bounded large-file baseline.
- [x] Capture `vmstat` and retain an existing same-path `cpu-clock:u` flame graph; a new capture was blocked by host `perf_event_paranoid=4`.
- [x] Stop the cluster while retaining report artifacts. The empty cluster state directory remains under `/tmp` for inspection.

### Task 3: Publish findings

**Files:**
- Create: `docs/V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md`

**Interfaces:**
- Consumes: benchmark CSVs and profiling artifacts.
- Produces: stated methodology, local-only caveats, measured bottlenecks, and a prioritized V4 optimization backlog.

- [x] Record commands, workload shape and build mode.
- [x] Separate measured facts from hypotheses.
- [x] Link every identified hotspot to a concrete source location.
- [x] Verify benchmark/V2 regression tests and `git diff --check`.

### Task 4: Complete bounded V2 test matrix and classify gaps

**Files:**
- Modify: `docs/V2_COMPLETE_PERFORMANCE_TEST_AND_ANALYSIS.md`
- Create: `/tmp/minikv-v2-v2complete-<timestamp>/`

**Interfaces:**
- Consumes: the isolated local cluster, benchmark executable, `vmstat`, and any installed HTTP load tool.
- Produces: a reproducible 8-file-placement failure result, bounded control-plane and mixed read/write observations, and an explicit list of metrics that V2 cannot yet emit.

- [x] Verify the current build and regression tests before workload execution.
- [x] Reproduce and preserve the 8-file Placement failure boundary without changing production code.
- [x] Run the feasible control-plane and mixed read/write tests; record unavailable tools or missing instrumentation as gaps.
- [x] Update the report with measured results, root-cause confidence, and a V2/V4/V5 optimization order.

## Active Test Constraints

- Current `/tmp` free space is approximately 3.7 GiB. Do not run a 4 GiB workload or create additional multi-round 1 GiB data.
- Do not delete existing `/tmp` benchmark, profile, or runtime artifacts without explicit user approval.
- Do not run `tc` fault injection against real nodes. A loopback-only experiment is permitted only if its metrics can be observed.

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| `ctest` reports no tests | 1 | The project builds standalone test executables but does not call `add_test`; run the binaries under `build-perf/bin` and record CTest registration as a test-infrastructure gap. |
| Test binaries not found under `build-perf/` | 1 | Inspect the actual output directory before retrying; do not repeat the same path. |
| `test_async_http_timeout` aborts | 1 | The workspace sandbox blocked local sockets; rerunning with approved loopback access passed. |
| `test_benchmark_http` aborts creating a listening socket | 1 | FD limits and ports were normal; approved loopback rerun passed, confirming a sandbox limitation rather than a code regression. |
| First control-plane script aborted with `name: unbound variable` | 1 | Treat all output as invalid, stop the isolated cluster, and rerun with a simpler non-`set -u` aggregation command. |
