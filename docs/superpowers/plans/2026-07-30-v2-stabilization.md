# V2 Stabilization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the V2 distributed upload/download path semantically stable and repeatably testable before using the new low-latency private network for a performance baseline.

**Architecture:** Gateway remains the control plane and DataNode remains the byte plane. Gateway leases reserve placement capacity; DataNode independently admits physical writes. The repair work makes retry semantics explicit, builds a manifest from one read snapshot, and serves persisted extents without copying whole chunks into application memory.

**Tech Stack:** C++17, LevelDB, OpenSSL SHA-256, Linux epoll/sendfile, CMake/CTest, existing V2 HTTP protocol.

## Global Constraints

- Keep V2 as one Gateway and one EventLoop per process.
- Do not alter `data/`, profiling outputs, or unrelated dirty worktree files.
- No new RPC, multi-Reactor, disk worker pool, repair/GC, or user system in this plan.
- Every production behavior change has a failing executable test first.
- Benchmarks run only after all executable regression tests pass.

---

### Task 1: Complete Lease and Commit Retry Semantics

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `test/test_gateway_write_lease.cpp`

**Produces:** a repeated `(sessionId, index, hash, size)` commit returns success without duplicating metadata; an explicit failed upload releases its lease immediately.

- [x] Write tests for duplicate commit success and explicit lease release.
- [x] Run the test and observe the current duplicate commit failure.
- [x] Add a typed commit result and `releaseLease` operation, updating route/session metadata only once.
- [x] Map retry success and invalid commit/lease states to deterministic HTTP responses.
- [x] Run `test_gateway_write_lease`.

### Task 2: Build Manifest From One Gateway Snapshot

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Modify: `src/gateway/gateway_main.cpp`
- Create: `test/test_gateway_manifest_snapshot.cpp`
- Modify: `CMakeLists.txt`

**Produces:** `buildManifestSnapshot(fileHash, out)` copies a file, each route, and required node records under one Gateway mutex; JSON serialization happens after unlocking.

- [x] Write a test for a file whose two chunks map to distinct replicas.
- [x] Run the test and observe that the snapshot API is missing.
- [x] Implement the snapshot value type and one-lock copy method.
- [x] Build the HTTP manifest exclusively from the snapshot.
- [x] Run the new test and existing lease test.

### Task 3: Serve Physical Extents With Offset-Aware sendfile

**Files:**
- Modify: `include/DataNode/FastDataStore.hpp`
- Modify: `src/DataNode/FastDataStore.cpp`
- Modify: `include/network/TcpConnection.hpp`
- Modify: `src/network/TcpConnection.cpp`
- Modify: `src/DataNode/datanode_main.cpp`
- Create: `test/test_fast_data_store_region.cpp`
- Create: `test/test_tcp_sendfile_offset.cpp`
- Modify: `CMakeLists.txt`

**Produces:** `FastDataStore::getRegion()` exposes an owned duplicate fd, offset, and length; HTTP GET sends headers then uses that region. DataNode no longer SHA-checks every ordinary download.

- [x] Write and run a non-zero-offset region test; it failed because the region API was absent.
- [x] Implement region lookup and `TcpConnection::startSendFile(path, offset, length)`.
- [x] Change the DataNode GET handler to use a `HttpResponse` file-region body.
- [x] Run region and existing storage-read regression tests.
- [x] Add a socket-level sendfile offset regression and verify downloaded bytes and hash externally.

### Task 4: Remove Known Queue Movement Without Changing Ownership

**Files:**
- Modify: `include/DataNode/ReplicaUploadPipe.hpp`
- Modify: `src/DataNode/ReplicaUploadPipe.cpp`
- Modify: `include/network/TcpConnection.hpp`
- Modify: `src/network/TcpConnection.cpp`
- Create: `test/test_replica_upload_pipe_queue.cpp`

**Produces:** `std::deque<std::string>` queue consumption via `pop_front()` and move-aware `TcpConnection::send(std::string)` while retaining safe byte ownership.

- [x] Defer this item to V4 after measurement. The V2 pending queue is capped at
  256 KiB, so replacing `vector.erase(begin())` is not a correctness repair and
  does not justify changing byte-ownership semantics before a profile proves it hot.

### Task 5: Make Regressions and Metrics Runnable

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `src/DataNode/datanode_main.cpp`
- Modify: `docs/V2_LOCAL_BENCHMARK.md`

**Produces:** CTest registration, machine-readable counters for route capacity rejection, lease expiration, admission rejection, replica pause, and output-buffer high watermark.

- [x] Add CTest registrations for every test binary.
- [ ] Add only atomic counters needed by the existing benchmark report.
- [ ] Add an admin metrics JSON endpoint to Gateway and DataNode.
- [x] Run `ctest --test-dir build-perf --output-on-failure` (12/12 passed).

### Task 6: Establish the Private-Network Baseline and Publish

**Files:**
- Modify: `docs/V2_LOCAL_BENCHMARK.md`
- Create: `docs/V2_PRIVATE_NETWORK_BASELINE_2026-07-30.md`
- Modify: `docs/V2_GIT_UPDATE_AND_DEPLOY.md`

**Produces:** reproducible commands for 1/2/4/8 concurrent files, 4 MiB and 1 GiB inputs, upload/download latency percentiles, route rejections, pause count, and buffer peak; then a documented Git commit/push/deploy procedure.

- [ ] Start Gateway and three DataNodes on private addresses using separate runtime directories.
- [ ] Run release benchmarks after CTest passes.
- [ ] Record hardware, Git revision, CMake flags, network topology, commands, output, and failures.
- [ ] Commit only source, tests, configuration templates, docs, and web assets; do not commit runtime DBs, secrets, build directories, or perf data.
- [ ] Push the V2 branch and pull fast-forward on each DataNode.

## Scope Review

- V2 correctness gaps from the current fix plan are covered by Tasks 1-3.
- Known user-space queue waste is covered by Task 4 without claiming zero-copy upload.
- Reproducibility and low-latency LAN measurement are covered by Tasks 5-6.
- V4 multi-loop, worker-pool, connection-pool, and adaptive-window work is intentionally excluded.
