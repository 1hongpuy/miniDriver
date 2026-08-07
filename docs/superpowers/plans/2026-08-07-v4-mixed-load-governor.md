# V4.1 Mixed Load Governor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound DataNode upload and download concurrency and ensure one sendfile response cannot monopolize an EventLoop callback.

**Architecture:** A process-wide `NodeResourceGovernor` issues RAII leases for upload and download work, including optional per-client limits. `TcpConnection` sends file bodies in bounded byte turns and reports completion or cancellation so download leases are always released.

**Tech Stack:** C++17, epoll, Linux `sendfile`, CMake/CTest.

## Global Constraints

- Preserve existing V2 HTTP endpoints and metadata contracts.
- Keep socket operations on the owning EventLoop.
- Do not add Multi-Reactor, HTTP/2, gRPC, or durable `fdatasync` semantics in V4.1.
- Do not edit unrelated dirty frontend files.

---

### Task 1: Node Resource Governor

**Files:**
- Create: `include/DataNode/NodeResourceGovernor.hpp`
- Create: `test/test_node_resource_governor.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces: `NodeResourceGovernor::tryAcquireUpload(clientId)` and `tryAcquireDownload(clientId)` returning move-only RAII leases.
- Produces: total and per-client active counters for observability.

- [x] Write tests proving global limits, per-client limits, move semantics, and automatic release.
- [x] Run the test and verify compilation fails because the governor does not exist.
- [x] Implement the minimal thread-safe governor.
- [x] Run the focused test and verify it passes.

### Task 2: DataNode Upload Migration

**Files:**
- Modify: `src/DataNode/datanode_main.cpp`

**Interfaces:**
- Consumes: `NodeResourceGovernor::UploadLease`.
- Produces: upload lease ownership tied to `ChunkUploadStream` lifetime.

- [x] Add a compile-level integration test through the DataNode target.
- [x] Replace manual `WriteAdmission` acquire/release state with an upload lease.
- [x] Keep heartbeat `activeUploads` sourced from the governor.
- [x] Build the DataNode and run upload/disk pipeline tests.

### Task 3: Bounded Sendfile Turns

**Files:**
- Modify: `include/network/TcpConnection.hpp`
- Modify: `src/network/TcpConnection.cpp`
- Modify: `test/test_tcp_sendfile_offset.cpp`

**Interfaces:**
- Produces: configurable `setSendFileQuantum(size_t)`.
- Produces: a file completion callback invoked exactly once with success or cancellation.

- [x] Extend the sendfile test to require a completion callback and bounded maximum syscall size.
- [x] Run it and verify failure against the old API.
- [x] Send at most one configured byte quantum per EventLoop write turn.
- [x] Preserve partial-write offsets and invoke completion on success, error, or connection close.
- [x] Run the focused network test.

### Task 4: Download Admission Integration

**Files:**
- Modify: `include/http/HttpResponse.hpp`
- Modify: `include/http/HttpServer.hpp`
- Modify: `src/http/DeferredResponse.cpp`
- Modify: `src/http/CorsPolicy.cpp`
- Modify: `src/DataNode/datanode_main.cpp`

**Interfaces:**
- Consumes: optional `X-Client-Instance-Id` header.
- Produces: HTTP 503 plus `Retry-After: 1` when download capacity is unavailable.
- Produces: automatic download-lease release when file transfer completes or the connection closes.

- [x] Thread the file completion callback from `HttpResponse` into `TcpConnection`.
- [x] Acquire a download lease before setting a GET file body.
- [x] Capture the lease in the completion callback and release it exactly once.
- [x] Permit `X-Client-Instance-Id` in CORS.
- [x] Build and run HTTP/network/DataNode tests.

### Task 5: Verification and Documentation

**Files:**
- Create: `docs/V4_1_MIXED_LOAD_GOVERNOR.md`

- [x] Run focused CTest targets.
- [x] Run the complete CTest suite.
- [x] Record changed ownership, overload semantics, defaults, metrics, and remaining V4 work.
- [x] Inspect `git diff` to confirm unrelated frontend changes were preserved.
