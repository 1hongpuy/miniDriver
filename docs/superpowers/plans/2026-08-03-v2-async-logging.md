# V2 Asynchronous Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded `spdlog` asynchronous logging to V2 services and emit per-Chunk timing that identifies upload bottlenecks.

**Architecture:** A small `miniKV::utils::AsyncLogger` wrapper owns one process-local spdlog asynchronous logger. Node Agent parses per-service logging YAML and exports it to children as environment variables. Gateway and DataNode initialize the wrapper once; V2 hot paths log one structured completion/failure event rather than writing synchronously for every body callback.

**Tech Stack:** C++17, spdlog, yaml-cpp, CMake, existing CTest binaries.

## Global Constraints

- Keep V1 logging behavior outside this scope.
- Use an 8192-entry non-blocking queue with `overrun_oldest`.
- Rotate at 20 MiB and retain five files unless YAML overrides the defaults.
- Do not log every 64 KiB body callback at INFO level.
- Preserve existing Node Agent stdout/stderr redirection for startup and fatal failures.
- Do not modify runtime `data/`, profiling data, or unrelated dirty files.

---

### Task 1: Add the process-local asynchronous logger

**Files:**
- Create: `include/utils/AsyncLogger.hpp`
- Create: `src/utils/AsyncLogger.cpp`
- Create: `test/test_async_logger.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces `miniKV::utils::LogConfig`, `initAsyncLogger`, `shutdownAsyncLogger`, `setLogContext`, `logInfo`, `logWarn`, `logError`, and `logDebug`.
- Later service mains consume `initAsyncLogger(LogConfig)` and `setLogContext(process, nodeId)`.

- [ ] **Step 1: Write the failing test**

```cpp
#include "utils/AsyncLogger.hpp"

int main() {
    miniKV::utils::LogConfig config;
    config.filePath = "/tmp/minikv_async_logger_test.log";
    config.level = "debug";
    config.queueSize = 8;
    miniKV::utils::initAsyncLogger(config);
    miniKV::utils::setLogContext("test", "node-test");
    miniKV::utils::logInfo("event=chunk_complete session=s1 chunk=0 bytes=1024");
    miniKV::utils::shutdownAsyncLogger();
    return containsFile(config.filePath, "process=test node=node-test event=chunk_complete");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cmake --build build --target test_async_logger -j2`

Expected: compilation fails because `utils/AsyncLogger.hpp` does not exist.

- [ ] **Step 3: Implement the minimal logger wrapper**

```cpp
struct LogConfig {
    std::string filePath;
    std::string level = "info";
    size_t queueSize = 8192;
    size_t rotateBytes = 20 * 1024 * 1024;
    size_t rotateFiles = 5;
};

void initAsyncLogger(const LogConfig& config);
void setLogContext(const std::string& process, const std::string& nodeId);
void logInfo(const std::string& fields);
```

`AsyncLogger.cpp` creates one `spdlog::async_logger` with a rotating file sink and
`spdlog::async_overflow_policy::overrun_oldest`. The emitted line pattern includes
timestamp, level, process, node, thread id, and the supplied fields. The wrapper
counts overflow notices and writes one warning summary at most once per minute.

- [ ] **Step 4: Register the source and test in CMake**

```cmake
find_package(spdlog CONFIG REQUIRED)
target_link_libraries(minikv_utils PUBLIC spdlog::spdlog)

add_executable(test_async_logger test/test_async_logger.cpp)
target_link_libraries(test_async_logger PRIVATE minikv_utils pthread)
add_test(NAME async_logger COMMAND test_async_logger)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cmake -S . -B build && cmake --build build --target test_async_logger -j2 && ctest --test-dir build -R async_logger --output-on-failure`

Expected: `async_logger` passes and the temporary log contains the structured event.

- [ ] **Step 6: Commit**

```bash
git add include/utils/AsyncLogger.hpp src/utils/AsyncLogger.cpp test/test_async_logger.cpp CMakeLists.txt
git commit -m "feat: add asynchronous structured logger"
```

### Task 2: Add YAML logging configuration and child environment propagation

**Files:**
- Modify: `include/agent/NodeAgentConfig.hpp`
- Modify: `src/agent/NodeAgentConfig.cpp`
- Modify: `src/agent/node_agent_main.cpp`
- Create: `test/test_node_agent_logging_config.cpp`

**Interfaces:**
- Consumes `ServiceLogs` and `ManagedServiceConfig`.
- Produces `ServiceLoggingConfig` with `filePath`, `level`, `queueSize`, `rotateBytes`, and `rotateFiles`.
- Exports `MINIKV_V2_LOG_FILE`, `MINIKV_V2_LOG_LEVEL`, `MINIKV_V2_LOG_QUEUE_SIZE`, `MINIKV_V2_LOG_ROTATE_BYTES`, and `MINIKV_V2_LOG_ROTATE_FILES` through `ChildSpec::environment`.

- [ ] **Step 1: Write the failing YAML parsing test**

```cpp
const auto config = parseNodeAgentConfigText(R"(
node: { nodeId: node-c, advertiseAddress: 100.89.50.125 }
cluster: { secretFile: /tmp/secret, gatewayAddress: 100.75.93.124, gatewayPort: 18081 }
services:
  - id: datanode-0
    type: datanode
    listenPort: 9002
    dataDir: /tmp/data
    logging: { file: /tmp/logs/datanode.log, level: debug, queueSize: 4096 }
)");
CHECK(config.services[0].logging.filePath == "/tmp/logs/datanode.log");
CHECK(config.services[0].logging.queueSize == 4096);
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cmake --build build --target test_node_agent_logging_config -j2`

Expected: compilation fails because `ManagedServiceConfig::logging` is absent.

- [ ] **Step 3: Implement configuration parsing and defaults**

```cpp
struct ServiceLoggingConfig {
    std::string filePath;
    std::string level = "info";
    uint32_t queueSize = 8192;
    uint64_t rotateBytes = 20ULL * 1024ULL * 1024ULL;
    uint32_t rotateFiles = 5;
};
```

Read an optional `logging` YAML map. If `file` is absent, derive
`<dataDir>/logs/<service-id>.log`. Reject zero queue size, zero rotate size, and
an unsupported level. In `node_agent_main.cpp`, copy values into the child
environment map before calling `supervisor.start`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cmake --build build --target test_node_agent_logging_config -j2 && ctest --test-dir build -R node_agent_logging_config --output-on-failure`

Expected: YAML values and defaults are parsed deterministically.

- [ ] **Step 5: Commit**

```bash
git add include/agent/NodeAgentConfig.hpp src/agent/NodeAgentConfig.cpp src/agent/node_agent_main.cpp test/test_node_agent_logging_config.cpp CMakeLists.txt
git commit -m "feat: configure V2 service logging from YAML"
```

### Task 3: Initialize logging in service processes and remove synchronous V2 diagnostics

**Files:**
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `src/DataNode/datanode_main.cpp`
- Modify: `src/agent/node_agent_main.cpp`
- Modify: `include/http/HttpContext.hpp`
- Modify: `include/http/HttpServer.hpp`
- Modify: `src/DataNode/ReplicaUploadPipe.cpp`

**Interfaces:**
- Consumes process environment variables from Task 2 and `AsyncLogger` from Task 1.
- Produces async lifecycle, route, replica, and failure events.

- [ ] **Step 1: Write a failing logging-level test**

```cpp
miniKV::utils::initAsyncLogger({logPath, "info", 64, 1024 * 1024, 1});
miniKV::utils::logDebug("event=http_request path=/ignored");
miniKV::utils::logInfo("event=gateway_start port=18081");
miniKV::utils::shutdownAsyncLogger();
CHECK(!containsFile(logPath, "event=http_request"));
CHECK(containsFile(logPath, "event=gateway_start"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `ctest --test-dir build -R async_logger --output-on-failure`

Expected: level filtering is not yet covered by the logger test.

- [ ] **Step 3: Initialize and use the logger**

At each V2 main startup, read environment variables into `LogConfig`, call
`initAsyncLogger`, set `process` and `node`, then emit `event=process_start`.
Replace V2 `std::cerr`/`std::cout` hot-path calls with level-appropriate wrapper
calls. Remove the unconditional request-line and response debug output from
`HttpContext.hpp` and `HttpServer.hpp`; retain it only through `logDebug`.

- [ ] **Step 4: Verify build and filtered output**

Run: `cmake --build build --target minikv_v2_gateway minikv_v2_datanode minikv_v2_node_agent test_async_logger -j2 && ctest --test-dir build -R async_logger --output-on-failure`

Expected: all targets build; INFO contains lifecycle lines while DEBUG lines are absent at INFO level.

- [ ] **Step 5: Commit**

```bash
git add src/gateway/gateway_main.cpp src/DataNode/datanode_main.cpp src/agent/node_agent_main.cpp include/http/HttpContext.hpp include/http/HttpServer.hpp src/DataNode/ReplicaUploadPipe.cpp test/test_async_logger.cpp
git commit -m "refactor: route V2 diagnostics through async logger"
```

### Task 4: Add chunk pipeline timing and backpressure summary

**Files:**
- Modify: `src/DataNode/datanode_main.cpp`
- Modify: `include/DataNode/ReplicaUploadPipe.hpp`
- Modify: `src/DataNode/ReplicaUploadPipe.cpp`
- Create: `test/test_replica_upload_metrics.cpp`

**Interfaces:**
- Produces `ReplicaUploadMetrics { pauseCount, pauseNanoseconds, maxPendingBytes }`.
- `ChunkUploadStream` emits `event=chunk_complete` and `event=chunk_failed` using stage durations.

- [ ] **Step 1: Write the failing metrics test**

```cpp
ReplicaUploadMetrics metrics;
metrics.recordPaused(524288, start);
metrics.recordResumed(start + std::chrono::milliseconds(25));
CHECK(metrics.pauseCount == 1);
CHECK(metrics.pauseNanoseconds >= 25000000);
CHECK(metrics.maxPendingBytes == 524288);
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cmake --build build --target test_replica_upload_metrics -j2`

Expected: compilation fails because `ReplicaUploadMetrics` is absent.

- [ ] **Step 3: Implement metrics and completion event**

Add monotonic timestamps to `ChunkUploadStream`: accepted, first body, local finish,
replica completion, Gateway commit completion, and client response. Increment metrics
only at actual pause/resume transitions. Emit one INFO completion line with
`total_ms`, `local_write_ms`, `replica_ms`, `gateway_commit_ms`, `pauses`,
`pause_ms`, and `max_pending_bytes`; emit an ERROR line with `stage` and `error` on
failure.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cmake --build build --target test_replica_upload_metrics -j2 && ctest --test-dir build -R replica_upload_metrics --output-on-failure`

Expected: pause timing and pending-byte maximum assertions pass.

- [ ] **Step 5: Commit**

```bash
git add src/DataNode/datanode_main.cpp include/DataNode/ReplicaUploadPipe.hpp src/DataNode/ReplicaUploadPipe.cpp test/test_replica_upload_metrics.cpp CMakeLists.txt
git commit -m "feat: log DataNode chunk pipeline timings"
```

### Task 5: Add Gateway preflight, routing, and commit observability

**Files:**
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `src/gateway/GategayState.cpp`
- Modify: `test/test_gateway_upload_preflight.cpp`

**Interfaces:**
- Consumes `AsyncLogger` from Task 1.
- Produces `event=upload_preflight`, `event=route_plan`, `event=chunk_commit`, and `event=file_commit` logs.

- [ ] **Step 1: Extend the failing preflight test**

```cpp
CHECK(logContains(logPath, "event=upload_preflight outcome=CONTENT_EXISTS"));
CHECK(logContains(logPath, "event=upload_preflight outcome=PATH_CONFLICT"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cmake --build build --target test_gateway_upload_preflight -j2 && ./build/bin/test_gateway_upload_preflight`

Expected: assertions fail because GatewayState does not emit preflight events.

- [ ] **Step 3: Emit only terminal Gateway decision events**

Log preflight outcome, present/missing count, route primary/replica IDs, commit
status, and elapsed time. Heartbeats remain DEBUG except state transitions.

- [ ] **Step 4: Verify the focused regression suite**

Run: `cmake --build build --target test_gateway_upload_preflight test_gateway_catalog test_gateway_manifest_snapshot -j2 && ctest --test-dir build -R 'gateway_upload_preflight|gateway_catalog|gateway_manifest_snapshot' --output-on-failure`

Expected: all focused Gateway tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/gateway/gateway_main.cpp src/gateway/GategayState.cpp test/test_gateway_upload_preflight.cpp
git commit -m "feat: add Gateway upload observability"
```

### Task 6: Document deployment and verify against a delayed replica

**Files:**
- Modify: `docs/V2_GIT_UPDATE_AND_DEPLOY.md`
- Modify: `config/*.yaml`

**Interfaces:**
- Consumes the YAML `logging` map and emitted completion events from Tasks 2-5.
- Produces deployment instructions and a reproducible log query workflow.

- [ ] **Step 1: Add example YAML**

```yaml
logging:
  file: /home/ubuntu/minikv-v2/logs/datanode.log
  level: info
  queueSize: 8192
  rotateBytes: 20971520
  rotateFiles: 5
```

- [ ] **Step 2: Build and run the affected CTest set**

Run: `cmake -S . -B build && cmake --build build -j2 && ctest --test-dir build -R 'async_logger|node_agent_logging_config|replica_upload_metrics|gateway_upload_preflight' --output-on-failure`

Expected: all four test groups pass.

- [ ] **Step 3: Manual delayed-replica verification**

Run: `tail -f ~/minikv-v2/logs/datanode-<nodeId>.log | grep 'event=chunk_'`

Expected: every transfer ends with a single `chunk_complete` or `chunk_failed` line;
with a rate-limited replica, `replica_ms` and `pause_ms` dominate the line.

- [ ] **Step 4: Commit**

```bash
git add docs/V2_GIT_UPDATE_AND_DEPLOY.md config
git commit -m "docs: document V2 async logging deployment"
```

## Self-Review

- Spec coverage: queue safety, rotation, process ownership, YAML propagation,
  structured fields, timing instrumentation, and deployment verification map to
  Tasks 1-6.
- Placeholder scan: no deferred implementation markers are present.
- Type consistency: `LogConfig` feeds `ServiceLoggingConfig`; the environment
  variables connect Node Agent configuration to process initialization; chunk
  timing uses `ReplicaUploadMetrics` in the two DataNode files.
