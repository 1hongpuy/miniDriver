# V2 Media Task Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable Gateway media-job ledger and non-blocking Redis Streams dispatch path that D3 JPEG thumbnail workers can consume.

**Architecture:** Gateway persists `MediaJob` and `ThumbnailMeta` in LevelDB before enqueueing a lightweight publish request to a bounded background `RedisTaskPublisher`. The publisher is the only Gateway component allowed to use synchronous hiredis calls. Gateway exposes internal job claim/complete/fail APIs; D3 later adds the Thumbnail Worker binary and JPEG handler on top of these stable contracts.

**Tech Stack:** C++17, LevelDB, hiredis, existing HTTP/EventLoop library, yaml-cpp, CTest.

## Global Constraints

- Gateway LevelDB is the source of truth; Redis Streams is not the only copy of task state.
- Never make a Redis request in a Gateway HTTP/EventLoop callback.
- D2 adds task infrastructure only; it must not decode media or add a thumbnail Worker binary yet.
- A Redis message contains `jobId` and routing fields only; task parameters are obtained through Gateway claim.
- Internal APIs require the existing cluster internal token.
- Preserve existing upload, catalog, deletion, Chunk and direct DataNode protocols.
- Test behavior first and observe each new test fail before adding production code.

---

### Task 1: Add media job value types and deterministic serialization

**Files:**
- Create: `include/media/MediaJob.hpp`
- Create: `src/media/MediaJob.cpp`
- Create: `test/test_media_job.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces `miniKV::media::MediaJob`, `ThumbnailMeta`, `JobState`, `JobType`.
- Produces `serializeMediaJob`, `parseMediaJob`, `serializeThumbnailMeta`, `parseThumbnailMeta`.
- Later tasks store these values under Gateway keys `j:{jobId}` and `d:{fileHash}:thumbnail:{profile}`.

- [ ] **Step 1: Write the failing serialization test**

```cpp
int main() {
    MediaJob expected;
    expected.jobId = "job-1";
    expected.type = JobType::kThumbnail;
    expected.sourceFileHash = "source";
    expected.profile = "thumb-512-jpeg-v1";
    expected.state = JobState::kPending;
    expected.attempts = 2;
    expected.leaseUntil = 123;
    expected.nextRetryAt = 456;

    MediaJob actual;
    MINIKV_CHECK(parseMediaJob(serializeMediaJob(expected), actual));
    MINIKV_CHECK(expected.jobId == actual.jobId);
    MINIKV_CHECK(expected.nextRetryAt == actual.nextRetryAt);
    return 0;
}
```

Include `TestCheck.hpp`; this matches the repository's existing standalone CTest executables.

- [ ] **Step 2: Configure and run the test to verify RED**

Run: `cmake -S . -B build && cmake --build build -j$(nproc) --target test_media_job && ./build/bin/test_media_job`

Expected: compilation fails because `media/MediaJob.hpp` does not exist.

- [ ] **Step 3: Implement the value types and length-delimited serialization**

```cpp
enum class JobType { kThumbnail };
enum class JobState { kPending, kRunning, kReady, kFailed, kUnsupported };

struct MediaJob {
    std::string jobId;
    JobType type = JobType::kThumbnail;
    std::string sourceFileHash;
    std::string profile;
    JobState state = JobState::kPending;
    uint32_t attempts = 0;
    int64_t leaseUntil = 0;
    int64_t nextRetryAt = 0;
    std::string lastError;
    int64_t createdAt = 0;
    int64_t updatedAt = 0;
};
```

Use the existing V2 length-prefixed/escaped metadata convention rather than ad-hoc JSON parsing. Reject unknown enum values and truncated input.

- [ ] **Step 4: Run the unit test to verify GREEN**

Run: `cmake --build build -j$(nproc) --target test_media_job && ./build/bin/test_media_job`

Expected: exit status 0.

- [ ] **Step 5: Commit the isolated value-type change**

```bash
git add include/media/MediaJob.hpp src/media/MediaJob.cpp test/test_media_job.cpp CMakeLists.txt
git commit -m "feat: add media job value types"
```

### Task 2: Add Gateway job ledger and idempotent thumbnail enqueue

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Create: `test/test_gateway_media_jobs.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes `media::MediaJob`, `media::ThumbnailMeta` from Task 1.
- Produces `GatewayState::enqueueThumbnail`, `claimMediaJob`, `completeMediaJob`, `failMediaJob`, `expiredMediaJobs`.
- Returns `ThumbnailEnqueueResult { MediaJob job; bool publishRequired; }`.

- [ ] **Step 1: Write failing Gateway tests**

```cpp
int main() {
    GatewayState state(tempDbPath);
    const auto first = state.enqueueThumbnail("file-a", "thumb-512-jpeg-v1", now);
    const auto second = state.enqueueThumbnail("file-a", "thumb-512-jpeg-v1", now + 1);
    MINIKV_CHECK(first.publishRequired);
    MINIKV_CHECK(!second.publishRequired);
    MINIKV_CHECK(first.job.jobId == second.job.jobId);

    media::MediaJob claimed;
    MINIKV_CHECK(state.claimMediaJob(first.job.jobId, now, 10, claimed));
    MINIKV_CHECK(!state.claimMediaJob(first.job.jobId, now + 1, 10, claimed));
    MINIKV_CHECK(state.claimMediaJob(first.job.jobId, now + 11, 10, claimed));
    MINIKV_CHECK(claimed.attempts == 2);
    return 0;
}
```

Include `TestCheck.hpp`; create a temporary LevelDB directory exactly as in
`test/test_gateway_object_cache.cpp`.

- [ ] **Step 2: Run the tests to verify RED**

Run: `cmake --build build -j$(nproc) --target test_gateway_media_jobs && ./build/bin/test_gateway_media_jobs`

Expected: compilation fails because the job ledger API is missing.

- [ ] **Step 3: Implement atomic job transitions**

Add `j:` and `d:` key helpers in `GategayState.cpp`. Use one LevelDB `WriteBatch` for each state transition:

```cpp
ThumbnailEnqueueResult GatewayState::enqueueThumbnail(
    const std::string& fileHash, const std::string& profile, int64_t now);

bool GatewayState::claimMediaJob(const std::string& jobId, int64_t now,
                                 int64_t leaseSeconds, media::MediaJob& out);
bool GatewayState::completeMediaJob(const std::string& jobId,
                                    const std::string& derivedObjectId,
                                    const std::string& derivedFileHash,
                                    int64_t now);
bool GatewayState::failMediaJob(const std::string& jobId, bool unsupported,
                                const std::string& error, int64_t nextRetryAt,
                                int64_t now);
```

`enqueueThumbnail` must return the existing task without publish when a `READY`, `PENDING`, or unexpired `RUNNING` record exists. `completeMediaJob` must validate that the claimed job is thumbnail type and update both `j:` and `d:` records atomically.

- [ ] **Step 4: Run Gateway media-job tests to verify GREEN**

Run: `cmake --build build -j$(nproc) --target test_gateway_media_jobs && ./build/bin/test_gateway_media_jobs`

Expected: exit status 0; repeated enqueue returns the same job ID.

- [ ] **Step 5: Commit the ledger change**

```bash
git add include/gateway/GatewayState.hpp src/gateway/GategayState.cpp test/test_gateway_media_jobs.cpp CMakeLists.txt
git commit -m "feat: persist Gateway media jobs"
```

### Task 3: Add a bounded Redis Streams publisher

**Files:**
- Create: `include/media/RedisTaskPublisher.hpp`
- Create: `src/media/RedisTaskPublisher.cpp`
- Create: `test/test_redis_task_publisher.cpp`
- Modify: `CMakeLists.txt`
- Modify: `docs/V2_DEPENDENCIES.md`

**Interfaces:**
- Consumes `media::MediaJob` from Task 1.
- Produces `RedisTaskPublisher::start`, `enqueue`, `stop`, `pendingCount`.
- Uses `RedisPublisherConfig { host, port, stream, maxQueueBytes }`.

- [ ] **Step 1: Write a failing bounded-queue test**

```cpp
int main() {
    RedisTaskPublisher publisher({"127.0.0.1", 6379, "media:thumbnail", 1});
    MINIKV_CHECK(publisher.enqueue(job("one")));
    MINIKV_CHECK(!publisher.enqueue(job("two")));
    return 0;
}
```

The test includes `TestCheck.hpp` and does not call `start()`, so it needs no live Redis server.

- [ ] **Step 2: Run the test to verify RED**

Run: `cmake --build build -j$(nproc) --target test_redis_task_publisher && ./build/bin/test_redis_task_publisher`

Expected: compilation fails because the publisher is missing.

- [ ] **Step 3: Implement publisher thread with hiredis**

Use `hiredis` synchronous calls exclusively on the publisher thread. The thread reconnects with bounded backoff and sends:

```text
XADD media:thumbnail MAXLEN ~ 100000 * jobId <id> type thumbnail profile thumb-512-jpeg-v1
```

Gateway EventLoop threads call only `enqueue(job)`, which copies the small job identifier into a bounded condition-variable queue. If the queue is full, leave the durable job in `PENDING`; the periodic reconciliation scan will enqueue it later. Never discard the LevelDB record.

- [ ] **Step 4: Document and configure the dependency**

Add `find_path`/`find_library` for hiredis to CMake and document Ubuntu setup:

```bash
sudo apt install redis-server libhiredis-dev
```

The build must fail with a direct message when hiredis headers or library are absent.

- [ ] **Step 5: Run publisher test and CMake configure to verify GREEN**

Run: `cmake -S . -B build && cmake --build build -j$(nproc) --target test_redis_task_publisher && ./build/bin/test_redis_task_publisher`

Expected: exit status 0. The unit test does not require a running Redis server.

- [ ] **Step 6: Commit the publisher**

```bash
git add include/media/RedisTaskPublisher.hpp src/media/RedisTaskPublisher.cpp test/test_redis_task_publisher.cpp CMakeLists.txt docs/V2_DEPENDENCIES.md
git commit -m "feat: add bounded Redis task publisher"
```

### Task 4: Wire Gateway commit, retry scan, and internal control APIs

**Files:**
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Create: `test/test_gateway_media_http.sh`

**Interfaces:**
- Consumes Gateway ledger from Task 2 and `RedisTaskPublisher` from Task 3.
- Produces `POST /internal/v2/media/jobs/{jobId}/claim`, `/complete`, `/fail`.
- Produces automatic thumbnail enqueue only after a successful JPEG file commit.

- [ ] **Step 1: Write a failing HTTP contract test**

```bash
assert_status 401 POST /internal/v2/media/jobs/job-1/claim '{}'
assert_status 200 POST /internal/v2/media/jobs/job-1/claim \
  '{"leaseSeconds":60}' -H "X-Cluster-Internal-Token: $SECRET"
assert_json_field state RUNNING
```

The test starts a temporary Gateway LevelDB directory and uses a local fake Redis listener or disabled publisher configuration. It verifies state changes are durable without requiring a real Redis deployment.

- [ ] **Step 2: Run the contract test to verify RED**

Run: `bash test/test_gateway_media_http.sh build/bin/minikv_v2_gateway`

Expected: endpoint returns 404 before implementation.

- [ ] **Step 3: Add Gateway integration**

At successful file commit:

```cpp
bool isJpegFile(const ObjectMeta& object) {
    const std::string name = lowerAscii(object.name);
    return object.contentType == "image/jpeg" || endsWith(name, ".jpg") ||
           endsWith(name, ".jpeg");
}

if (isJpegFile(object)) {
    const auto job = state.enqueueThumbnail(object.fileHash, "thumb-512-jpeg-v1", nowSeconds());
    if (job.publishRequired) publisher.enqueue(job.job);
}
```

Add a timer that retrieves due `PENDING` and expired `RUNNING` jobs in a fixed batch (100), then offers them to the publisher queue. It must not iterate the entire database in one EventLoop turn.

Claim/complete/fail endpoints must verify `X-Cluster-Internal-Token`; only result endpoints update the job ledger. Add structured async logs for enqueue, claim, requeue, completion and failure.

- [ ] **Step 4: Run contract and regression tests to verify GREEN**

Run:

```bash
bash test/test_gateway_media_http.sh build/bin/minikv_v2_gateway
ctest --test-dir build -R '^(gateway_media_jobs|gateway_upload_preflight|gateway_catalog|gateway_delete)$' --output-on-failure
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit Gateway integration**

```bash
git add src/gateway/gateway_main.cpp include/gateway/GatewayState.hpp src/gateway/GategayState.cpp test/test_gateway_media_http.sh
git commit -m "feat: enqueue and control media jobs"
```

### Task 5: Extend NodeAgent configuration for Redis and capabilities

**Files:**
- Modify: `include/agent/NodeAgentConfig.hpp`
- Modify: `src/agent/NodeAgentConfig.cpp`
- Modify: `src/agent/node_agent_main.cpp`
- Modify: `test/test_node_agent_logging_config.cpp`
- Create: `docs/V2_REDIS_TASK_DEPLOYMENT.md`

**Interfaces:**
- Produces `RedisConfig { address, port, thumbnailStream, streamMaxLen }`.
- Produces `NodeAgentConfig::capabilities`.
- Gateway child receives `MINIKV_V2_REDIS_*`; DataNode behavior remains unchanged.

- [ ] **Step 1: Write failing YAML parser tests**

```cpp
int main() {
    const auto config = parseNodeAgentConfigText(R"yaml(
node:
  nodeId: node-c
  advertiseAddress: 100.1.1.1
  capabilities: [storage, thumbnail]
cluster:
  secretFile: /tmp/secret
  redisAddress: 127.0.0.1
  redisPort: 6379
services:
  - id: gateway-0
    type: gateway
    enabled: true
    listenPort: 18081
    dataDir: /tmp/gateway
)yaml");
    MINIKV_CHECK(config.redis.address == "127.0.0.1");
    MINIKV_CHECK(config.redis.port == 6379);
    MINIKV_CHECK(config.capabilities[1] == "thumbnail");
    return 0;
}
```

Extend the existing `test/test_node_agent_logging_config.cpp` and retain its `TestCheck.hpp` style.

- [ ] **Step 2: Run the parser test to verify RED**

Run: `cmake --build build -j$(nproc) --target test_node_agent_logging_config && ./build/bin/test_node_agent_logging_config`

Expected: compilation fails because Redis/capability fields are absent.

- [ ] **Step 3: Implement parser and child environment propagation**

Require Redis configuration only when a Gateway service is enabled. Validate nonempty capability names and port range. Pass Redis settings and node capabilities only to Gateway. Extend `ServiceType` only in D3 when the thumbnail worker executable actually exists.

- [ ] **Step 4: Add deployment document and sample YAML**

Document Node C Redis startup, `bind 127.0.0.1 <tailscale-ip>` policy, protected-mode/ACL guidance, `MAXLEN ~`, service restart behavior, and logs. Do not commit private cluster secrets or production IPs.

- [ ] **Step 5: Run parser test and full build to verify GREEN**

Run:

```bash
cmake --build build -j$(nproc)
./build/bin/test_node_agent_logging_config
```

Expected: exit status 0.

- [ ] **Step 6: Commit agent configuration support**

```bash
git add include/agent/NodeAgentConfig.hpp src/agent/NodeAgentConfig.cpp src/agent/node_agent_main.cpp test/test_node_agent_logging_config.cpp docs/V2_REDIS_TASK_DEPLOYMENT.md
git commit -m "feat: configure Redis task dispatch"
```

### Task 6: End-to-end D2 validation and operations documentation

**Files:**
- Create: `test/test_v2_media_task_redis.sh`
- Create: `docs/V2_D2_MEDIA_TASK_OPERATIONS.md`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes the Gateway binary, real local Redis, and D2 control APIs.
- Produces a reproducible validation command for operations.

- [ ] **Step 1: Write the failing end-to-end script**

The script must start an isolated local `redis-server` with a temporary config, start Gateway against temporary metadata, create a JPEG-backed committed object fixture, and assert:

```text
one j:{jobId} PENDING record
one XREADGROUP-visible media:thumbnail message
claim transitions PENDING -> RUNNING
expired lease is re-published by reconciliation
```

- [ ] **Step 2: Run it before completion to verify the intended assertion initially fails where expected**

Run: `bash test/test_v2_media_task_redis.sh build/bin/minikv_v2_gateway`

Expected before final wiring: a missing Redis message or unavailable endpoint assertion.

- [ ] **Step 3: Complete minimal gaps revealed by the end-to-end test**

Fix only behavior required for durable enqueue, publish, claim and requeue. Do not add JPEG decoding, derived upload, catalog thumbnail fields, or a Worker executable.

- [ ] **Step 4: Run D2 verification**

Run:

```bash
bash test/test_v2_media_task_redis.sh build/bin/minikv_v2_gateway
ctest --test-dir build -R '^(media_job|gateway_media_jobs|node_agent_logging_config|gateway_catalog|gateway_delete)$' --output-on-failure
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit D2 validation documentation**

```bash
git add test/test_v2_media_task_redis.sh docs/V2_D2_MEDIA_TASK_OPERATIONS.md CMakeLists.txt
git commit -m "test: validate D2 media task dispatch"
```

## Plan Self-Review

- Spec coverage: task ledger is Task 2; Redis Streams and bounded publisher are Task 3; claim/result
  control and recovery scan are Task 4; configuration and deployment are Task 5; real Redis recovery
  validation is Task 6.
- Scope: JPEG decoding, thumbnail object upload, catalog thumbnail fields and worker process are deliberately
  deferred to D3.
- Type consistency: all later tasks use `MediaJob`, `ThumbnailMeta`, `JobState`, `JobType`, `jobId`,
  profile `thumb-512-jpeg-v1`, and `RedisTaskPublisher` defined in Tasks 1-3.
- Placeholder scan: no deferred implementation steps are part of D2; explicit D3 exclusions are scope limits.
