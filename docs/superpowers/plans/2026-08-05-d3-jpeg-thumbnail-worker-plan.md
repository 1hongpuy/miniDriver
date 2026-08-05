# D3 JPEG Thumbnail Worker Implementation Plan

> **For Codex:** Execute this plan task by task.  Each task starts with a focused failing test,
> then implements only the required production code, then runs the named verification.

**Goal:** Consume persisted thumbnail jobs from Redis and generate a private
`thumb-512-jpeg-v1` object for committed JPEG files without blocking Gateway or DataNode upload
paths.

**Architecture:** Gateway remains the durable task authority and the only metadata writer.  A
dedicated Thumbnail Worker claims jobs through the internal Gateway API, downloads source chunks
through normal DataNode HTTP reads, runs a bounded JPEG CPU task, and uploads the thumbnail
through a Gateway-created internal derived session.  The derived object is not a catalog entry;
the source `ThumbnailMeta` references it once the internal commit succeeds.

**Tech Stack:** C++17, existing EventLoop/AsyncHttpClient, hiredis, LevelDB, OpenSSL SHA-256,
stb_image/stb_image_resize2/stb_image_write, yaml-cpp, CTest.

---

### Task 1: Add Thumbnail Worker configuration and process lifecycle

**Files:**
- Modify: `include/agent/NodeAgentConfig.hpp`
- Modify: `src/agent/NodeAgentConfig.cpp`
- Modify: `src/agent/node_agent_main.cpp`
- Modify: `CMakeLists.txt`
- Test: `test/test_node_agent_thumbnail_worker_config.cpp`

1. Write a parser test for `type: thumbnail_worker`, its required `dataDir` and `tempDir`,
   `maxConcurrentJobs`, and an enabled worker requiring valid `cluster.redis` plus Gateway address.
2. Run it and confirm it fails because the service type is unknown.
3. Add `ServiceType::kThumbnailWorker`, worker fields to `ManagedServiceConfig`, YAML parsing and
   validation, plus a Node Agent child spec that launches `minikv_v2_thumbnail_worker`.
4. Forward cluster secret, Redis connection, Gateway address, worker directories and logging as
   environment variables.
5. Add an initially minimal worker executable that validates configuration and logs startup.
6. Run `ctest -R '^node_agent_thumbnail_worker_config$' --output-on-failure`.

### Task 2: Make Gateway create and commit private derived sessions

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Modify: `src/gateway/gateway_main.cpp`
- Test: `test/test_gateway_derived_upload.cpp`

1. Write a failing GatewayState test that creates a source JPEG job, claims it, creates a derived
   session using that lease, commits the only chunk, then atomically commits the derived result.
2. Assert the result has a private `ObjectMeta`, no catalog path entry, and `ThumbnailMeta` is
   `READY` with the returned object ID.
3. Add derived-session identity to `SessionState` (`derivedJobId` and `derivedProfile`) and persist
   it in the existing session encoding.
4. Add `createDerivedUpload` and `commitDerivedUpload` GatewayState methods.  Both validate the
   job lease; the commit validates all chunks then writes file/object/job/thumbnail state together
   in one LevelDB WriteBatch.
5. Add internal HTTP endpoints protected by the cluster token:
   `POST /internal/v2/media/jobs/{jobId}/derived-uploads` and
   `POST /internal/v2/media/jobs/{jobId}/derived-uploads/{sessionId}/commit`.
6. Return the standard route-planning data from the create endpoint so the worker reuses DataNode
   PUT and capability tokens unchanged.
7. Run `ctest -R '^gateway_derived_upload$' --output-on-failure`.

### Task 3: Add catalog thumbnail projection

**Files:**
- Modify: `src/gateway/gateway_main.cpp`
- Test: extend `test/test_gateway_derived_upload.cpp`

1. Add a failing catalog JSON assertion for `thumbnail.profile`, `thumbnail.state`, and only when
   ready `thumbnail.objectId`.
2. Build catalog JSON from the source object's file hash and `ThumbnailMeta`; no per-directory
   thumbnail record is written.
3. Add cache invalidation for source-file catalog snapshots on media completion/failure.
4. Run the focused Gateway tests.

### Task 4: Build safe JPEG thumbnail conversion utility

**Files:**
- Add: `include/media/JpegThumbnailGenerator.hpp`
- Add: `src/media/JpegThumbnailGenerator.cpp`
- Test: `test/test_jpeg_thumbnail_generator.cpp`

1. Write a failing test that writes a small JPEG fixture, generates a thumbnail, decodes it, and
   asserts valid JPEG output and max dimension <= 512.
2. Reject non-JPEG input, inputs above 100 MiB, dimension > 16000 and decoded image pixels above
   80 million.
3. Implement decode with `stb_image`, aspect-ratio resize with `stb_image_resize2`, output JPEG
   quality 82 with `stb_image_write`; never upscale.
4. Run `ctest -R '^jpeg_thumbnail_generator$' --output-on-failure`.

### Task 5: Implement Redis consumer and worker state machine

**Files:**
- Add: `include/media/RedisTaskConsumer.hpp`
- Add: `src/media/RedisTaskConsumer.cpp`
- Add: `include/media/ThumbnailWorker.hpp`
- Add: `src/media/ThumbnailWorker.cpp`
- Add: `src/media/thumbnail_worker_main.cpp`
- Test: `test/test_redis_task_consumer.cpp`

1. Write a focused fake/loopback Redis test for consumer-group creation, parsing a thumbnail
   message and acknowledging a completed message.
2. Implement a dedicated blocking hiredis consumer thread: `XGROUP CREATE ... MKSTREAM`,
   `XREADGROUP`, periodic `XAUTOCLAIM`, bounded handoff queue, and `XACK` only after Gateway
   accepts terminal handling.
3. Implement the worker's explicit job states: claim -> source manifest -> sequential verified
   chunk downloads -> JPEG CPU conversion -> internal derived create -> normal route/PUT/commit ->
   acknowledge.  All network state transitions use the Worker EventLoop and `AsyncHttpClient`.
4. Dispatch JPEG decoding only to one bounded CPU executor.  Worker completion callbacks use
   `weak_ptr` before returning to the EventLoop.
5. Map source suffixes: `.jpg`/`.jpeg` process; other formats call Gateway `fail` with
   `UNSUPPORTED`. Retryable transport/decode failures call Gateway `fail` with retry time.
6. Run `ctest -R '^redis_task_consumer$' --output-on-failure`.

### Task 6: Integrate frontend thumbnail rendering and final verification

**Files:**
- Modify: `www/app.js`
- Modify: `www/style.css` only if required
- Add/modify: `test/test_thumbnail_worker_jpeg_e2e.cpp` or a documented manual E2E checklist
- Modify: `docs/V2_D2_MEDIA_TASK_OPERATIONS.md`

1. Update the catalog renderer: `READY` loads the private object manifest through the existing
   download route; `PENDING/RUNNING` shows a stable placeholder; `FAILED/UNSUPPORTED` leaves the
   file icon intact.
2. Keep the source preview/download behavior unchanged.
3. Build all targets and run all relevant CTests.
4. Start local Redis, Gateway, two DataNodes and one worker; upload a JPEG; verify the source
   appears, its task transitions to `READY`, the thumbnail object is not independently cataloged,
   and the browser displays the 512px image.
5. Record deployment YAML and recovery behavior in operations documentation.

