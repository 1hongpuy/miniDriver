# D1 Bounded Disk Write Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move DataNode SHA-256 and local `pwrite` work off its EventLoop while keeping memory bounded and preserving Chunk upload, replica, and Gateway commit semantics.

**Architecture:** `DiskWriteExecutor` owns a fixed 128 x 64 KiB block pool and two disk workers. `ChunkDiskWritePipeline` owns one Chunk's FIFO and schedules at most one worker operation for its `WriteSession`. Completion returns through `EventLoop::queueInLoop` using weak ownership. `ChunkUploadStream` retains HTTP, replica, and Gateway control flow.

**Tech Stack:** C++17, pthread, existing `EventLoop`, `HttpContext::BodyConsumeResult`, `FastDataStore::WriteSession`, OpenSSL SHA-256, CTest.

## Global Constraints

- Keep the current `inputBuffer -> owned BlockPool block` copy.
- Body callbacks remain at most 64 KiB.
- Use two workers and 128 fixed 64 KiB blocks, giving an 8 MiB global payload bound.
- One `WriteSession` is accessed by at most one worker at a time.
- Cross-thread callbacks use weak references; no raw `this` crosses threads.
- Do not add Redis, media tasks, thumbnails, WAL, `fdatasync`, multi-Reactor, or `io_uring`.
- Keep the current success requirement: local finish, replica outcome, and Gateway commit.

## File Structure

- Create `include/DataNode/DiskWriteExecutor.hpp`: fixed BlockPool, move-only lease, worker queue, and executor lifecycle.
- Create `src/DataNode/DiskWriteExecutor.cpp`: pool allocation, predicate-based worker loop, and shutdown.
- Create `include/DataNode/ChunkDiskWritePipeline.hpp`: per-Chunk FIFO, serial scheduling, cancellation, input completion, and metrics.
- Create `src/DataNode/ChunkDiskWritePipeline.cpp`: `WriteSession::append()` and `finish()` worker operations.
- Modify `src/DataNode/datanode_main.cpp`: delegate local writes to the pipeline, merge disk/replica backpressure, and log disk metrics.
- Modify `CMakeLists.txt`: build and register isolated executor and pipeline tests.
- Create `test/test_disk_write_executor.cpp`: pool reuse, worker execution, stop behavior.
- Create `test/test_chunk_disk_write_pipeline.cpp`: byte order, finalization after drain, pause/resume, cancellation safety.

## Public Interfaces

```cpp
class DiskWriteExecutor {
public:
    static constexpr size_t kBlockBytes = 64 * 1024;
    struct Config { size_t workerCount = 2; size_t blockCount = 128; };
    struct Metrics { uint64_t leasedBytes; uint64_t peakLeasedBytes; uint64_t queuedTasks; };

    class BlockLease {
    public:
        BlockLease(const BlockLease&) = delete;
        BlockLease& operator=(const BlockLease&) = delete;
        BlockLease(BlockLease&&) noexcept;
        BlockLease& operator=(BlockLease&&) noexcept;
        ~BlockLease();
        char* data();
        explicit operator bool() const;
    };

    using Work = std::function<void(BlockLease)>;
    explicit DiskWriteExecutor(Config config = {});
    ~DiskWriteExecutor();
    std::optional<BlockLease> tryAcquireBlock();
    bool submit(BlockLease block, Work work);
    Metrics metrics() const;
    void stop();
};

class ChunkDiskWritePipeline : public std::enable_shared_from_this<ChunkDiskWritePipeline> {
public:
    struct Metrics { uint64_t queuedBytes; uint64_t peakQueuedBytes; uint64_t pauseCount; uint64_t pauseNanoseconds; };
    using Ptr = std::shared_ptr<ChunkDiskWritePipeline>;
    using ReadyCallback = std::function<void()>;
    using FinishCallback = std::function<void(bool ok, bool alreadyExists)>;

    static Ptr create(network::EventLoop*, DiskWriteExecutor&,
                      std::shared_ptr<FastDataStore::WriteSession>, ReadyCallback);
    http::HttpContext::BodyConsumeResult push(const char* bytes, size_t size);
    void finishInput(FinishCallback callback);
    void cancel();
    Metrics metrics() const;
};
```

`push()` runs only on the owning EventLoop. It returns `kPause` only after accepting the current block and reaching 1 MiB per-upload queued payload. With the existing two-write admission limit, two 1 MiB queues cannot exhaust the fixed 8 MiB pool. A pool-acquisition failure is an explicit upload error in D1, not an unconsumed pause result; the existing `HttpContext::kPause` means the current body bytes were consumed.

### Task 1: Fixed BlockPool and Disk Worker Executor

**Files:**
- Create: `include/DataNode/DiskWriteExecutor.hpp`
- Create: `src/DataNode/DiskWriteExecutor.cpp`
- Create: `test/test_disk_write_executor.cpp`
- Modify: `CMakeLists.txt`

**Consumes:** C++17 standard threading primitives.

**Produces:** `tryAcquireBlock()`, `submit()`, move-only `BlockLease`, and `stop()`.

- [ ] **Step 1: Write the failing pool/worker test**

```cpp
DiskWriteExecutor::Config config;
config.workerCount = 1;
config.blockCount = 2;
DiskWriteExecutor executor(config);
auto first = executor.tryAcquireBlock();
auto second = executor.tryAcquireBlock();
CHECK(first.has_value());
CHECK(second.has_value());
CHECK(!executor.tryAcquireBlock().has_value());

std::promise<int> ran;
CHECK(executor.submit(std::move(*first), [&ran](DiskWriteExecutor::BlockLease block) {
    block.data()[0] = 'x';
    ran.set_value(1);
}));
CHECK(ran.get_future().wait_for(std::chrono::seconds(1)) == std::future_status::ready);
CHECK(executor.tryAcquireBlock().has_value());
```

- [ ] **Step 2: Register the test and prove it fails**

```cmake
add_executable(test_disk_write_executor test/test_disk_write_executor.cpp)
target_link_libraries(test_disk_write_executor PRIVATE minikv_datanode pthread)
add_test(NAME disk_write_executor COMMAND test_disk_write_executor)
```

Run: `cmake -S . -B build && cmake --build build -j2 --target test_disk_write_executor`

Expected: build failure because the executor header does not exist.

- [ ] **Step 3: Implement the fixed pool and worker queue**

Allocate all blocks once in the constructor:

```cpp
blocks_ = std::make_unique<Block[]>(config_.blockCount);
for (uint16_t i = 0; i < config_.blockCount; ++i) freeIds_.push_back(i);
```

Use a `std::vector<uint16_t>` as a LIFO free-id stack. `BlockLease::reset()` returns its id exactly once. A queued work item owns both the move-only lease and work callback. Worker waiting must use:

```cpp
cv_.wait(lock, [this] { return stopping_ || !readyQueue_.empty(); });
if (stopping_ && readyQueue_.empty()) return;
```

Catch exceptions at the worker boundary so a failed callback cannot terminate a worker thread. The lease destructor returns the block even when work returns early.

- [ ] **Step 4: Run the focused test**

Run: `cmake --build build -j2 --target test_disk_write_executor && ctest --test-dir build -R '^disk_write_executor$' --output-on-failure`

Expected: `100% tests passed`.

- [ ] **Step 5: Commit the executor unit**

```bash
git add CMakeLists.txt include/DataNode/DiskWriteExecutor.hpp src/DataNode/DiskWriteExecutor.cpp test/test_disk_write_executor.cpp
git commit -m "feat: add bounded DataNode disk write executor"
```

### Task 2: Per-Chunk Serial Disk Pipeline

**Files:**
- Create: `include/DataNode/ChunkDiskWritePipeline.hpp`
- Create: `src/DataNode/ChunkDiskWritePipeline.cpp`
- Create: `test/test_chunk_disk_write_pipeline.cpp`
- Modify: `CMakeLists.txt`

**Consumes:** `DiskWriteExecutor`, `FastDataStore::WriteSession`, and `network::EventLoop`.

**Produces:** ordered `push()`, `finishInput()`, `cancel()`, and queue metrics.

- [ ] **Step 1: Write failing pipeline tests**

Create a temporary `FastDataStore`, construct a 128 KiB expected payload, push two 64 KiB blocks, and call `finishInput()`. Assert the finish callback reports success only after both blocks are processed and `store.get(hash, bytes)` equals the concatenated input. Add a gated single-worker case that fills 16 blocks and asserts the sixteenth `push()` returns `HttpContext::BodyConsumeResult::kPause` after accepting the block that reaches the 1 MiB watermark.

- [ ] **Step 2: Prove the test fails before pipeline implementation**

Run: `cmake --build build -j2 --target test_chunk_disk_write_pipeline`

Expected: build failure because the pipeline header does not exist.

- [ ] **Step 3: Implement ordered scheduling**

Keep this state only on the EventLoop:

```cpp
std::deque<PendingBlock> pending_;
bool writeInFlight_ = false;
bool inputFinished_ = false;
bool cancelled_ = false;
size_t queuedBytes_ = 0;
FinishCallback finishCallback_;
```

`push()` borrows a lease, copies bytes, appends a `PendingBlock`, records queue metrics, and invokes `scheduleNextInLoop()`. That method schedules a block only when `writeInFlight_` is false. The worker owns a separate pipeline state needed for `WriteSession`; the EventLoop completion captures a weak pipeline pointer, subtracts logical bytes, clears `writeInFlight_`, invokes `onReady` once below 512 KiB, then schedules the next block.

Only when `inputFinished_`, `pending_` is empty, and no block is in flight may the pipeline submit `writer_->finish()` as a worker operation. Its completion invokes `finishCallback_(ok, alreadyExists)` exactly once. `cancel()` rejects future pushes and causes later worker completion to release resources without touching a raw stream pointer.

- [ ] **Step 4: Run pipeline and storage regressions**

Run: `cmake --build build -j2 --target test_chunk_disk_write_pipeline test_fast_data_store_read && ctest --test-dir build -R '^(chunk_disk_write_pipeline|fast_data_store_read)$' --output-on-failure`

Expected: both tests pass.

- [ ] **Step 5: Commit the pipeline unit**

```bash
git add CMakeLists.txt include/DataNode/ChunkDiskWritePipeline.hpp src/DataNode/ChunkDiskWritePipeline.cpp test/test_chunk_disk_write_pipeline.cpp
git commit -m "feat: add serial Chunk disk write pipeline"
```

### Task 3: DataNode Integration and Disk Backpressure

**Files:**
- Modify: `src/DataNode/datanode_main.cpp`
- Modify: `test/test_replica_upload_metrics.cpp`

**Consumes:** executor, pipeline, existing HTTP body pause/resume, replica pipe.

**Produces:** DataNode chunks write through workers and expose disk queue metrics.

- [ ] **Step 1: Add a failing log-field assertion**

Extend the existing success-line test so it requires:

```text
disk_queue_peak_bytes=
disk_pause_count=
disk_pause_ms=
```

Run: `cmake --build build -j2 --target test_replica_upload_metrics && ctest --test-dir build -R '^replica_upload_metrics$' --output-on-failure`

Expected: failure because current chunk logs omit disk fields.

- [ ] **Step 2: Construct one executor per DataNode process**

At `main()` startup create:

```cpp
DiskWriteExecutor::Config diskConfig;
diskConfig.workerCount = 2;
diskConfig.blockCount = 128;
DiskWriteExecutor diskExecutor(diskConfig);
```

Pass it into every `ChunkUploadStream`. Convert the `unique_ptr` from `beginPut()` into `std::shared_ptr<FastDataStore::WriteSession>` before constructing `ChunkDiskWritePipeline`.

- [ ] **Step 3: Replace synchronous writes and merge backpressure**

Replace `writer_->append(bytes, size)` in `consume()` with `diskPipeline_->push(bytes, size)`. Continue calling `ReplicaUploadPipe::push(bytes, size)` for the same body block. Return `kPause` when either subsystem returns `kPause`; return `kAbort` when either returns `kAbort`.

Store a weak upstream `TcpConnection` and give the pipeline an `onReady` callback that calls `resumeRead()` only after weak locking succeeds. When HTTP body reception ends, defer the response and call `diskPipeline_->finishInput(...)`; only successful pipeline completion sets `localFinishedAt_`, appends the local node to `successfulNodes_`, and proceeds to current replica/Gateway logic.

- [ ] **Step 4: Add disk metrics and run focused regression tests**

Append the three disk metrics to `event=chunk_complete` and `event=chunk_failed` without removing current SHA, pwrite, replica, or Gateway metrics.

Run: `cmake --build build -j2 --target minikv_v2_datanode test_replica_upload_metrics test_http_stream_context && ctest --test-dir build -R '^(replica_upload_metrics|http_stream_context|disk_write_executor|chunk_disk_write_pipeline)$' --output-on-failure`

Expected: all selected tests pass.

- [ ] **Step 5: Commit the integration**

```bash
git add src/DataNode/datanode_main.cpp test/test_replica_upload_metrics.cpp
git commit -m "feat: move DataNode local writes off EventLoop"
```

### Task 4: Runtime Verification and Operations Notes

**Files:**
- Create: `docs/V2_D1_DISK_QUEUE_VERIFICATION.md`
- Modify: `reference_impl/step_GPT_v2/13_V2_COMPLETION_AND_MEDIA_PLAN.md`

**Consumes:** current NodeAgent YAML, built DataNode, structured DataNode log.

**Produces:** repeatable deployment checks and documented D1 limits.

- [ ] **Step 1: Start the configured processes**

```bash
./build/bin/minikv_v2_node_agent --config ~/minikv-v2/config/gateway.yaml --bin-dir "$PWD/build/bin"
```

Expected: the configured Gateway/DataNode children stay running.

- [ ] **Step 2: Upload one 25 MiB file and inspect chunk records**

```bash
tail -F ~/minikv-v2/logs/datanode.log | grep --line-buffered 'event=chunk_complete'
```

Expected: every completed Chunk includes `disk_queue_peak_bytes`, `disk_pause_count`, and `disk_pause_ms`. A WAN-limited upload may legitimately show zero disk pauses.

- [ ] **Step 3: Upload two files concurrently and verify control responsiveness**

```bash
curl --noproxy '*' --max-time 2 http://127.0.0.1:18081/api/v2/admin/nodes
```

Expected: HTTP 200 while uploads run. Record queue peaks and pauses; do not claim WAN throughput improvement unless `body_receive_ms` changes.

- [ ] **Step 4: Commit the verification notes**

```bash
git add docs/V2_D1_DISK_QUEUE_VERIFICATION.md reference_impl/step_GPT_v2/13_V2_COMPLETION_AND_MEDIA_PLAN.md
git commit -m "docs: record D1 disk queue verification"
```

## Self-Review

- Task 1 covers fixed allocation, move-only return, bounded workers, and predicate waits.
- Task 2 covers ordering, weak completion ownership, finish-after-drain, pause/resume, and cancellation.
- Task 3 covers existing HTTP and replica integration plus observable metrics.
- Task 4 records the runtime boundary: D1 isolates EventLoop work and bounds memory; it neither removes the copy nor makes disk writes crash durable.
