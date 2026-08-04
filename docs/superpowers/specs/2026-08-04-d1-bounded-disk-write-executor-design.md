# D1: DataNode Bounded Disk Write Executor

## Status

Approved design for implementation. This change isolates DataNode local SHA-256
and `pwrite` work from its EventLoop. It does not add Redis, media workers,
thumbnail generation, WAL, `fdatasync`, or multi-Reactor networking.

## Problem

`ChunkUploadStream::consume()` currently runs on the DataNode EventLoop thread.
For every HTTP body block it calls `FastDataStore::WriteSession::append()`, which
performs both `EVP_DigestUpdate()` and `pwrite()`. Slow local storage therefore
blocks the EventLoop from serving other sockets, heartbeats, replica events, and
downloads. A future media task must not share this user-upload execution path.

The HTTP input buffer is reclaimed after a body callback returns. A disk worker
cannot keep the callback's raw pointer, so handing bytes to another thread
requires an owned bounded block.

## Scope

### Included

- A fixed-size `DiskWriteExecutor` owned by each DataNode process.
- A fixed 64 KiB block pool and a per-upload FIFO of pool-owned blocks,
  preserving byte order without allocating one heap buffer per body callback.
- A bounded global queued-byte budget and a bounded per-upload budget.
- Disk-backpressure integration with existing `HttpContext::kPause` and resume.
- Worker execution of SHA-256 update, `pwrite`, SHA finalization, and physical
  index commit for a `FastDataStore::WriteSession`.
- Completion callbacks returned to the owning EventLoop through `queueInLoop`.
- Metrics for queued bytes and pauses in the existing structured chunk log.

### Excluded

- Redis, task queues for media, thumbnail/preview generation, and Worker
  processes.
- Changes to Gateway metadata or its upload protocol.
- Connection pooling, multi-EventLoop, `io_uring`, and `sendfile` changes.
- Crash-safe WAL, `fdatasync`, recovery, GC, or repair.

## Execution Model

```text
HTTP EventLoop receives <= 64 KiB body block
  -> borrow a 64 KiB block from DiskWriteExecutor's fixed block pool
  -> copy bytes into ChunkWritePipeline FIFO
  -> schedule serial DiskWriteExecutor work for this upload
  -> concurrently pass the original block to ReplicaUploadPipe

Disk worker
  -> WriteSession::append(owned block)  [SHA-256 update + pwrite]
  -> queue completion to owning EventLoop

EventLoop completion
  -> account queued bytes
  -> resume input if low watermarks permit
  -> when input is complete and FIFO is empty, schedule WriteSession::finish()
  -> after local finish and replica completion, commit Gateway and reply
```

Only one worker task for an individual `WriteSession` may run at a time. This
preserves file offset order and use of its OpenSSL digest context. Different
Chunk uploads may run on different workers.

Disk work owns a separate `DiskWritePipelineState` while an operation is
outstanding. Its completion callback captures `std::weak_ptr<ChunkUploadStream>`
and first locks it after returning to the EventLoop. It must not dereference a
raw `this` pointer. The weak reference prevents a completion notification from
accessing an HTTP stream that was cancelled and released. The pipeline state,
not the HTTP stream, keeps the `WriteSession` and borrowed block alive until the
worker has returned the block to the pool. A temporary strong stream hold is
still permitted only while a deferred client response must remain semantically
pending; it is not used as an accidental lifetime guarantee for worker lambdas.

The replica path remains EventLoop-driven. The current body block is copied for
the disk FIFO before the callback returns, while `ReplicaUploadPipe` continues
to own its existing output buffering. A client receives success only after local
finish, replica outcome, and Gateway commit keep their current semantics.

## Flow Control

Initial configuration defaults:

```text
diskWorkers                    2
globalQueuedWriteBytes         8 MiB
globalResumeQueuedWriteBytes   4 MiB
perUploadQueuedWriteBytes      1 MiB
perUploadResumeBytes           512 KiB
diskBlockBytes                 64 KiB
diskBlockCount                 128
```

The body callback returns `kPause` only after accepting and copying the current
owned block when its per-upload high watermark is reached. `HttpContext` then
retains later unconsumed bytes and the connection disables `EPOLLIN`. Worker
completion queues an EventLoop callback. When the per-upload queue is below its
low watermark, that callback resumes the HTTP body parser and `EPOLLIN`.

The queue is deliberately byte-bounded rather than task-count-bounded because
the memory risk comes from body bytes, not task metadata. D1 keeps the existing
`maxConcurrentWrites = 2` and limits each active upload to 1 MiB. Therefore at
most roughly 2 MiB of blocks are leased, well below the 8 MiB pool. A failed
pool acquisition is treated as an internal resource invariant violation and
returns an explicit upload error rather than returning `kPause` after failing to
own the current input bytes. General global-pool waiter registration, which
would require a new HTTP result meaning “pause without consume”, is deferred.

`HttpContext` already limits one streaming body callback to 64 KiB. The executor
therefore allocates its 128 reusable 64 KiB blocks at startup, matching the
8 MiB global byte budget. A per-block `std::vector<char>` would still allocate
on the heap for each callback, so it is not treated as an allocation solution.
The implementation uses the condition-variable predicate form:

```cpp
cv_.wait(lock, [this] { return stopping_ || !readyQueue_.empty(); });
```

This prevents a spurious wakeup from running an empty queue iteration.

## Lifecycle and Errors

- `beginPut()` still reserves the extent before body processing.
- `WriteSession` is owned by the pipeline until the final worker operation has
  completed. It is never concurrently used or destroyed from an EventLoop while
  a disk task references it.
- A failed `append()` aborts that upload, clears pending blocks, and schedules
  the existing deferred HTTP error response on the EventLoop.
- HTTP client disconnects mark the pipeline cancelled. Queued work stops before
  new blocks are run; an in-progress `pwrite` is allowed to return before the
  session is released. V2 may leave an unreachable extent after this failure,
  consistent with its existing non-WAL storage model.
- `finish()` runs only after the input body finished and all accepted blocks were
  processed. It performs SHA finalization and index visibility exactly once.

## Expected Effects and Limits

This design improves EventLoop responsiveness and bounded-memory behavior under
slow disks or concurrent uploads. It does not materially increase a single
browser upload whose limiting factor is WAN/Tailscale bandwidth; the existing
measurements show most time in `body_receive_ms`, not `pwrite_us`.

There is an intentional extra copy per accepted body block and worker scheduling
overhead. That cost is accepted to make byte ownership and backpressure correct.
Shared immutable buffers and `writev`/zero-copy optimizations are V4 work.

## Validation

1. Unit test serial ordering: blocks for one upload reach `WriteSession` in
   byte order and finalization happens after the final block.
2. Unit test queue bounds: a full per-upload/global queue produces pause and
   resumes only after low watermarks.
3. Unit test cancellation: no queued operation dereferences a released upload.
4. Integration test: two concurrent Chunk PUTs with a deliberately delayed
   write path still allow heartbeat/admin requests to complete.
5. Structured logs expose `disk_queue_peak_bytes`, `disk_pause_count`, and
   `disk_pause_ms`; compare them with existing `replica` metrics.
6. Lifetime test: cancel an upload with queued work, then run worker completion;
   it must not access the released `ChunkUploadStream` and must return its block
   to the pool.
