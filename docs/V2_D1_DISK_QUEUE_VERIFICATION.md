# V2 D1 Bounded Disk Queue Verification

## Scope

D1 moves the DataNode local write path off its EventLoop without changing the
Gateway API, upload token, HTTP body format, replica protocol, or LevelDB
physical-index format.

The DataNode creates one `DiskWriteExecutor` at startup with these fixed V2
defaults:

```text
disk workers: 2
block size:   64 KiB
block count:  128
pool size:    8 MiB
chunk queue high watermark: 1 MiB
chunk queue low watermark:  512 KiB
local write admission:      2 active Chunk uploads
```

The values are process-local, not Gateway placement settings. They are kept
fixed in D1 so that the initial behaviour remains testable. YAML tuning and
adaptive scheduling belong to a later performance phase.

## Data Path

```text
TcpConnection / HTTP input Buffer (EventLoop)
  -> copy at most 64 KiB into a borrowed fixed BlockLease
  -> ChunkDiskWritePipeline FIFO
  -> DiskWriteExecutor worker
       -> SHA256_Update
       -> pwrite(disk0.data)
  -> EventLoop completion callback
  -> after all blocks: worker executes SHA finalization + LevelDB index commit
  -> local completion and replica completion join
  -> primary commits Chunk metadata to Gateway
```

There is deliberately one copy from the HTTP input buffer into a fixed block.
`HttpContext` reclaims its input bytes after the body callback returns, so it
cannot safely lend that storage to a Worker. Removing this copy requires a
future network/HTTP ownership redesign, not a local `pwrite` optimization.

## Backpressure Rules

- Each Chunk pipeline schedules at most one `WriteSession::append()` at a
  time. SHA state and the append extent therefore remain serial.
- When queued local bytes reach 1 MiB, its body callback returns `kPause`.
  `HttpServer` pauses the upstream connection's `EPOLLIN`.
- Once the queue drains to 512 KiB, the Worker completion queues a callback to
  the owning EventLoop. It resumes the upstream only through a weak
  `TcpConnection` reference.
- The existing replica pipe can independently request a pause. The upload
  body pauses when either local disk or replica forwarding is congested.
- The current HTTP parser has no `pause without consume` outcome. If the fixed
  pool is unexpectedly exhausted, D1 aborts that Chunk instead of pretending
  that bytes were accepted. With 2 active uploads, 1 MiB per-Chunk queue, and
  an 8 MiB pool this indicates a configuration or lifecycle defect.

## Correctness Rules

- Worker callbacks retain the separate disk pipeline while calling
  `WriteSession::append()`.
- Callbacks back to `EventLoop` lock a `weak_ptr`; neither they nor the disk
  worker capture a raw `ChunkUploadStream*`.
- `WriteSession::finish()` runs as a Worker task after the final append.
- Gateway commit waits for both local finish and replica response. A fast
  replica response can no longer commit a Chunk before local SHA/index finish.
- During shutdown, `EventLoop` is constructed before the executor. Destruction
  therefore stops and joins disk Workers before the EventLoop is destroyed.

## Build And Focused Tests

```bash
cmake -S . -B build
cmake --build build -j2 --target \
  minikv_v2_datanode \
  test_http_stream_context \
  test_replica_upload_metrics \
  test_disk_write_executor \
  test_chunk_disk_write_pipeline

ctest --test-dir build -R \
  '^(http_stream_context|replica_upload_metrics|disk_write_executor|chunk_disk_write_pipeline|fast_data_store_read)$' \
  --output-on-failure
```

`test_disk_write_executor` verifies preallocated block reuse and the
predicate-based worker queue. `test_chunk_disk_write_pipeline` verifies serial
append/finalization and that a saturated local queue returns `kPause`.

## Runtime Observation

After deploying the rebuilt DataNode, inspect completed Chunk records:

```bash
tail -F ~/minikv-v2/logs/datanode.log | grep 'event=chunk_'
```

Relevant fields:

```text
sha_update_us / pwrite_us / sha_finalize_us / index_us
disk_queue_peak_bytes / disk_pause_count / disk_pause_ms
pauses / pause_ms / max_pending_bytes
replica_ms / gateway_commit_ms
```

Interpretation:

- High `pwrite_us` plus non-zero `disk_pause_*`: local disk or its worker
  queue is controlling throughput.
- Low disk pause values but high replica `pause_ms`: downstream replica/network
  is controlling throughput.
- Low both values but long total Chunk time: investigate client-to-primary
  network bandwidth and the browser's sequential Chunk window.

## D1 Limits

- Two disk workers are bounded but not dynamically tuned.
- The fixed block pool is not a zero-copy receive buffer.
- There is still one EventLoop per DataNode process.
- Download uses the existing `sendfile` route; this change only moves upload
  disk writes.
- No Redis, WAL recovery, Repair, GC, or media worker task is included here.
