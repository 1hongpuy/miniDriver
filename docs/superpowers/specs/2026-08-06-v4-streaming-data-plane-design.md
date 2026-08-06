# V4 Streaming Data Plane Design

## Goal

Increase MiniKV's sustained V2-compatible data-plane throughput by replacing
per-Chunk short-lived replication connections and single-loop execution with
bounded long-lived HTTP/1.1 lanes, bounded parallel Chunk scheduling, shared
body blocks, and multiple I/O EventLoops.

The first V4 target remains the existing **protocol-throughput** contract:
successful HTTP completion means both replicas accepted the Chunk and the
Gateway committed its metadata. It does not claim crash durability until the
separate durable-mode work adds explicit `fdatasync` semantics.

## Scope and Compatibility

The following external contracts remain unchanged:

- fixed 4 MiB Chunk format;
- `SessionState`, `ChunkRoute`, file manifest, `manifestHash`, and upload
  capability token fields;
- browser-to-Gateway and browser-to-DataNode HTTP endpoints;
- DataNode Chunk PUT validation and Gateway Chunk commit semantics;
- chain replication order and `PROTECTING` behavior.

V4 changes internal scheduling and transport execution only. V4 does not add
gRPC, HTTP/2, a custom binary protocol, WAL recovery, repair, GC, user ACLs,
or multi-Gateway metadata consensus.

## Current Constraints

The V2 benchmark and source establish these limits:

- a benchmark file sends its Chunks serially;
- browser routes are bounded but the end-to-end scheduling does not use
  DataNode lane availability as a first-class signal;
- DataNode-to-Replica requests use a short HTTP connection per Chunk;
- each 64 KiB body segment is copied into the disk block pool and separately
  copied into the replica output buffer;
- a DataNode has one EventLoop; disk worker completion returns to that loop;
- local loopback testing may place both replicas on one physical disk, so it
  cannot be interpreted as independent-disk sustained throughput.

## Architecture

```text
acceptor EventLoop
  -> chooses one I/O EventLoop for each accepted TCP connection

I/O EventLoops (N, connection-affine)
  -> HTTP parsing, bounded request state, socket read/write, pause/resume
  -> submit SharedBlock references to disk and replica lanes

Disk worker pool
  -> SHA-256 update, pwrite, optional later fdatasync
  -> returns completion to owning I/O EventLoop

ReplicaConnectionPool (per target DataNode)
  -> 2 initially configured HTTP/1.1 Keep-Alive lanes per peer
  -> exactly one active Chunk request per lane
  -> a lane becomes reusable only after its HTTP response is parsed

Gateway
  -> batch route issuance, route leases, batch Chunk metadata writes
```

Every TCP connection has exactly one owning I/O EventLoop from creation through
close. A disk worker, metadata worker, or another I/O loop never writes a
socket directly; it schedules the state transition with `queueInLoop()` on the
connection owner.

## Replica Transport

The first V4 transport is persistent HTTP/1.1, not HTTP/2 or gRPC.

```text
Pool key: target node ID + route endpoint + transport configuration

Pool: N independent Keep-Alive lanes
Lane: Connecting | Idle | SendingChunk | AwaitingResponse | Closing | Failed
```

One lane carries exactly one Chunk PUT at a time. HTTP/1.1 request pipelining
is not used, so response ordering and error ownership remain explicit. Two
lanes allow two Chunks to the same replica to proceed concurrently without
opening a new TCP connection for every Chunk. A failed lane fails only its
active Chunk, closes, and reconnects with bounded exponential backoff.

### Lane Liveness and Half-Open Connections

`Idle` means that this process has no assigned request. It does not prove that
the peer or the network path is still alive. Each outbound lane therefore:

- enables `SO_KEEPALIVE`; on Linux it configures `TCP_KEEPIDLE`,
  `TCP_KEEPINTVL`, and `TCP_KEEPCNT` from configuration;
- treats `EPOLLRDHUP`, `EPOLLHUP`, `EPOLLERR`, `read() == 0`, and a send error
  as an immediate transition to `Failed`;
- uses `send(..., MSG_NOSIGNAL)` for normal socket writes so a peer close
  cannot terminate the process with `SIGPIPE`;
- closes the fd, releases queued blocks, and reconnects with bounded backoff
  after a failure.

The pool does not rely on a pre-assignment socket-health probe. `SO_ERROR` or
a successful probe cannot prove that a peer will still be alive at the later
write, so such a check creates a false guarantee and a race. Instead, a lane
is event-driven while idle; if its first real write fails, the uncommitted
Chunk is retried through the existing idempotent route/commit semantics after
the lane is recreated. Optional application-level idle probes may reduce the
first-request reconnect delay, but are not a correctness requirement.

The existing upload token, replica chain, `X-Replica-Position`, content length,
and Chunk hash headers are sent for every request. Keep-Alive changes socket
lifetime, not authorization scope.

Initial defaults are deliberately conservative:

```text
replica lanes per peer: 2
per-file active Chunk limit: 2
browser/process global active Chunk limit: 4
per-node route lease limit: existing configured write capacity
```

These values become configuration, not protocol constants. Increasing them
requires benchmark evidence for queue occupancy, pause time, latency, and
failure rate.

## Parallel Chunk Scheduling

The client obtains routes for a small set of not-yet-completed Chunks and
schedules them subject to all limits:

```text
global active Chunks < global window
active Chunks for file < per-file window
active routes to node < node lease/lane capacity
```

The scheduler never starts a replacement Chunk until it has atomically removed
the completed, failed, or cancelled Chunk from its active set. HTTP 503 with
`Retry-After` is a queued state, not a terminal file failure. Retries retain
the same Session and only re-route uncommitted Chunks.

Gateway route leases are the authoritative admission counter. Heartbeats report
node health and load but cannot serve as the exact active-write counter because
they are periodic and stale by design.

## Shared Body Block Ownership

V4 retains 64 KiB bounded body units. It does not promise literal NIC-to-disk
zero-copy because the system still needs integrity hashing, flow control, and
cross-thread disk ownership.

Instead, one heap/pool copy creates a reference-counted immutable
`SharedBodyBlock`:

```text
HTTP input Buffer
  -> copy once into SharedBodyBlock (64 KiB)
  -> disk work item holds one reference
  -> replica lane output holds one reference
  -> block returns to pool after all references are released
```

The replica lane writes block views with `iovec`/`writev`. A partial socket
write records the current block offset and retains the block reference until
the remaining bytes are sent. No output queue may retain raw pointers into an
HTTP input Buffer after that Buffer consumes its bytes.

The lane owns a FIFO of references, rather than a concatenated byte string:

```cpp
struct SharedWriteSlice {
    std::shared_ptr<SharedBodyBlock> block;
    size_t offset;
    size_t length;
};
```

On each `EPOLLOUT`, the lane creates a temporary `iovec` array for the leading
slices and calls `writev`. If it returns `n > 0`, the lane consumes exactly
`n` bytes: it removes fully written slices and advances the first remaining
slice's offset for a partial write. `EAGAIN` retains the queue unchanged and
keeps `EPOLLOUT` enabled; `EINTR` retries; any terminal error releases all
references and fails only the affected Chunk. A block cannot return to the
pool until both its disk reference and every unsent replica reference are
gone.

High/low watermarks apply to bytes retained by unsent SharedBodyBlocks, not
only a `std::string` output buffer. If any required downstream consumer is
above its high watermark, the upstream connection pauses reading. It resumes
only once the owning loop observes all relevant low-watermark conditions.

### Shared-Block Capacity and Liveness Invariants

The current V2 disk pool has `128 * 64 KiB = 8 MiB`. V4 shared blocks use a
similarly bounded pool. A block held by both the disk worker and a replica
lane counts once, not twice: the pool budget is the union of uniquely leased
blocks, while the following per-consumer counters diagnose who retains them:

```text
unique leased shared bytes <= pool capacity - reserved free capacity

diagnostic counters: disk queued, disk in-flight, replica unsent,
                     and retained bytes per stream
```

The exact values are configuration, but V4 must enforce all of the following
at admission time:

- a global shared-block byte/block limit below pool capacity;
- a per-upload-stream retained-byte limit;
- a per-replica-lane unsent-byte limit;
- a nonzero reserved-free-block count so one noisy stream cannot consume every
  block;
- global active-Chunk and route-lease limits consistent with those bounds.

For example, with 128 blocks, a conservative initial configuration may reserve
16 blocks and cap each of four active Chunk streams at eight retained blocks.
That admits at most 32 stream blocks, leaving headroom for in-flight work and
control transitions. The implementation must validate the configuration rather
than trusting an informal inequality such as `activeChunks * perStreamBytes <
poolBytes` alone.

Pool exhaustion is backpressure, not an HTTP error: the HTTP parser leaves the
unconsumed body bytes in its input buffer and pauses `EPOLLIN`. It is not a
deadlock because releasing an already-held block never needs a new block:
disk completion, successful socket writes, lane failure, and cancellation all
release references independently. The implementation must preserve that
property and test a permanently stalled replica, a full pool, lane failure,
and subsequent recovery.

## Metadata Batching

Gateway preserves per-Chunk idempotency and route-lease release semantics. Its
metadata worker may collect completed Chunk commits into a short bounded batch
and apply them with one LevelDB `WriteBatch`. A response is released only after
the batch containing that Chunk has been applied. Batch delay and batch size
must be bounded so an idle Chunk cannot wait indefinitely.

File commit still requires every declared Chunk to be committed. A batch must
never make an uncommitted Chunk visible in a manifest.

## Durable Mode Boundary

V4.1 protocol-throughput mode keeps current buffered `pwrite` behavior and
labels results accordingly. A later durable mode provides:

```text
pwrite
-> fdatasync on Primary and Replica
-> Chunk ACK
-> Gateway Chunk commit
```

It is separately benchmarked. `O_DIRECT` is not part of the first durable
mode: it requires aligned allocation, aligned offsets and lengths, and a
separate page-cache/read-path design.

## Failure Handling

- Client disconnect: all affected streams cancel, release leases, and release
  SharedBodyBlock references without direct cross-thread socket access.
- Replica lane error: mark its active Chunk failed or `PROTECTING` according
  to the existing replica policy, close/recreate the lane, and do not poison
  unrelated lanes.
- Disk queue pressure: pause the upstream body without converting temporary
  memory-pool exhaustion into HTTP 400.
- Gateway route capacity exhaustion: return 503 plus `Retry-After`; client
  scheduler waits and re-routes rather than starting a competing upload.
- EventLoop shutdown: each connection and lane rejects new work, drains or
  cancels owned blocks, and prevents callbacks from retaining raw `this`.

## Metrics and Acceptance Criteria

Every test report separates protocol throughput from durable throughput and
records hardware, filesystem, disk topology, and whether replicas share a
physical disk.

Required metrics:

- logical upload/download MiB/s and aggregate MiB/s;
- file and Chunk P50/P95/P99 completion latency;
- active Chunk count, active lane count, connection reuse rate;
- replica and disk queue peak bytes, pause count, pause duration;
- Gateway route/commit batch size and latency;
- I/O EventLoop lag and per-loop connection count;
- retries, HTTP 503 count, replica failures, and integrity failures.

Acceptance proceeds by stage:

1. Existing V2 serial upload/download and hash verification remain correct.
2. Two simultaneous Chunks on two persistent lanes complete with no new TCP
   connection after warm-up.
3. A slow replica produces bounded queued bytes and upstream pause/resume;
   unrelated connections continue to make progress.
4. Four logical active Chunks do not exceed configured node leases or block
   pool capacity; 503 retries eventually complete after a slot becomes free.
5. Shared-block profiling shows removal of the replica-path intermediate
   `std::string` copy without use-after-free or buffer corruption.
6. Multi-loop load has lower EventLoop tail lag than the one-loop baseline at
   the same workload, while preserving end-to-end SHA-256 validation.
7. A peer closing an idle lane, a partial `writev`, and complete shared-block
   pool exhaustion each recover without process termination, leaked blocks,
   duplicate Chunk commit, or permanent upstream pause.

## Implementation Order

1. Instrument and freeze a reproducible baseline on separate data disks where
   possible.
2. Add connection-affine multi-EventLoop infrastructure and regression tests.
3. Add HTTP/1.1 Keep-Alive request reset support and `ReplicaConnectionPool`.
4. Add route leases plus bounded client/benchmark Chunk-window scheduling.
5. Replace independent disk/replica copies with shared body blocks and partial
   `writev` state.
6. Add bounded Gateway commit batching.
7. Run correctness, fault-injection, long-duration, flame-graph, and real
   multi-machine benchmarks.

This order intentionally introduces no durability behavior change before the
protocol-throughput path is correct and measurable.
