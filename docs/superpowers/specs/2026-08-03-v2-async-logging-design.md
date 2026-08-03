# V2 Asynchronous Logging Design

## Goal

Replace V2 Gateway and DataNode hot-path `std::cout`/`std::cerr` diagnostics with
bounded asynchronous logging. The logging path must make upload latency observable
without blocking EventLoop, HTTP body, heartbeat, or worker threads.

## Scope

This change covers V2 Gateway, DataNode, and Node Agent. It does not rewrite V1
server diagnostics, add a remote log platform, or introduce distributed tracing.

## Library and Process Model

Use the packaged `spdlog` library with its asynchronous thread pool. Every process
owns a logger and its sinks:

```text
Gateway process   -> gateway.log
DataNode process  -> datanode-<nodeId>.log
Node Agent        -> agent.log
```

The logger is initialized once at process startup. It receives a path, process
name, node id, level, rotation size, retained file count, and queue capacity from
the service configuration passed by Node Agent. Existing stdout/stderr redirection
remains only for startup failures and fatal process termination.

## Queue and Failure Policy

Default settings:

```text
queue capacity: 8192 log messages
overflow policy: overrun oldest
rotation: 20 MiB per file, 5 retained files
default level: info
```

Normal logging must never block an EventLoop. If the queue overflows, the oldest
diagnostic entries may be discarded and an atomic drop counter is incremented. A
one-minute summary reports the number discarded. Fatal logging also writes a short
message directly to stderr because the process is about to stop.

## Log Format

Use one plain-text structured line per event, readable through grep without a JSON
tool:

```text
ts=2026-08-03T12:34:56.123+08:00 level=INFO process=datanode node=node-c
tid=1234 event=chunk_complete session=... chunk=3 hash=abcd1234
bytes=4194304 total_ms=6200 local_write_ms=15 replica_ms=6060
gateway_commit_ms=40 pauses=18 pause_ms=5700 max_pending_bytes=524288
```

`hash` is truncated to 12 characters. Free-form errors are escaped so each event
remains on one line.

## Correlation and Timing

`sessionId` is the cross-process upload correlation id. A ChunkUploadStream keeps
monotonic timestamps for:

```text
request accepted
first body byte
local write/hash finished
replica response received
Gateway commit response received
client response written
```

ReplicaUploadPipe tracks pause count, accumulated pause duration, and maximum
pending bytes. A final `chunk_complete` or `chunk_failed` line emits all known
durations. The result distinguishes network/backpressure delay from local pwrite,
hashing, and Gateway control-plane delay.

## Event Levels

```text
INFO  startup/shutdown, node state transition, preflight outcome,
      route plan, chunk completion, file commit, delete completion
WARN  queue overflow summary, temporary replica failure, retry, slow chunk
ERROR chunk failure, Gateway commit failure, persistence failure
DEBUG per-request parser detail, heartbeat detail, pause/resume detail
```

Every 64 KiB body callback is intentionally excluded from INFO. Pause/resume is
logged only as DEBUG and summarized when a chunk completes.

## Initial Instrumentation

Gateway logs: node registration/state transition, upload preflight, placement,
chunk commit, file commit, and delete dispatch.

DataNode logs: chunk begin, replica connection/failure, local completion, Gateway
commit result, chunk completion/failure. Timing is emitted only at completion.

Node Agent logs: service spawn, exit status or signal, restart schedule, and
operator shutdown.

## Verification

1. Unit test structured field formatting and level filtering.
2. Start Gateway/DataNode with a small queue and force diagnostic overflow; verify
   the EventLoop remains responsive and a dropped-message summary is emitted.
3. Upload a Chunk with a delayed replica and verify the final line contains
   `replica_ms`, `pauses`, `pause_ms`, and `max_pending_bytes`.
4. Build all V2 targets and run the affected CTest subset.
