# V4.2 Multi-Reactor DataNode Design

## 1. Status And Scope

This specification defines V4.2 of the MiniKV data plane. V4.2 changes the
DataNode HTTP server from one process-wide EventLoop to one accept loop plus a
fixed pool of connection I/O loops.

V4.2 does not implement the replica connection pool, request multiplexing,
shared body blocks, `writev`, adaptive admission control, or disk scheduling.
Those changes depend on a correct connection-affine Multi-Reactor foundation
and remain later V4 work.

The Gateway stays on its current single EventLoop in this phase. Its metadata
operations already use their existing worker and locking boundaries, and its
traffic consists primarily of control requests rather than Chunk bytes.

## 2. Existing Problem

The current `TcpServer` creates every `TcpConnection` on the acceptor's
EventLoop:

```cpp
EventLoop* ioLoop = loop_;
```

Consequently, one thread performs all DataNode socket reads, HTTP parsing,
body callbacks, response writes, and `sendfile` progress. The D1 disk executor
removes most blocking `pwrite` work from that thread, but it cannot make socket
processing use more than one CPU core. Concurrent uploads and downloads still
compete in one event queue.

Changing only `TcpServer` to assign connections to worker loops would be
incorrect:

- `HttpServer::contexts_` is a shared, unlocked map.
- `ChunkUploadStream` is constructed with the base loop instead of the
  connection loop.
- `DeferredResponse` is constructed with the base loop.
- the shared `HttpGatewayControlClient` dispatches Chunk commit callbacks on
  the base loop.

Those objects currently happen to be safe because the server has one loop.
They would become cross-thread state mutations after a partial Multi-Reactor
conversion.

## 3. Required Invariants

V4.2 must preserve the following invariants:

1. The acceptor and the `TcpServer::connections_` registry belong exclusively
   to the base EventLoop.
2. A `TcpConnection` is assigned to exactly one I/O EventLoop and never moves.
3. The connection's Channel, input/output buffers, HTTP parser, upload stream,
   replica pipe, deferred response, and asynchronous control-request callback
   are used only on that connection's I/O EventLoop.
4. Worker threads may perform disk work, but completion is posted back to the
   originating connection loop before upload state changes.
5. Closing a connection removes it from the base-loop registry and destroys
   its Channel on its owner I/O loop.
6. No callback may capture a raw `ChunkUploadStream*` across asynchronous work.
   Existing weak ownership remains mandatory.
7. Setting the I/O thread count to zero preserves the current single-loop
   behavior for compatibility and focused tests.

## 4. Architecture

```text
DataNode process

base EventLoop
  -> Acceptor
  -> TcpServer connection registry
  -> round-robin EventLoop selection
  -> registration and heartbeat control requests

EventLoopThreadPool
  -> I/O EventLoop 0
       -> TcpConnection A
       -> connection-local HttpContext A
       -> ChunkUploadStream A / DeferredResponse A
  -> I/O EventLoop 1
       -> TcpConnection B
       -> connection-local HttpContext B
       -> ChunkUploadStream B / DeferredResponse B

shared thread-safe services
  -> FastDataStore
  -> DiskWriteExecutor
  -> NodeResourceGovernor
  -> AsyncLogger
```

The base loop does not parse HTTP bodies and does not send Chunk responses once
worker loops are enabled. It accepts sockets, selects an I/O loop, and owns the
server-level connection registry.

## 5. Components

### 5.1 EventLoopThread

`EventLoopThread` owns one `std::thread` and the EventLoop running inside it.
`startLoop()` blocks only until the thread has constructed its EventLoop and
returns a stable pointer to that loop. A mutex and condition variable protect
startup publication.

Destruction requests `quit()` and joins the thread. Copying is disabled. The
class must not expose the thread's EventLoop before it is ready or allow its
thread to outlive the owner.

### 5.2 EventLoopThreadPool

`EventLoopThreadPool` belongs to the base EventLoop and owns a configured number
of `EventLoopThread` instances.

```cpp
void setThreadNum(size_t count);
void start();
EventLoop* nextLoop();
size_t size() const;
```

`nextLoop()` uses round-robin assignment. With zero worker threads it returns
the base loop. The pool does not migrate established connections and does not
perform load-based scheduling in V4.2.

### 5.3 TcpServer

`TcpServer` gains `setThreadNum(size_t)`. `start()` starts the I/O loop pool
before enabling the acceptor.

On accept:

1. The base loop chooses `ioLoop = threadPool_->nextLoop()`.
2. It constructs the `TcpConnection` with `ioLoop` and stores it in the
   base-loop `connections_` map.
3. It queues `connectEstablished()` onto `ioLoop`.

On close:

1. The connection owner loop invokes the internal close callback.
2. The callback queues `removeConnectionInLoop()` onto the base loop.
3. The base loop erases the registry entry.
4. It queues `connectDestroyed()` back onto the connection owner loop.

This two-loop handoff prevents concurrent map access and ensures Channel
removal occurs on the Poller that owns it.

### 5.4 Acceptor

One readable event drains `accept4()` until `EAGAIN` or `EWOULDBLOCK` instead
of accepting only one socket. `EINTR` retries. Other errors are logged without
spinning. This keeps the base accept loop from becoming the first bottleneck
under connection bursts.

### 5.5 TcpConnection Context And Loop Access

`TcpConnection` exposes its immutable owner through:

```cpp
EventLoop* ownerLoop() const noexcept;
```

It also stores one opaque, connection-local application context. The network
layer does not depend on `HttpContext`; `HttpServer` stores and retrieves a
`std::shared_ptr<HttpContext>` through the opaque context API.

This replaces `HttpServer::contexts_`. A mutex around the old global map is not
used because it would introduce shared contention on every message and leave
parser lifetime coupled to file descriptors that can be reused.

### 5.6 HttpServer

`HttpServer::onConnection()` creates the parser context on connection
establishment and attaches it to the connection. `onMessage()` retrieves that
connection-local parser.

All immediate response operations use `conn->ownerLoop()`. `DeferredResponse`
is created with the connection owner loop, not `HttpServer::loop_`.

The optional existing business `ThreadPool` remains supported. Its completion
lambda posts the response to `conn->ownerLoop()`. It must not post connection
work to the base loop.

### 5.7 DataNode Upload Affinity

The DataNode body setup callback constructs `ChunkUploadStream` with
`upstream->ownerLoop()`.

Registration and heartbeat continue to use the base-loop
`HttpGatewayControlClient`. Each `ChunkUploadStream` creates or owns a control
request sender bound to its connection loop for Chunk commit and lease release.
This avoids invoking upload completion on the base loop.

The disk pipeline and replica pipe are also created on the upload's owner loop.
Their existing weak-pointer callbacks remain in place.

## 6. Configuration

The DataNode reads:

```text
MINIKV_V4_IO_THREADS
```

Rules:

- default: `2`
- minimum: `0`
- maximum: `32`
- `0`: compatibility mode using the base EventLoop
- invalid values: use the default and emit one warning

The value is process-local and is not advertised as storage write capacity.
`maxConcurrentWrites` and `maxConcurrentDownloads` remain independent admission
limits controlled by `NodeResourceGovernor`.

## 7. Shared-State Review

The following services may be called concurrently by multiple connection
loops and must retain their existing internal synchronization:

- `FastDataStore`
- `DiskWriteExecutor`
- `NodeResourceGovernor`
- asynchronous logging

Immutable configuration such as node ID, CORS policy, Gateway address, port,
and cluster secret may be copied into per-upload objects.

The following state must not be shared across connection loops:

- `HttpContext`
- `TcpConnection` buffers and Channel
- `ChunkUploadStream`
- `ChunkDiskWritePipeline` state transitions
- `ReplicaUploadPipe`
- `AsyncHttpRequest`
- `DeferredResponse`

## 8. Shutdown And Failure Handling

`TcpServer` shutdown follows this order:

1. stop accepting new sockets;
2. execute registry cleanup on the base loop;
3. queue connection destruction on each owner loop;
4. wait until queued connection destruction has run;
5. quit and join I/O loop threads.

No EventLoop thread may be joined while one of its Channels is still registered.
Thread startup failure propagates before the acceptor starts listening; the
server must not silently run with a partial pool.

`TcpConnection` keeps the existing SIGPIPE protection. A client disconnect,
HTTP parser error, disk cancellation, replica failure, or Gateway commit failure
affects only the connection and upload stream that owns the operation.

## 9. Observability

Startup logs include:

```text
event=datanode_started io_threads=<N>
```

Multi-Reactor tests and debug logs must make it possible to identify the owner
loop/thread of a connection. Per-request production logs do not add thread
handoff noise because the async logger already records the thread ID.

The V4.2 performance comparison records:

- aggregate upload and download throughput;
- 1, 2, 4, and 8 concurrent connections;
- mixed upload/download throughput;
- CPU utilization by I/O thread;
- EventLoop delay percentiles;
- admission rejection counts;
- disk queue and replica pause metrics retained from V4.1.

## 10. Test Strategy

Focused tests must verify:

1. `EventLoopThread` publishes a running loop and exits cleanly.
2. `EventLoopThreadPool` returns the base loop in zero-thread mode.
3. Round-robin assignment distributes connections across configured loops.
4. All callbacks for one connection execute on the same thread.
5. Closing a worker-loop connection removes it from the base registry without
   leaking a Channel or file descriptor.
6. Two simultaneous HTTP connections use independent parser contexts,
   including fragmented headers and streaming bodies.
7. A deferred response completes on the connection owner loop.
8. Disk, replica, and Gateway commit completions mutate `ChunkUploadStream`
   only on its owner loop.
9. Existing single-loop network, HTTP, upload, download, CORS, and sendfile
   tests continue to pass.

Verification ends with a full CMake build and complete CTest run. Performance
results are compared against the V4.1 loopback baseline; correctness and thread
affinity are acceptance requirements, while a specific throughput increase is
not required for correctness.

## 11. Completion Boundary

V4.2 is complete when the DataNode can use multiple I/O EventLoops without
cross-thread parser or upload state access, while zero-thread compatibility
mode and the existing test suite remain valid.

The following work remains outside V4.2:

- persistent replica connections and health checks;
- HTTP request multiplexing;
- shared body-block ownership and partial `writev` cursors;
- adaptive or weighted connection scheduling;
- `SO_REUSEPORT` acceptors;
- multiple Gateway I/O loops;
- fair disk scheduling and read/write quotas.
