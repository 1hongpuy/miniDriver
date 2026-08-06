# V4 Replica Connection Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace one-TCP-connection-per-Chunk replica forwarding with bounded, reusable HTTP/1.1 lanes that survive idle periods, recover from half-open connections, and preserve existing V2 upload semantics.

**Architecture:** Each DataNode EventLoop owns one `ReplicaConnectionPool`. The pool maintains two `PersistentHttpLane` objects per replica endpoint; a lane owns one `TcpClient`, carries one request at a time, and returns to `Idle` only after parsing the complete response. `ReplicaUploadPipe` acquires a lane asynchronously and retains its existing bounded pre-lane buffer and upstream pause/resume behavior.

**Tech Stack:** C++17, Linux epoll, TCP keepalive, HTTP/1.1 Content-Length framing, CMake/CTest, socketpair and loopback integration tests.

## Global Constraints

- Preserve the fixed 4 MiB Chunk, token, replica-chain, route, Session, manifest, and Gateway commit contracts.
- Use HTTP/1.1 Keep-Alive with exactly one active request per lane; do not use HTTP pipelining.
- Start with two lanes per replica endpoint; make the value configuration, not a protocol constant.
- Every socket remains owned by one EventLoop; callbacks from other threads use `queueInLoop()`.
- Use `MSG_NOSIGNAL`, TCP keepalive, and event-driven HUP/EOF/error detection.
- A failed lane may fail only its active uncommitted Chunk; unrelated lanes continue.
- Do not introduce shared body blocks, `writev`, multiple EventLoops, HTTP/2, gRPC, or durability changes in this plan.
- Preserve all unrelated dirty worktree changes.

---

### Task 1: Harden outbound TCP connection liveness

**Files:**
- Modify: `include/network/TcpClient.hpp`
- Modify: `src/network/TcpClient.cpp`
- Modify: `include/network/channel.hpp`
- Modify: `src/network/Channel.cpp`
- Modify: `src/network/TcpConnection.cpp`
- Create: `test/test_tcp_connection_liveness.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces: `network::TcpKeepAliveOptions`
- Produces: `TcpClient::setKeepAliveOptions(TcpKeepAliveOptions options)`
- Preserves: existing `TcpClient::connect()` and `TcpConnection::send()` callers.

- [ ] **Step 1: Write the failing liveness test**

Create a socketpair-backed test that installs a SIGPIPE counter, closes the peer, calls `TcpConnection::send()`, runs the EventLoop, and verifies that the process remains alive and the error/close callback runs exactly once. Add a loopback TCP case that reads `SO_KEEPALIVE`, `TCP_KEEPIDLE`, `TCP_KEEPINTVL`, and `TCP_KEEPCNT` from a socket created by `TcpClient`.

```cpp
static volatile std::sig_atomic_t gSigpipeCount = 0;
std::signal(SIGPIPE, [](int) { ++gSigpipeCount; });

// Close the peer before sending. MSG_NOSIGNAL must turn this into a normal
// connection error instead of process termination.
::close(sockets[1]);
connection->send("x", 1);

MINIKV_CHECK(gSigpipeCount == 0);
MINIKV_CHECK(errorCount == 1 || closeCount == 1);
```

- [ ] **Step 2: Register and run the test to verify failure**

Add:

```cmake
add_executable(test_tcp_connection_liveness test/test_tcp_connection_liveness.cpp)
target_include_directories(test_tcp_connection_liveness PRIVATE include test)
target_link_libraries(test_tcp_connection_liveness PRIVATE minikv_net pthread)
```

and under `BUILD_TESTING`:

```cmake
add_test(NAME tcp_connection_liveness COMMAND test_tcp_connection_liveness)
```

Run:

```bash
cmake --build build -j2 --target test_tcp_connection_liveness
ctest --test-dir build -R '^tcp_connection_liveness$' --output-on-failure
```

Expected: FAIL because ordinary `write()` may raise SIGPIPE and `TcpClient` does not configure keepalive.

- [ ] **Step 3: Add explicit keepalive configuration**

Add to `TcpClient.hpp`:

```cpp
struct TcpKeepAliveOptions {
    bool enabled = true;
    int idleSeconds = 30;
    int intervalSeconds = 10;
    int probeCount = 3;
};

void setKeepAliveOptions(TcpKeepAliveOptions options) {
    keepAliveOptions_ = options;
}
```

Validate positive values in `TcpClient::onNewConnection()` and apply
`SO_KEEPALIVE`, `TCP_KEEPIDLE`, `TCP_KEEPINTVL`, and `TCP_KEEPCNT` before
constructing `TcpConnection`. On configuration failure, close the socket and
report connection failure rather than silently creating a lane with different
liveness semantics.

- [ ] **Step 4: Make read-half-close and writes safe**

Change `Channel::enableReading()` to register `EPOLLRDHUP`. Keep
`Channel::handleEventWithGuard()` routing RDHUP through the read callback so
`recv()==0` reaches `TcpConnection::handleClose()`.

Replace normal socket `write()` calls in `TcpConnection::sendInLoop()` and
`TcpConnection::handleWrite()` with:

```cpp
const ssize_t written = ::send(fd_, data, size, MSG_NOSIGNAL);
```

Handle `EINTR` by retrying later without consuming bytes, retain the existing
`EAGAIN/EWOULDBLOCK` behavior, and route terminal errors through one close path
so error and close callbacks cannot destroy the same Channel twice.

- [ ] **Step 5: Run focused and existing network tests**

```bash
cmake --build build -j2 --target test_tcp_connection_liveness test_tcp_sendfile_offset test_async_http_timeout
ctest --test-dir build -R '^(tcp_connection_liveness|tcp_sendfile_offset|async_http_timeout)$' --output-on-failure
```

Expected: all three tests PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add include/network/TcpClient.hpp src/network/TcpClient.cpp \
  include/network/channel.hpp src/network/Channel.cpp \
  src/network/TcpConnection.cpp test/test_tcp_connection_liveness.cpp CMakeLists.txt
git commit -m "fix: harden outbound TCP lane liveness"
```

---

### Task 2: Extract a resettable HTTP response parser

**Files:**
- Create: `include/http/HttpResponseParser.hpp`
- Create: `src/http/HttpResponseParser.cpp`
- Modify: `include/http/AsyncHttpClient.hpp`
- Modify: `src/http/AsyncHttpClient.cpp`
- Create: `test/test_http_response_parser.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces: `http::HttpResponseParser::consume(network::Buffer&)`
- Produces: `HttpResponseParser::reset()`
- Produces: `HttpResponseParseResult { kNeedMore, kComplete, kError }`
- Consumes: existing `HttpClientResponse`.

- [ ] **Step 1: Write parser tests for fragmented and sequential responses**

Test these exact cases:

```text
1. Status line, headers, and body split across multiple Buffer appends.
2. HTTP 200 with Content-Length: 2 and body "ok".
3. reset(), then parse a second response on the same connection.
4. malformed status line returns kError.
5. response above maxResponseBytes returns kError.
6. a keep-alive response without Content-Length returns kError.
```

Core assertion:

```cpp
HttpResponseParser parser(1024);
Buffer input;
const std::string first = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n";
input.append(first.data(), first.size());
MINIKV_CHECK(parser.consume(input) == HttpResponseParseResult::kNeedMore);
const std::string second = "Connection: keep-alive\r\n\r\nok";
input.append(second.data(), second.size());
MINIKV_CHECK(parser.consume(input) == HttpResponseParseResult::kComplete);
MINIKV_CHECK(parser.response().body == "ok");
parser.reset();
```

- [ ] **Step 2: Run the parser test to verify failure**

```bash
cmake --build build -j2 --target test_http_response_parser
ctest --test-dir build -R '^http_response_parser$' --output-on-failure
```

Expected: build FAIL because `HttpResponseParser` does not exist.

- [ ] **Step 3: Implement the parser**

Use this public shape:

```cpp
enum class HttpResponseParseResult { kNeedMore, kComplete, kError };

class HttpResponseParser {
public:
    explicit HttpResponseParser(size_t maxResponseBytes);
    HttpResponseParseResult consume(network::Buffer& input);
    void reset();
    const HttpClientResponse& response() const;
    HttpClientResponse takeResponse();
    const std::string& error() const;
};
```

The parser owns only the current response bytes. Completion requires a valid
`Content-Length`; internal replica responses must not use connection close as
body framing because that would make the lane non-reusable. No chunked response
support is added in this phase.

- [ ] **Step 4: Refactor one-shot `AsyncHttpRequest` to use the parser**

Replace `responseBytes_`, `parsedResponse_`, `responseBodyOffset_`, and related
flags with `HttpResponseParser responseParser_`. Preserve the existing
`Connection: close` behavior of `AsyncHttpRequest`; this task is a behavior-
preserving extraction that gives the persistent lane an independently tested
parser.

- [ ] **Step 5: Run parser and one-shot client tests**

```bash
cmake --build build -j2 --target test_http_response_parser test_async_http_timeout
ctest --test-dir build -R '^(http_response_parser|async_http_timeout)$' --output-on-failure
```

Expected: PASS.

- [ ] **Step 6: Commit Task 2**

```bash
git add include/http/HttpResponseParser.hpp src/http/HttpResponseParser.cpp \
  include/http/AsyncHttpClient.hpp src/http/AsyncHttpClient.cpp \
  test/test_http_response_parser.cpp CMakeLists.txt
git commit -m "refactor: extract HTTP response parser"
```

---

### Task 3: Implement one persistent HTTP lane

**Files:**
- Create: `include/http/PersistentHttpLane.hpp`
- Create: `src/http/PersistentHttpLane.cpp`
- Create: `test/test_persistent_http_lane.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `TcpKeepAliveOptions`, `HttpResponseParser`, `AsyncHttpRequestOptions`, `AsyncWriteResult`.
- Produces: `PersistentHttpLane::connect()`
- Produces: `PersistentHttpLane::beginRequest(AsyncHttpRequestOptions, ReadyCallback, ResponseCallback)`
- Produces: `PersistentHttpLane::write(const char*, size_t)`, `finishBody()`, `close()`
- Produces: lane states `kDisconnected`, `kConnecting`, `kIdle`, `kWritingBody`, `kWaitingResponse`, `kBackoff`, `kClosing`.

- [ ] **Step 1: Write a two-request one-connection integration test**

Create a loopback server that accepts one TCP connection, parses two complete
Content-Length PUT requests sequentially, responds to each with:

```http
HTTP/1.1 200 OK
Content-Length: 2
Connection: keep-alive

ok
```

The test submits request 2 only after request 1 completes and asserts:

```cpp
MINIKV_CHECK(server.acceptCount() == 1);
MINIKV_CHECK(server.requestCount() == 2);
MINIKV_CHECK(first.status == 200);
MINIKV_CHECK(second.status == 200);
MINIKV_CHECK(lane->state() == PersistentHttpLane::State::kIdle);
```

- [ ] **Step 2: Run the test to verify failure**

```bash
cmake --build build -j2 --target test_persistent_http_lane
ctest --test-dir build -R '^persistent_http_lane$' --output-on-failure
```

Expected: build FAIL because the lane does not exist.

- [ ] **Step 3: Implement lane connection and request state**

Use this public API:

```cpp
class PersistentHttpLane : public std::enable_shared_from_this<PersistentHttpLane> {
public:
    using Ptr = std::shared_ptr<PersistentHttpLane>;
    enum class State { kDisconnected, kConnecting, kIdle, kWritingBody,
                       kWaitingResponse, kBackoff, kClosing };
    using StateCallback = std::function<void(State)>;
    using ReadyCallback = std::function<void()>;
    using ResponseCallback = std::function<void(HttpClientResponse, std::string)>;

    static Ptr create(network::EventLoop* loop,
                      std::string address,
                      uint16_t port,
                      network::TcpKeepAliveOptions keepAlive);
    void connect();
    bool beginRequest(AsyncHttpRequestOptions options,
                      ReadyCallback ready,
                      ResponseCallback response);
    AsyncWriteResult write(const char* data, size_t size);
    void finishBody();
    void close();
    State state() const;
    size_t queuedBytes() const;
    void setStateCallback(StateCallback callback);
};
```

`beginRequest()` succeeds only in `kIdle`, resets the response parser, emits
`Connection: keep-alive`, and starts the body stream. Completion clears all
per-request callbacks and counters before setting `kIdle`. It never destroys
the `TcpClient` after a successful response.

- [ ] **Step 4: Implement reconnect and half-open behavior**

On idle EOF/HUP/error, transition to `kBackoff` and reconnect after bounded
delays `100, 200, 400, 800, 1600, 3200, 5000 ms`. Reset backoff after a
successful connection. On active failure, invoke that request's response
callback exactly once with an error, clear request state, then reconnect.

Use weak captures for timers and connection callbacks:

```cpp
std::weak_ptr<PersistentHttpLane> weakSelf(shared_from_this());
reconnectTimerId_ = loop_->runAfter(delayMs, [weakSelf] {
    if(auto self = weakSelf.lock()) self->connectInLoop();
});
```

- [ ] **Step 5: Add half-open fault injection to the test**

After the first response, have the server close the idle connection. Submit a
second request after the lane reconnects and assert two accepts, one completion
per request, and no SIGPIPE. Also test closing during an active body and assert
one request error followed by successful lane recovery.

- [ ] **Step 6: Run focused tests**

```bash
cmake --build build -j2 --target test_persistent_http_lane test_tcp_connection_liveness test_http_response_parser
ctest --test-dir build -R '^(persistent_http_lane|tcp_connection_liveness|http_response_parser)$' --output-on-failure
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```bash
git add include/http/PersistentHttpLane.hpp src/http/PersistentHttpLane.cpp \
  test/test_persistent_http_lane.cpp CMakeLists.txt
git commit -m "feat: add persistent HTTP lane"
```

---

### Task 4: Build the bounded replica connection pool

**Files:**
- Create: `include/DataNode/ReplicaConnectionPool.hpp`
- Create: `src/DataNode/ReplicaConnectionPool.cpp`
- Create: `test/test_replica_connection_pool.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `PersistentHttpLane`.
- Produces: `ReplicaEndpoint` key.
- Produces: `ReplicaConnectionPool::acquire(endpoint, callback)`.
- Produces: `ReplicaConnectionPool::release(lane)`.

- [ ] **Step 1: Write pool bound and fairness tests**

Configure two lanes for one endpoint. Queue three acquisitions and assert the
first two callbacks receive distinct lanes, the third remains queued, and
releasing either lane serves the third waiter exactly once. Add a second
endpoint and verify its lane count and waiter queue are independent.

```cpp
ReplicaConnectionPool::Config config;
config.lanesPerEndpoint = 2;
config.maxQueuedAcquiresPerEndpoint = 16;
config.keepAlive = {true, 30, 10, 3};
```

- [ ] **Step 2: Run the test to verify failure**

```bash
cmake --build build -j2 --target test_replica_connection_pool
ctest --test-dir build -R '^replica_connection_pool$' --output-on-failure
```

Expected: build FAIL because the pool does not exist.

- [ ] **Step 3: Implement endpoint-keyed lane ownership**

Use this API:

```cpp
struct ReplicaEndpoint {
    std::string nodeId;
    std::string address;
    uint16_t port = 0;
    bool operator<(const ReplicaEndpoint& other) const;
};

class ReplicaConnectionPool {
public:
    struct Config {
        size_t lanesPerEndpoint = 2;
        size_t maxQueuedAcquiresPerEndpoint = 16;
        network::TcpKeepAliveOptions keepAlive;
    };
    using LanePtr = http::PersistentHttpLane::Ptr;
    using AcquireCallback = std::function<void(LanePtr, std::string)>;

    ReplicaConnectionPool(network::EventLoop* loop, Config config);
    void acquire(ReplicaEndpoint endpoint, AcquireCallback callback);
    void release(const LanePtr& lane);
    void shutdown();
};
```

All methods marshal to the pool's EventLoop. Acquisition is FIFO. Only a
connected `kIdle` lane is handed out. A released healthy lane serves the oldest
waiter; a failed lane remains owned by the pool while it reconnects. Reject a
new waiter with a clear overload error when its bounded queue is full.

- [ ] **Step 4: Add pool metrics**

Expose a snapshot containing endpoint count, connected/active/failed lane
counts, queued acquisitions, reconnect count, and reused request count. Reading
the snapshot must happen on the owner loop or through an asynchronous callback;
do not add a mutex around lane internals.

- [ ] **Step 5: Run the pool and lane tests**

```bash
cmake --build build -j2 --target test_replica_connection_pool test_persistent_http_lane
ctest --test-dir build -R '^(replica_connection_pool|persistent_http_lane)$' --output-on-failure
```

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add include/DataNode/ReplicaConnectionPool.hpp \
  src/DataNode/ReplicaConnectionPool.cpp test/test_replica_connection_pool.cpp CMakeLists.txt
git commit -m "feat: add bounded replica connection pool"
```

---

### Task 5: Route replica uploads through pooled lanes

**Files:**
- Modify: `include/DataNode/ReplicaUploadPipe.hpp`
- Modify: `src/DataNode/ReplicaUploadPipe.cpp`
- Modify: `src/DataNode/datanode_main.cpp`
- Create: `test/test_replica_keepalive_integration.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `ReplicaConnectionPool::acquire()` and `release()`.
- Changes: `ReplicaUploadPipe::create(EventLoop*, ReplicaConnectionPool*)`.
- Preserves: `ReplicaUploadPipe::push()`, `finish()`, `abort()`, completion callback, and upstream pause/resume semantics.

- [ ] **Step 1: Write an end-to-end two-Chunk reuse test**

Run a fake replica HTTP server and two sequential `ReplicaUploadPipe` instances
through one pool. Each sends a known body and receives HTTP 200. Assert:

```cpp
MINIKV_CHECK(server.acceptCount() == 1);
MINIKV_CHECK(server.requestCount() == 2);
MINIKV_CHECK(server.body(0) == firstBody);
MINIKV_CHECK(server.body(1) == secondBody);
MINIKV_CHECK(pool.metrics().reusedRequests >= 1);
```

- [ ] **Step 2: Run the integration test to verify failure**

```bash
cmake --build build -j2 --target test_replica_keepalive_integration
ctest --test-dir build -R '^replica_keepalive_integration$' --output-on-failure
```

Expected: build FAIL because `ReplicaUploadPipe` still owns one-shot
`AsyncHttpRequest` objects.

- [ ] **Step 3: Replace one-shot request ownership in `ReplicaUploadPipe`**

Store `ReplicaConnectionPool* pool_` and `PersistentHttpLane::Ptr lane_`.
`start()` requests a lane using `nodeId/address/port` from the existing route.
Until acquisition completes, keep the existing bounded `pendingBlocks_`; when
it reaches `maxPendingBytes`, return pause rather than error. Once acquired,
call `lane_->beginRequest()`, flush pending bytes in order, and resume upstream.

After response or error:

```cpp
auto lane = std::move(lane_);
if(lane) pool_->release(lane);
completeInLoop(std::move(response), std::move(error));
```

Abort releases the lane only after the lane has cancelled/cleared the active
request, preventing a second pipe from observing stale callbacks.

- [ ] **Step 4: Create one pool in DataNode startup**

Construct one `ReplicaConnectionPool` next to the DataNode EventLoop and pass
its pointer into each `ChunkUploadStream`/`ReplicaUploadPipe`. Do not construct
a pool per upload. Shutdown the pool before destroying the EventLoop.

Initial configuration:

```cpp
ReplicaConnectionPool::Config replicaPoolConfig;
replicaPoolConfig.lanesPerEndpoint = 2;
replicaPoolConfig.maxQueuedAcquiresPerEndpoint = 16;
replicaPoolConfig.keepAlive = {true, 30, 10, 3};
```

- [ ] **Step 5: Add reuse and reconnect log/metrics events**

Emit structured events only on state changes, not per 64 KiB body block:

```text
event=replica_lane_connected peer=<node> lane=<id>
event=replica_lane_reused peer=<node> lane=<id> requests=<count>
event=replica_lane_failed peer=<node> lane=<id> active=<0|1> error=<reason>
event=replica_lane_reconnected peer=<node> lane=<id> backoff_ms=<n>
```

- [ ] **Step 6: Run all affected unit and integration tests**

```bash
cmake --build build -j2
ctest --test-dir build -R '^(tcp_connection_liveness|http_response_parser|persistent_http_lane|replica_connection_pool|replica_keepalive_integration|async_http_timeout|http_stream_context|chunk_disk_write_pipeline)$' --output-on-failure
```

Expected: PASS.

- [ ] **Step 7: Run a local two-DataNode smoke test**

Upload at least three files totaling at least 64 MiB through the existing V2
benchmark/client, then verify:

```text
- every Chunk and downloaded file hash matches;
- both replicas contain each committed Chunk;
- after warm-up, replica request count increases while accepted TCP connection
  count remains at two or fewer per endpoint;
- killing the replica during idle causes reconnect and the next uncommitted
  Chunk retries without terminating either DataNode;
- queued bytes remain below existing watermarks.
```

- [ ] **Step 8: Commit Task 5**

```bash
git add include/DataNode/ReplicaUploadPipe.hpp src/DataNode/ReplicaUploadPipe.cpp \
  src/DataNode/datanode_main.cpp test/test_replica_keepalive_integration.cpp CMakeLists.txt
git commit -m "feat: reuse replica HTTP connections"
```

---

## Follow-On Plans

After this plan passes its smoke and fault-injection tests, write separate plans
in this order:

1. bounded per-file/global parallel Chunk scheduling and Gateway route leases;
2. `SharedBodyBlock` ownership, partial `writev`, pool quotas, and exhaustion tests;
3. connection-affine multiple EventLoops and cross-loop scheduling;
4. bounded Gateway commit batching;
5. protocol-throughput and durable-mode benchmark comparison.

This separation keeps each performance change attributable to one measured
mechanism and preserves a working V2-compatible system at every checkpoint.
