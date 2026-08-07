# V4.2 Multi-Reactor DataNode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DataNode socket and HTTP processing use a connection-affine pool of EventLoops while preserving the existing single-loop mode and all V4.1 upload/download behavior.

**Architecture:** The base EventLoop owns the Acceptor and connection registry. `EventLoopThreadPool` assigns each accepted socket to one immutable owner loop; HTTP parsing, upload state, replica forwarding, deferred responses, and per-upload Gateway control requests remain on that owner loop. Shared storage, disk-executor, governor, and logging services retain their existing synchronization.

**Tech Stack:** C++17, Linux epoll/eventfd/timerfd, pthread/std::thread, HTTP/1.1, CMake/CTest.

## Global Constraints

- V4.2 changes the DataNode to Multi-Reactor; Gateway remains single Reactor.
- `MINIKV_V4_IO_THREADS` defaults to `2`, accepts `0..32`, and `0` preserves single-loop behavior.
- A connection and all of its protocol/upload state stay on one immutable owner EventLoop.
- `TcpServer::connections_` and Acceptor remain base-loop owned.
- Connection Channel destruction always runs on the connection owner EventLoop.
- Existing weak-pointer ownership across asynchronous disk, replica, and control-plane work remains mandatory.
- Do not implement replica connection pooling, `writev`, shared body blocks, adaptive scheduling, or `SO_REUSEPORT` in this plan.
- Do not overwrite unrelated dirty frontend, profiling, configuration, or note files.

---

## File Map

**Create**

- `include/network/EventLoopThread.hpp`: owns one EventLoop thread and its startup/shutdown synchronization.
- `src/network/EventLoopThread.cpp`: starts, publishes, quits, and joins the EventLoop.
- `include/network/EventLoopThreadPool.hpp`: base-loop-owned round-robin I/O loop pool.
- `src/network/EventLoopThreadPool.cpp`: pool startup, selection, barriers, and shutdown.
- `test/test_event_loop_thread.cpp`: EventLoopThread lifecycle and callback-affinity tests.
- `test/test_event_loop_thread_pool.cpp`: zero-thread fallback and round-robin tests.
- `test/test_tcp_connection_context.cpp`: owner-loop and connection-local opaque context test.
- `test/test_tcp_server_multi_reactor.cpp`: real-socket connection distribution and close-lifecycle test.
- `test/test_http_server_multi_reactor.cpp`: fragmented HTTP requests and connection-local parser test.

**Modify**

- `include/network/TcpConnection.hpp`: expose `ownerLoop()` and opaque connection context.
- `src/network/TcpConnection.cpp`: enforce context cleanup on the owner loop.
- `include/network/Acceptor.hpp`: add explicit stop support.
- `src/network/Acceptor.cpp`: drain `accept4()` and stop accepting safely.
- `include/network/TcpServer.hpp`: own the loop pool and expose `setThreadNum()`/`stop()`.
- `src/network/TcpServer.cpp`: assign connections, perform two-loop removal, and synchronize shutdown.
- `include/http/HttpServer.hpp`: replace the global parser map and route responses to connection owner loops.
- `include/http/DeferredResponse.hpp`: derive the completion loop from the connection.
- `src/http/DeferredResponse.cpp`: queue completion to `connection->ownerLoop()`.
- `src/DataNode/datanode_main.cpp`: configure I/O loops and bind each upload/control request to its connection loop.
- `CMakeLists.txt`: build and register the five new focused tests.

---

### Task 1: EventLoop Thread Lifecycle

**Files:**
- Create: `include/network/EventLoopThread.hpp`
- Create: `src/network/EventLoopThread.cpp`
- Create: `test/test_event_loop_thread.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces: `EventLoopThread::startLoop() -> EventLoop*`
- Produces: `EventLoopThread::stop()`
- Consumes: existing `EventLoop::loop()`, `EventLoop::quit()`, and `EventLoop::queueInLoop()`

- [ ] **Step 1: Write the failing lifecycle test**

```cpp
#include "network/EventLoop.hpp"
#include "network/EventLoopThread.hpp"
#include "TestCheck.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

int main()
{
    miniKV::network::EventLoopThread thread;
    auto* loop = thread.startLoop();
    MINIKV_CHECK(loop != nullptr);

    std::mutex mutex;
    std::condition_variable condition;
    bool called = false;
    std::thread::id callbackThread;
    loop->queueInLoop([&] {
        std::lock_guard<std::mutex> lock(mutex);
        called = true;
        callbackThread = std::this_thread::get_id();
        condition.notify_one();
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        MINIKV_CHECK(condition.wait_for(lock, std::chrono::seconds(2), [&] { return called; }));
    }
    MINIKV_CHECK(callbackThread != std::this_thread::get_id());
    MINIKV_CHECK(thread.startLoop() == loop);
    thread.stop();
    thread.stop();
    return 0;
}
```

- [ ] **Step 2: Register and run the test to verify it fails**

Add to `CMakeLists.txt`:

```cmake
add_executable(test_event_loop_thread test/test_event_loop_thread.cpp)
target_link_libraries(test_event_loop_thread PRIVATE minikv_net pthread)
```

Add under `if(BUILD_TESTING)`:

```cmake
add_test(NAME event_loop_thread COMMAND test_event_loop_thread)
```

Run:

```bash
cmake -S . -B build
cmake --build build --target test_event_loop_thread -j2
```

Expected: compilation fails because `network/EventLoopThread.hpp` does not exist.

- [ ] **Step 3: Implement EventLoopThread**

Use this public contract:

```cpp
class EventLoopThread {
public:
    EventLoopThread() = default;
    ~EventLoopThread();
    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    EventLoop* startLoop();
    void stop();

private:
    void threadMain();
    std::mutex mutex_;
    std::condition_variable condition_;
    std::thread thread_;
    EventLoop* loop_ = nullptr;
    bool started_ = false;
};
```

`threadMain()` constructs `EventLoop loop` on the worker stack, publishes
`loop_` while holding `mutex_`, notifies `condition_`, runs `loop.loop()`, and
sets `loop_ = nullptr` before returning. `startLoop()` starts once and waits on
`condition_` with predicate `loop_ != nullptr`. `stop()` snapshots `loop_`,
calls `quit()`, joins if joinable, and is idempotent.

- [ ] **Step 4: Run the focused test**

```bash
cmake --build build --target test_event_loop_thread -j2
ctest --test-dir build -R '^event_loop_thread$' --output-on-failure
```

Expected: `100% tests passed, 0 tests failed`.

- [ ] **Step 5: Commit the lifecycle component**

```bash
git add CMakeLists.txt include/network/EventLoopThread.hpp src/network/EventLoopThread.cpp test/test_event_loop_thread.cpp
git commit -m "feat: add EventLoop thread lifecycle"
```

---

### Task 2: EventLoop Thread Pool

**Files:**
- Create: `include/network/EventLoopThreadPool.hpp`
- Create: `src/network/EventLoopThreadPool.cpp`
- Create: `test/test_event_loop_thread_pool.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `EventLoopThread::startLoop()` and `EventLoopThread::stop()`
- Produces: `setThreadNum(size_t)`, `start()`, `nextLoop()`, `size()`, and `stop()`

- [ ] **Step 1: Write the failing pool test**

The test constructs a base `EventLoop` without running it and verifies fallback
and worker-loop selection:

```cpp
miniKV::network::EventLoop baseLoop;
miniKV::network::EventLoopThreadPool fallback(&baseLoop);
fallback.setThreadNum(0);
fallback.start();
MINIKV_CHECK(fallback.size() == 0);
MINIKV_CHECK(fallback.nextLoop() == &baseLoop);

miniKV::network::EventLoopThreadPool pool(&baseLoop);
pool.setThreadNum(2);
pool.start();
auto* first = pool.nextLoop();
auto* second = pool.nextLoop();
auto* third = pool.nextLoop();
MINIKV_CHECK(first != &baseLoop);
MINIKV_CHECK(second != &baseLoop);
MINIKV_CHECK(first != second);
MINIKV_CHECK(third == first);
pool.stop();
```

- [ ] **Step 2: Register and run the failing test**

Add target `test_event_loop_thread_pool`, link `minikv_net pthread`, and add
CTest name `event_loop_thread_pool`. Build it and expect failure because the
pool header is absent.

- [ ] **Step 3: Implement the fixed-size round-robin pool**

Use this contract:

```cpp
class EventLoopThreadPool {
public:
    explicit EventLoopThreadPool(EventLoop* baseLoop);
    ~EventLoopThreadPool();
    void setThreadNum(size_t count);
    void start();
    void stop();
    EventLoop* nextLoop();
    size_t size() const noexcept { return loops_.size(); }

private:
    EventLoop* baseLoop_;
    size_t threadCount_ = 0;
    size_t next_ = 0;
    bool started_ = false;
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;
};
```

Reject `setThreadNum()` after `start()` with `std::logic_error`. `start()` must
fully start every requested loop or stop already-started threads and rethrow.
`nextLoop()` must be called from the base loop thread and return the base loop
when `loops_` is empty. `stop()` is idempotent and clears loops after joins.

- [ ] **Step 4: Run both EventLoop tests**

```bash
cmake --build build --target test_event_loop_thread test_event_loop_thread_pool -j2
ctest --test-dir build -R '^event_loop_thread(_pool)?$' --output-on-failure
```

Expected: both tests pass.

- [ ] **Step 5: Commit the pool**

```bash
git add CMakeLists.txt include/network/EventLoopThreadPool.hpp src/network/EventLoopThreadPool.cpp test/test_event_loop_thread_pool.cpp
git commit -m "feat: add EventLoop thread pool"
```

---

### Task 3: Connection Owner And Opaque Context

**Files:**
- Modify: `include/network/TcpConnection.hpp`
- Modify: `src/network/TcpConnection.cpp`
- Create: `test/test_tcp_connection_context.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces: `TcpConnection::ownerLoop() const noexcept`
- Produces: opaque `setContext(std::any)`, `context()`, and `clearContext()`

- [ ] **Step 1: Write the failing owner/context test**

Use `socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, ...)` and
an `EventLoopThread`. Construct a `TcpConnection` on the worker loop, then queue:

```cpp
connection->connectEstablished();
connection->setContext(std::string("connection-local"));
MINIKV_CHECK(connection->ownerLoop()->isInLoopThread());
MINIKV_CHECK(std::any_cast<std::string>(connection->context()) == "connection-local");
connection->connectDestroyed();
```

Signal the test thread with a condition variable. Assert `ownerLoop()` is the
worker loop, the callback thread differs from the test thread, and destruction
completes without leaving the context populated.

- [ ] **Step 2: Register and run the test to verify it fails**

Add `test_tcp_connection_context`, link `minikv_net pthread`, and add CTest name
`tcp_connection_context`. Build and expect failure because `ownerLoop()` and
the context API are absent.

- [ ] **Step 3: Add owner-loop and opaque context APIs**

Add `<any>` and these methods to `TcpConnection`:

```cpp
EventLoop* ownerLoop() const noexcept { return loop_; }
void setContext(std::any context);
const std::any& context() const noexcept { return context_; }
void clearContext();
```

`setContext()` and `clearContext()` assert or abort when called outside
`ownerLoop()`. Store `std::any context_` beside connection callbacks. Clear it
inside `connectDestroyed()` after the disconnected connection callback has run.

- [ ] **Step 4: Run the focused connection test**

```bash
cmake --build build --target test_tcp_connection_context -j2
ctest --test-dir build -R '^tcp_connection_context$' --output-on-failure
```

Expected: the test passes and the connection is destroyed on its owner loop.

- [ ] **Step 5: Commit the connection ownership primitive**

```bash
git add CMakeLists.txt include/network/TcpConnection.hpp src/network/TcpConnection.cpp test/test_tcp_connection_context.cpp
git commit -m "feat: add connection-local protocol context"
```

---

### Task 4: Multi-Reactor TcpServer And Acceptor

**Files:**
- Modify: `include/network/Acceptor.hpp`
- Modify: `src/network/Acceptor.cpp`
- Modify: `include/network/TcpServer.hpp`
- Modify: `src/network/TcpServer.cpp`
- Create: `test/test_tcp_server_multi_reactor.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `EventLoopThreadPool::nextLoop()` and `TcpConnection::ownerLoop()`
- Produces: `TcpServer::setThreadNum(size_t)` and synchronous `TcpServer::stop()`

- [ ] **Step 1: Write the failing real-socket distribution test**

Start a `TcpServer` on a test-selected loopback port with two I/O threads. Open
four clients. In the connection callback record each connected callback's
thread ID and `conn->ownerLoop()`. Assert:

- four connections are established;
- exactly two worker thread IDs are observed;
- callbacks distribute `2/2` by round robin;
- each message callback uses the same thread ID as its connection callback;
- after clients close, all four disconnected callbacks occur before `stop()` returns.

- [ ] **Step 2: Register and run the failing test**

Add target `test_tcp_server_multi_reactor`, link `minikv_net pthread`, and add
CTest name `tcp_server_multi_reactor`. Build and expect failure because
`TcpServer::setThreadNum()` is missing.

- [ ] **Step 3: Drain the Acceptor and add stop()**

Add `Acceptor::stop()` that disables the accept Channel on its owner loop.
Change `handleRead()` to:

```cpp
for(;;) {
    sockaddr_in client{};
    socklen_t length = sizeof(client);
    int fd = ::accept4(acceptFd_, reinterpret_cast<sockaddr*>(&client), &length,
                       SOCK_NONBLOCK | SOCK_CLOEXEC);
    if(fd >= 0) {
        configureAcceptedSocket(fd);
        if(newConnectionCallback_) newConnectionCallback_(fd, client);
        else ::close(fd);
        continue;
    }
    if(errno == EINTR) continue;
    if(errno == EAGAIN || errno == EWOULDBLOCK) break;
    miniKV::utils::logError("event=accept_failed errno=" + std::to_string(errno));
    break;
}
```

- [ ] **Step 4: Implement TcpServer loop assignment and two-loop close**

Add an `EventLoopThreadPool` member. `start()` starts it before `listen()`.
`newConnection()` stores the connection on the base loop, then calls:

```cpp
ioLoop->runInLoop([conn] { conn->connectEstablished(); });
```

The internal close callback queues removal to the base loop. Base-loop removal
erases the map and queues `connectDestroyed()` to `conn->ownerLoop()`.

Implement synchronous `stop()` for calls on the base loop:

1. stop the Acceptor;
2. move all registry connections into a local vector and clear the map;
3. queue `connectDestroyed()` to each owner loop;
4. queue one FIFO barrier per distinct worker loop and wait for all barriers;
5. stop and join the EventLoopThreadPool.

Zero-thread mode executes destruction directly on the base loop. Destructor
calls `stop()` only when it is running on the base-loop thread; tests and
servers must destroy `TcpServer` on that thread.

- [ ] **Step 5: Run network Multi-Reactor tests**

```bash
cmake --build build --target test_event_loop_thread_pool test_tcp_server_multi_reactor -j2
ctest --test-dir build -R '^(event_loop_thread_pool|tcp_server_multi_reactor)$' --output-on-failure
```

Expected: all tests pass repeatedly, including ten consecutive runs of
`tcp_server_multi_reactor`.

- [ ] **Step 6: Commit the Multi-Reactor server**

```bash
git add CMakeLists.txt include/network/Acceptor.hpp src/network/Acceptor.cpp include/network/TcpServer.hpp src/network/TcpServer.cpp test/test_tcp_server_multi_reactor.cpp
git commit -m "feat: distribute connections across EventLoops"
```

---

### Task 5: Connection-Affine HttpServer And Deferred Responses

**Files:**
- Modify: `include/http/HttpServer.hpp`
- Modify: `include/http/DeferredResponse.hpp`
- Modify: `src/http/DeferredResponse.cpp`
- Create: `test/test_http_server_multi_reactor.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `TcpServer::setThreadNum()`, `TcpConnection::ownerLoop()`, and opaque connection context
- Produces: `HttpServer::setThreadNum(size_t)` and connection-affine deferred completion

- [ ] **Step 1: Write the failing HTTP isolation test**

Create two real loopback clients. Send fragmented requests concurrently:

```text
client A: "GET /alpha HTTP/1.1\r\nHost: local\r\n" then "Connection: close\r\n\r\n"
client B: "GET /beta HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
```

Configure `HttpServer::setThreadNum(2)`. The callback records
`std::this_thread::get_id()` by request path and returns the path as the body.
Assert both clients receive their own path, both callbacks run off the base
test thread, and `/alpha` and `/beta` execute on different worker threads.

- [ ] **Step 2: Register and run the failing test**

Add `test_http_server_multi_reactor`, link `minikv_http minikv_net minikv_utils
pthread`, and add CTest name `http_server_multi_reactor`. Build and expect
failure because `HttpServer::setThreadNum()` is absent.

- [ ] **Step 3: Replace HttpServer's global context map**

Remove `contexts_`. On connect attach:

```cpp
conn->setContext(std::make_shared<HttpContext>());
```

In `onMessage()` retrieve:

```cpp
const auto* holder = std::any_cast<std::shared_ptr<HttpContext>>(&conn->context());
if(holder == nullptr || !*holder) return;
auto context = *holder;
HttpContext* ctx = context.get();
```

Capture `context`, not a map iterator, in worker tasks. On parser error clear
the context before shutdown. Forward `setThreadNum(size_t)` to `TcpServer`.
Post business-thread response completion to `conn->ownerLoop()`, never
`HttpServer::loop_`.

- [ ] **Step 4: Make DeferredResponse connection-affine**

Replace its factory with:

```cpp
static Ptr create(const network::TcpConnectionPtr& connection,
                  bool closeAfterResponse);
```

Initialize `loop_` from `connection->ownerLoop()`. Update both HttpServer call
sites while retaining atomic duplicate-completion protection and weak
connection ownership.

- [ ] **Step 5: Add synchronous HttpServer shutdown**

Add `~HttpServer() { server_.stop(); }`. This runs while HttpServer callbacks
still exist, before member destruction invalidates callback captures.

- [ ] **Step 6: Run HTTP and deferred-response regressions**

```bash
cmake --build build --target test_http_server_multi_reactor test_http_stream_context test_async_http_timeout -j2
ctest --test-dir build -R '^(http_server_multi_reactor|http_stream_context|async_http_timeout)$' --output-on-failure
```

Expected: all tests pass and each connection keeps an independent parser.

- [ ] **Step 7: Commit connection-affine HTTP state**

```bash
git add CMakeLists.txt include/http/HttpServer.hpp include/http/DeferredResponse.hpp src/http/DeferredResponse.cpp test/test_http_server_multi_reactor.cpp
git commit -m "refactor: bind HTTP state to connection loops"
```

---

### Task 6: DataNode Connection Affinity And Configuration

**Files:**
- Modify: `src/DataNode/datanode_main.cpp`
- Modify: `test/test_v2_datanode_delete.sh`
- Modify: `CMakeLists.txt` only if a separate configuration test target is needed

**Interfaces:**
- Consumes: `HttpServer::setThreadNum()` and `TcpConnection::ownerLoop()`
- Produces: `MINIKV_V4_IO_THREADS` configuration and startup logging

- [ ] **Step 1: Add a failing DataNode configuration/integration assertion**

Extend the DataNode shell integration setup with:

```bash
export MINIKV_V4_IO_THREADS=2
```

Start DataNode, upload and read two different chunks concurrently, verify both
SHA-256 values, and require the DataNode log to contain:

```text
event=datanode_started io_threads=2
```

Run the integration test and expect failure because the log field and server
thread configuration do not exist.

- [ ] **Step 2: Parse the I/O thread setting**

Add:

```cpp
constexpr uint32_t kDefaultIoThreads = 2;

uint32_t configuredIoThreads()
{
    const char* value = std::getenv("MINIKV_V4_IO_THREADS");
    if(value == nullptr || *value == '\0') return kDefaultIoThreads;
    try {
        const unsigned long parsed = std::stoul(value);
        if(parsed <= 32) return static_cast<uint32_t>(parsed);
    } catch(...) {}
    miniKV::utils::logWarn("event=invalid_io_thread_config fallback=2");
    return kDefaultIoThreads;
}
```

Call `server.setThreadNum(ioThreads)` before `server.start()` and include
`io_threads=<N>` in the DataNode startup log.

- [ ] **Step 3: Bind ChunkUploadStream to the upstream connection loop**

In `setBodyStreamSetup`, replace the base-loop constructor argument with:

```cpp
upstream->ownerLoop()
```

Remove the shared base-loop `GatewayControlClient&` from `ChunkUploadStream`.
Inside each upload stream construct:

```cpp
std::unique_ptr<GatewayControlClient> gatewayControl_ =
    std::make_unique<HttpGatewayControlClient>(
        loop_, gatewayAddress_, gatewayPort_, clusterSecret_);
```

Use `gatewayControl_->commitChunk()` and `gatewayControl_->releaseLease()`.
Registration and heartbeat keep using the existing base-loop control client.

- [ ] **Step 4: Add loop-affinity assertions at upload state boundaries**

At the beginning of `consume`, `finish`, `onLocalFinish`,
`onReplicaComplete`, Gateway commit callback, and `completeClient`, assert:

```cpp
if(!loop_->isInLoopThread()) std::abort();
```

Keep disk, replica, and commit callbacks weak. Do not capture a strong stream
reference solely to cross a thread boundary.

- [ ] **Step 5: Run DataNode focused tests**

```bash
cmake --build build --target minikv_v2_datanode test_chunk_disk_write_pipeline test_node_resource_governor -j2
ctest --test-dir build -R '^(datanode_delete|chunk_disk_write_pipeline|node_resource_governor)$' --output-on-failure
```

Expected: all tests pass with `MINIKV_V4_IO_THREADS=2`; rerun
`datanode_delete` with `MINIKV_V4_IO_THREADS=0` and expect the same result.

- [ ] **Step 6: Commit DataNode integration**

```bash
git add src/DataNode/datanode_main.cpp test/test_v2_datanode_delete.sh
git commit -m "feat: enable DataNode Multi-Reactor I/O"
```

---

### Task 7: Full Verification And V4.2 Performance Comparison

**Files:**
- Create: `docs/V4_2_MULTI_REACTOR_PERFORMANCE_REPORT_2026-08-07.md`
- Modify: only files required by defects found during verification

**Interfaces:**
- Consumes: existing `minikv_v2_bench` and V4.1 metrics/log fields
- Produces: correctness evidence and single-loop versus two-loop comparison

- [ ] **Step 1: Build the full repository**

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
```

Expected: all targets build without warnings promoted to errors or link
failures.

- [ ] **Step 2: Run the full test suite**

```bash
ctest --test-dir build --output-on-failure
```

Expected: `100% tests passed`.

- [ ] **Step 3: Stress the lifecycle tests**

```bash
for i in $(seq 1 100); do
  ctest --test-dir build -R '^(event_loop_thread|event_loop_thread_pool|tcp_server_multi_reactor|http_server_multi_reactor)$' --output-on-failure || exit 1
done
```

Expected: all 100 iterations pass without hang, crash, leaked process, or file
descriptor growth.

- [ ] **Step 4: Compare zero and two I/O loops**

Run the same loopback workload twice:

```text
MINIKV_V4_IO_THREADS=0
MINIKV_V4_IO_THREADS=2
```

For each mode record:

- upload-only throughput at 1/2/4/8 connections;
- download-only throughput at 1/2/4/8 connections;
- simultaneous upload/download aggregate throughput;
- Chunk P50/P95/P99 latency;
- process and per-thread CPU utilization;
- EventLoop delay percentiles;
- 503 admission counts;
- disk queue peak bytes and pause counts.

Use identical Chunk data, replica count, governor limits, disk path, build type,
and benchmark duration in both runs.

- [ ] **Step 5: Write the comparison report**

The report must include environment, exact commands, raw tables, interpretation,
and remaining bottlenecks. Do not claim Multi-Reactor improved throughput when
the measurements show disk, network, SHA-256, or replica synchronization is
still the limiting resource.

- [ ] **Step 6: Check only intended diffs**

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; unrelated pre-existing dirty files remain
untouched.

- [ ] **Step 7: Commit verification documentation**

```bash
git add docs/V4_2_MULTI_REACTOR_PERFORMANCE_REPORT_2026-08-07.md
git commit -m "docs: report V4.2 Multi-Reactor performance"
```

---

## Final Acceptance Criteria

- DataNode starts with `0` or `2` I/O worker loops and serves valid uploads and downloads in both modes.
- Connection callbacks, HTTP parser state, upload state, replica callbacks, deferred responses, and per-upload Gateway callbacks stay on one owner loop.
- The base loop alone owns accept and the connection registry.
- Connection shutdown does not hang, leak file descriptors, or destroy Channels from the wrong Poller thread.
- Existing V4.1 admission, disk backpressure, CORS, sendfile, logging, thumbnail, and metadata tests still pass.
- The performance report compares identical single-loop and Multi-Reactor workloads without overstating the result.
