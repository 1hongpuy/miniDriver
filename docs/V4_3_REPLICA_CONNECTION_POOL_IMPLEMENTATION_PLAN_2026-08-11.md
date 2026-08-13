# V4.3 副本长连接池：完整修改计划（2026-08-11）

## 目标

将 DataNode 链式复制从“每个 Chunk 新建一次 TCP/HTTP 短连接”改为“按副本目标复用有界、
顺序化的 HTTP/1.1 Keep-Alive 连接”。目标是降低副本路径的连接建立、HTTP 解析和对象分配
开销，重点改善高并发或跨机网络下的 Replica P95/P99；不改变 Chunk hash、链式双副本、
Gateway commit、写入租约或 `503 + Retry-After` 语义。

## 现状与约束

当前 `AsyncHttpRequest` 是一次性对象：

```text
ReplicaUploadPipe
  → AsyncHttpRequest::create()
  → TcpClient::create()
  → Connection: close
  → 请求完成后 client->stop()
```

因此不能通过移除 `Connection: close` 获得安全复用。HTTP 请求的响应解析器、超时、Body 计数、
回调和连接销毁均绑定单次请求；若直接复用，会有以下风险：

- 上一个响应剩余字节被下一个请求误解析；
- 超时回调作用于下一请求；
- 对端半开或关闭后仍把连接借出；
- 同时两条请求写入同一 HTTP/1.1 连接导致响应无法正确匹配；
- DataNode shutdown 时跨 EventLoop 的对象销毁。

本轮只支持 **一条连接同一时刻一个请求，不实现 HTTP pipelining**。这降低状态机复杂度，
并符合当前单 Chunk 串行发送模型。

## 修改后的结构

```text
ChunkUploadStream
  → ReplicaUploadPipe
      → ReplicaConnectionPool（按目标 endpoint 取 idle session）
          → PersistentHttpSession（一个 TcpClient + 一个 TcpConnection）
              → 顺序执行一个 PUT 请求
              → 完整解析 response 后归还池
```

连接键采用：

```text
nodeId + address + httpPort
```

不能只用地址端口：`nodeId` 是生命周期、日志和节点替换时的业务身份；同一 endpoint 被错误注册为
不同节点时应在日志中可诊断。

## 阶段 1：可复用持久 HTTP 会话

### 新增/修改文件

- 新增 `include/http/PersistentHttpSession.hpp`
- 新增 `src/http/PersistentHttpSession.cpp`
- 保留 `AsyncHttpRequest` 原有语义，不在第一步强行重构它。
- `CMakeLists.txt`：将新实现加入 `minikv_http`。

### 会话状态机

```text
Idle
  → Connecting
  → WritingHeaders
  → WritingBody
  → WaitingResponse
  → Idle                 （成功、完整响应、连接仍健康）
  → Closing / Closed     （超时、EOF、协议错误、HTTP 连接关闭、取消）
```

关键规则：

1. 请求头发送 `Connection: keep-alive`；若服务端回包声明 `Connection: close`，会话不归还池。
2. 只接受具有明确 `Content-Length` 的响应；副本 PUT 成功响应本身很小，协议缺少长度时直接关闭，
   不尝试猜测消息边界。
3. 请求 Body 字节数必须恰好等于 `contentLength`；超出、提前 finish 或部分写失败都关闭会话。
4. 响应完成后清空所有请求态字段：headers、response body、Body 计数、timeout、回调、解析状态；
   再转换为 Idle。
5. 连接关闭、读写错误、超时、解析错误、取消均是 session 不可复用条件。
6. 所有状态机操作只在 `ownerLoop`；外部线程只通过 `runInLoop/queueInLoop` 投递。

### 必须先写的测试

新增 `test/test_persistent_http_session.cpp`，以 loopback mock server 覆盖：

- 同一 TCP 连接顺序完成两个 PUT，服务端 accept 次数为 1；
- 第一响应后保留的输入字节/第二响应不会串包；
- 服务端 `Connection: close` 后会话标记 Closed，第二次请求必须新连接；
- 对端 EOF、连接中途关闭、无效 Content-Length、超时、取消均回调一次且会话不可复用；
- Body 超过/少于 `Content-Length` 时请求失败；
- 所有回调和关闭均发生在 session 的 EventLoop。

### 阶段验收

- 新测试及现有 `async_http_timeout`、Multi-Reactor HTTP 测试通过。
- 原 `AsyncHttpRequest` 仍可独立工作；未接入副本路径前，原有 benchmark 行为不变。

## 阶段 2：ReplicaConnectionPool

### 新增/修改文件

- 新增 `include/DataNode/ReplicaConnectionPool.hpp`
- 新增 `src/DataNode/ReplicaConnectionPool.cpp`
- 新增 `test/test_replica_connection_pool.cpp`
- `CMakeLists.txt`：加入 `minikv_datanode` 与测试目标。

### 数据结构与资源上限

```text
PoolKey(nodeId, address, port)
  ├─ idle sessions: deque<Session>
  ├─ connecting sessions: bounded
  ├─ borrowed sessions: bounded
  ├─ retryUntil / consecutiveFailures
  └─ lastUsed timestamp
```

初始保守配置：

- 每目标最多 2 条 session；
- 每 DataNode 全局最多 `maxActiveUploads` × 副本深度的连接数，初始可设为 4；
- 空闲 TTL 30 秒；
- connect/response timeout 复用当前 30 秒副本超时；
- 连续失败采用指数退避，例如 100 ms、500 ms、1 s，上限 5 s；
- 不建立无限等待队列：无空闲 session 且达到上限时向 `ReplicaUploadPipe` 返回可背压的“暂不可借”。

连接池不拥有上传写槽；它只管理网络连接预算。上传准入仍由 Gateway WriteLease 与 DataNode
`NodeResourceGovernor` 决定。

### 公共操作

- `borrow(key, callback)`：仅在 owner Loop 调用；返回可用 session、连接中、或暂不可用。
- `release(session)`：仅在响应完整、健康且 Keep-Alive 可用时入 idle 队列。
- `discard(session, reason)`：关闭并移除；更新失败计数/退避。
- `sweepIdle(now)`：定时关闭超过 TTL 的 idle session。
- `shutdown(callback)`：停止新借用、取消借用者、在所属 Loop 关闭所有 session，再执行 callback。
- `metrics()`：连接建立、复用命中、借用拒绝、丢弃原因、idle/borrowed/connecting 峰值；标签只用
  `reason`、`nodeId` 的聚合桶，不记录 sessionId/chunk hash。

### 必须先写的测试

- 首次借用创建连接、归还后第二次借用复用同一连接；
- 同一 session 不会被同时借出；达到每目标或全局上限时返回背压；
- 服务端关闭/半开/超时后 discard，下一借用创建新连接；
- 失败退避期间不发生连接风暴；
- idle TTL 回收；
- pool shutdown 与 EventLoopThread 停止时无 UAF/死锁；
- metrics 计数正确。

### 阶段验收

- 池在 mock server 下可稳定复用，所有故障测试通过。
- 未接入生产副本路径前，不修改 `ReplicaUploadPipe` 的既有短连接行为。

## 阶段 3：接入 ReplicaUploadPipe

### 修改文件

- 修改 `include/DataNode/ReplicaUploadPipe.hpp`
- 修改 `src/DataNode/ReplicaUploadPipe.cpp`
- 修改 `src/DataNode/datanode_main.cpp`
- 修改 `test/test_replica_upload_metrics.cpp`，新增集成测试。

### 接入方式

1. `datanode_main` 创建一个由 base Loop 管理、但 session 始终绑定发起请求的 worker Loop 的池实例。
   若池跨多个 I/O Loop，池按 Loop 分片；不能把某个 `TcpConnection` 从 Loop A 借给 Loop B。
2. `ChunkUploadStream::startReplica()` 将 pool/key 传给 `ReplicaUploadPipe`。
3. `ReplicaUploadPipe` 借到 session 后发送 headers/body；连接中或池满时使用现有 bounded
   `pendingBlocks_` 与 `pauseRead()`，不读入无限 Body。
4. 成功 200、完整响应、无 `Connection: close` 时归还；任何错误、非 200、超时、cancel 时 discard。
5. **保留短连接 fallback**：仅当池暂时不可用且尚未达到失败退避时允许一次受控新建；fallback
   失败后必须报告明确错误，不能静默丢副本。

### 关闭顺序

```text
停止新 HTTP 接收
  → 取消/结束 ChunkUploadStream
  → ReplicaUploadPipe 归还或 discard session
  → Pool 停止借用并关闭 idle/borrowed session
  → 等待各 I/O Loop barrier
  → EventLoopThreadPool stop
```

所有步骤应有测试；不得在 base Loop 直接析构 worker Loop 所属连接。

### 阶段验收

- 双副本上传、hash/commit、断点续传、503 写入准入、DataNode 删除等既有测试保持通过。
- 相邻 Chunk 到同一目标 DataNode 时日志显示 connection reuse，而不是每个 Chunk 新建 TCP。
- 连接错误时请求不会挂住，调用方得到确定错误或可重试失败。

## 阶段 4：指标与性能验证

### 新增指标

- `replica_connection_created_total`
- `replica_connection_reused_total`
- `replica_connection_discarded_total{reason}`
- `replica_pool_borrow_rejected_total{reason}`
- `replica_pool_idle/borrowed/connecting`
- `replica_connect_ms`、`replica_request_ms`

日志聚合器增加连接复用率、discard 原因、每目标连接数峰值。不得使用 `sessionId`、`chunkHash`
作为指标标签。

### 固定对比矩阵

在相同 `/data`、RelWithDebInfo、16 MiB、双节点双副本条件下，对比短连接与长连接：

- upload-only：并发 1/2；
- mixed：并发 2/4；
- 每档 3 轮；
- 如有可用跨机节点，再重复一组受控网络延迟测试。

每档记录：吞吐、文件 P50/P95/P99、Chunk/Replica P50/P95/P99、连接建立次数、复用率、
CPU/RSS、BlockPool peak、磁盘 queue/pause、EventLoop lag、错误码和 SHA-256 结果。

### 成功标准

- 所有成功对象端到端 hash 正确；不出现异常 400、泄漏、悬挂或连接数无限增长。
- 同负载下 TCP 连接建立次数明显下降；至少一个目标指标有可重复改善：Replica P95/P99、CPU
  或整体上传吞吐。
- 若 loopback P50 无收益但跨机 P95/P99 改善，应如实保留该结论；不能只挑有利指标。
- 若长连接使失败恢复、内存或尾延迟恶化，则回退为短连接并保留测试证据。

## 不在本次修改中做

- HTTP pipelining、多请求并发复用同一连接；
- TLS、HTTP/2/gRPC；
- 多 Gateway、跨地域故障恢复；
- SharedBodyBlock、Chunk window、磁盘 worker 数调整。

这些应在长连接池正确性和性能结果稳定后再处理，避免把多个变量混入一次优化实验。
