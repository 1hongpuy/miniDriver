# MiniKVCine V4.1 混合负载治理

## 1. 本轮解决的问题

V2 已经把上传磁盘写入移出 EventLoop，但下载仍存在两个缺口：

1. 上传和下载分别管理，没有统一的节点级并发边界。
2. `TcpConnection::handleWrite()` 会在一次回调中持续调用 `sendfile`，直到文件发送完成或
   socket 返回 `EAGAIN`。当 loopback 或高速内网 socket 一直可写时，一个大下载可能长期
   占用 EventLoop，增加心跳、上传解析和其他下载的尾延迟。

V4.1 增加进程级资源准入，并把文件发送改为有界执行。它不改变 Chunk、Session、Token、
manifest 或 HTTP endpoint。

## 2. NodeResourceGovernor

`NodeResourceGovernor` 统一管理两类租约：

```text
UploadLease    一个已经准入的 Chunk PUT
DownloadLease  一个已经准入的 Chunk GET 文件响应
```

租约是 move-only RAII 对象。上传流结束、下载完成、连接关闭或错误路径销毁回调时，租约析构
并自动归还计数，业务代码不再维护容易遗漏的 acquire/release 布尔状态。

默认边界：

```text
全节点活动上传       2
全节点活动下载       8
单客户端活动上传     2
单客户端活动下载     4
```

客户端可以发送 `X-Client-Instance-Id` 作为协作式公平标识；没有该 Header 时，上传按
Session、下载按 TCP 连接区分。这个标识不是认证凭据，用户系统加入前不能把它当作安全配额。

准入失败发生在响应体开始前，因此下载返回：

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 1
```

## 3. 有界 sendfile

旧逻辑在一个 `handleWrite()` 中循环发送整个 extent。新逻辑每次 EPOLLOUT 回调最多执行一次
`sendfile`，且请求字节数不超过默认的 256 KiB：

```text
EPOLLOUT
  -> sendfile(min(remaining, quantum))
  -> 尚未完成：保留 offset/remaining，返回 EventLoop
  -> 完成：关闭文件 fd，触发完成回调
  -> 连接关闭/错误：关闭文件 fd，触发失败回调
```

这保证单次网络回调的工作量有上界，让 EventLoop 有机会处理其他 ready fd。它仍不是完整的
DRR/WFQ 跨连接调度器；Linux epoll 的 ready 顺序仍决定下一次处理哪个连接。

V4.1 的 Chunk GET 响应显式使用 `Connection: close`。这是为了在 HTTP 层尚未实现文件响应
队列前，禁止同一 HTTP/1.1 连接流水化第二个 GET 并覆盖第一个 sendfile 状态。V4.2 引入连接
调度后才能安全恢复 DataNode 下载 Keep-Alive。

`SendFileResult` 记录成功状态、累计发送字节、系统调用次数和单次最大发送字节，测试可验证
非零 extent offset、量子边界、零字节文件以及对端断开。网络层同时忽略 `SIGPIPE`，普通
发送使用 `MSG_NOSIGNAL`，将断连转换成正常错误路径而不是终止 DataNode 进程。

## 4. 配置

第一版使用环境变量，便于在不扩展现有 YAML schema 的情况下验证策略：

```text
MINIKV_V4_MAX_ACTIVE_UPLOADS
MINIKV_V4_MAX_ACTIVE_DOWNLOADS
MINIKV_V4_MAX_UPLOADS_PER_CLIENT
MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT
MINIKV_V4_SENDFILE_QUANTUM_BYTES
```

无效值或零值会回退到默认值。DataNode 启动日志会记录实际的上传上限、下载上限和 sendfile
量子；每个 Chunk 下载完成或取消时记录 `chunk_download_complete`、字节数与总耗时。

## 5. 代码边界

```text
NodeResourceGovernor.hpp  节点级上传/下载准入与 RAII 租约
datanode_main.cpp         PUT/GET 获取租约、503、指标和运行参数
TcpConnection             有界 sendfile 状态机和完成通知
HttpResponse/HttpServer   把文件响应完成通知传到连接层
CorsPolicy                允许浏览器发送客户端实例 Header
```

## 6. 验证

```bash
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

2026-08-07 验证结果：完整构建成功，41/41 测试通过。新增测试覆盖全局/单客户端准入、租约移动
与自动释放、非零 offset 文件响应、sendfile 量子、零字节和断连终止路径，以及 HTTP 文件
完成回调中的实际发送字节。

## 7. 后续 V4 工作

V4.1 只建立了治理入口和有界网络执行。混合负载要进一步稳定，还需要：

1. 多 I/O EventLoop，使连接固定归属不同 Reactor，真正使用多核网络处理能力。
2. ReplicaConnectionPool 和长连接健康检查，减少每 Chunk 建连开销。
3. 跨连接 DRR/WFQ，区分前台下载、上传副本、Repair 和媒体任务的权重。
4. 共享只读块与 `writev` 部分写状态，减少上传副本链上的内存复制。
5. 磁盘读写调度和独立配额，防止下载 pread、上传 pwrite、缩略图读取互相挤压。
6. 将客户端实例 ID、资源上限和观测指标正式接入 YAML、前端和压测工具。

当前上传准入发生在请求头解析后，但 V2 流式 Handler 还不能在 Body 到达前返回自定义 503。
因此被拒绝的 PUT 仍会读完当前请求 Body，再返回容量错误。要消除这部分无效流量，需要给
`HttpServer` 增加 header-stage rejection，并配合客户端 `Expect: 100-continue`；这属于下一
个协议层增量，不应通过返回 400 或静默断开替代。
