# V2 完整性能测试与分析框架

> 状态：测试设计和当前基线分析。本文不把尚未采集的指标写成已有结果。
> 适用范围：单 Gateway、两个 DataNode、客户端直连主 DataNode、链式副本的 V2 数据面。

## 1. 要回答的问题

性能测试不是只报告“上传多少 MiB/s”。必须同时回答：

1. 单文件上传、下载的实际速度是多少？
2. 多文件并发时，集群聚合吞吐能否扩展，单文件尾延迟如何变化？
3. Gateway 的建会话、路由、commit、manifest 是否先成为瓶颈？
4. 副本或磁盘变慢时，背压是否限制内存并正确减慢上游？
5. EventLoop 是否被 hash、磁盘 I/O、内存复制或同步控制面操作阻塞？
6. 哪些问题属于 V2 的有意边界，哪些是实现缺陷？

## 2. 术语和指标口径

### 2.1 吞吐

```text
单文件上传吞吐 = fileBytes / (首个 Chunk PUT 开始到最后一次 commit 成功)
单文件下载吞吐 = fileBytes / (manifest 完成到整文件 SHA-256 校验完成)
聚合端到端吞吐 = 一轮所有成功文件字节数 / 该轮墙钟时间
```

统一使用 `MiB/s`。网络展示可补充 `Gbit/s`：

```text
Gbit/s = MiB/s * 8 / 1024
```

端到端吞吐可包含本地输入生成、上传、下载和校验，但必须明确标注，不能和纯网络带宽混用。

### 2.2 两种并发

```text
文件并发（file concurrency）
  同时上传多少个不同文件，例如 1 / 2 / 4 / 8。

Chunk 窗口（chunk window）
  同一个文件同一时刻允许多少个未完成 Chunk，例如 1 / 2 / 4 / 8。
```

当前 V2 基准仅实现文件并发。单文件传输近似窗口 `1`：客户端完成一个 Chunk 的上传与
确认后，才处理下一个 Chunk。Chunk 窗口属于后续 V4 数据路径能力，不应在当前测试中
伪造为已经支持。

### 2.3 延迟分位数

对每一个 Chunk 记录总耗时和分阶段耗时，再输出：

```text
P50：50% 的请求不超过该耗时
P95：95% 的请求不超过该耗时
P99：99% 的请求不超过该耗时
max：最慢一次请求
```

平均值不能替代 P95/P99。例如 99 个 Chunk 用时 100 ms、1 个用时 10 s，平均值约 199 ms，
但用户实际会看到一次严重卡顿。

## 3. 完整测试矩阵

### A. 单流基线

| 配置 | 文件大小 | 文件并发 | Chunk 窗口 | 次数 | 目标 |
|---|---:|---:|---:|---:|---|
| A1 | 64 MiB | 1 | 1 | 10 | 小文件基础吞吐和抖动 |
| A2 | 1 GiB | 1 | 1 | 3 | RAW/视频连续流和写回压力 |
| A3 | 4 GiB | 1 | 1 | 1 | 长文件稳定性；空间足够时才执行 |

### B. 文件并发扩展

| 配置 | 文件大小 | 文件并发 | Chunk 窗口 | 次数 | 目标 |
|---|---:|---:|---:|---:|---|
| B1 | 64 MiB | 1 | 1 | 5 | 对照组 |
| B2 | 64 MiB | 2 | 1 | 5 | 低并发扩展 |
| B3 | 64 MiB | 4 | 1 | 5 | V2 推荐上限附近 |
| B4 | 64 MiB | 8 | 1 | 5 | 压力边界；2 GiB 节点先观察再决定 |

每轮都记录“每文件速度”和“该轮聚合速度”。只看聚合速度会掩盖单文件体验恶化。

### C. Chunk 窗口扩展（V4 前置设计验证）

此组在 V2 不执行。实现 chunk window 后执行：

| 配置 | 文件大小 | 文件并发 | Chunk 窗口 | 次数 |
|---|---:|---:|---:|---:|
| C1 | 1 GiB | 1 | 1 | 3 |
| C2 | 1 GiB | 1 | 2 | 3 |
| C3 | 1 GiB | 1 | 4 | 3 |
| C4 | 1 GiB | 1 | 8 | 3 |

验收不只是吞吐提高。必须保证断点续传位图正确、乱序 commit 幂等、同一节点写入数受限，
且 `P99` 不因无界排队恶化。

### D. 读写混合

| 配置 | 上传端 | 下载端 | 目标 |
|---|---:|---:|---|
| D1 | 2 个 64 MiB 上传 | 0 | 写基线 |
| D2 | 2 个 64 MiB 上传 | 2 个已有 64 MiB 下载 | 读写争用 |
| D3 | 4 个上传 | 4 个下载 | 资源隔离和尾延迟 |

下载对象应预先存在，避免把上传失败或 commit 延迟混入下载结果。

### E. 控制面 QPS

控制面与字节传输分开测。使用固定小 Body 或 mock DataNode 响应，逐级增加客户端数：

```text
10 / 50 / 100 个并发客户端
30 秒预热 + 60 秒统计 + 30 秒冷却
```

分别统计：

| API | QPS | 延迟 | 正确性 |
|---|---|---|---|
| `POST /api/v2/upload/sessions` | 创建会话 | P50/P95/P99 | Session 持久化且 ID 不冲突 |
| `POST .../routes` | Placement/租约 | P50/P95/P99 | 不选 Offline、容量不足或重复副本 |
| `POST /internal/v2/chunk-commits` | commit | P50/P95/P99 | 同一幂等键不重复写引用 |
| `GET /api/v2/files/{hash}/manifest` | 读取元数据 | P50/P95/P99 | 副本列表与 Gateway 一致 |

控制面 QPS 没有单一“合格数值”。先画客户端数与 P95 的曲线；P95 陡升、错误率增加或
EventLoop lag 上升的位置，就是当前 Gateway 的饱和点。

### F. 背压与故障注入

| 情况 | 注入方式 | 必须观察 |
|---|---|---|
| 慢副本 | `tc netem` 限速/延迟，或测试代理限速 | 输出缓冲峰值、pause 次数、pause 时长 |
| 副本短断线 | 传输中关闭副本端口 | 主副本结果、客户端响应、Session 可恢复性 |
| 慢磁盘 | 测试盘或 `fio` 干扰，仅隔离环境执行 | EventLoop lag、P99、内存上限 |
| Gateway 短暂不可用 | 暂停 Gateway 进程 | DataNode commit 超时、客户端重试语义 |

禁止在存有真实摄影素材的机器上直接执行破坏性 `tc`、磁盘写满或 kill 测试。

### G. 长稳测试

```text
时长：30 分钟起步，目标 2 小时
负载：2--4 个持续 1 GiB 上传循环 + 1--2 个下载循环
采样：每秒系统指标，每 10 秒应用指标
```

观察 RSS 是否单调增长、磁盘可用空间是否符合预期、错误率是否随时间升高、节点是否因
心跳延迟被误判 Offline，以及 P99 是否持续劣化。

## 4. 每个请求需要的观测点

### 4.1 Gateway

```text
gateway_http_requests_total{route,status}
gateway_request_duration_us{route}
gateway_sessions_active
gateway_routes_issued_total
gateway_placement_duration_us
gateway_chunk_commits_total{result}
gateway_commit_duration_us
gateway_manifest_duration_us
gateway_leveldb_write_duration_us
gateway_event_loop_lag_us
```

### 4.2 DataNode

```text
datanode_chunk_upload_duration_us
datanode_chunk_download_duration_us
datanode_bytes_written_total
datanode_bytes_read_total
datanode_hash_duration_us
datanode_pwrite_duration_us
datanode_pread_duration_us
datanode_replica_duration_us
datanode_active_uploads
datanode_active_downloads
datanode_active_replica_writes
datanode_event_loop_lag_us
```

### 4.3 背压

```text
tcp_output_buffer_bytes{peer}
tcp_output_buffer_peak_bytes{peer}
replica_pause_total
replica_pause_duration_us
replica_pending_bytes
replica_pending_bytes_peak
upstream_read_pause_total
```

不要把 nodeId、sessionId、chunkHash 放进 Prometheus label；它们基数无限，应写入结构化
日志或按需 trace。指标 label 只保留有限集合，例如 route、结果码、node role。

### 4.4 分阶段时间线

每个 Chunk 以 `sessionId + chunkIndex + attempt` 关联：

```text
t0 Gateway route request begin
t1 route response received
t2 client PUT begin
t3 primary local write/hash complete
t4 replica ACK received
t5 Gateway commit complete
```

由此可以得到：

```text
route = t1 - t0
primaryWrite = t3 - t2
replication = t4 - t3
commit = t5 - t4
chunkEndToEnd = t5 - t0
```

没有这条时间线时，只能知道“慢”，不能证明是 Gateway、主盘、副本还是网络慢。

## 5. 当前 V2 已知结果

以下来自本机 loopback 基准 [V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md](V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md)：

| 负载 | 上传吞吐 | 下载吞吐 | 聚合端到端中位数 |
|---|---:|---:|---:|
| 64 MiB，1 文件 | 277.6 MiB/s | 177.3 MiB/s | 81.08 MiB/s |
| 64 MiB，2 文件 | 202.3 MiB/s/文件中位数 | 115.6 MiB/s/文件中位数 | 117.38 MiB/s |
| 64 MiB，4 文件 | 104.9 MiB/s/文件中位数 | 64.8 MiB/s/文件中位数 | 132.20 MiB/s |
| 1 GiB，1 文件 | 153.6 MiB/s | 108.6 MiB/s | 51.71 MiB/s |

这说明 V2 从 1 到 2 并发有收益，但从 2 到 4 已出现强竞争和显著波动。4 并发三轮聚合吞吐
为 `138.63 / 132.20 / 88.82 MiB/s`；不能报告成“稳定 138 MiB/s”。

## 6. 当前问题与代码原因

### 问题 0：8 文件并发触发陈旧心跳导致的 Placement 全拒绝

2026-07-29 在本机 loopback 集群执行 `64 MiB x 8 文件并发 x 2 轮`：

```text
第 1 轮：8/8 成功，端到端聚合吞吐 84.10 MiB/s
第 2 轮：0/8 成功，全部在 routes 阶段返回 HTTP 400
错误：no route available or invalid route request
```

原始产物：

```text
/tmp/minikv-v2-placement-repro-20260729-160928/
```

复现期间 Gateway 的 `/api/v2/admin/nodes` 一直显示两个节点为 `ONLINE`，各有约
5.23 GB 可用空间；单个待路由 Chunk 为 4 MiB。因此下列 Placement 过滤条件中，
状态、容量、capability 都不构成拒绝理由。剩余的硬过滤条件是：

```cpp
runtime.activeUploads >= record.maxConcurrentWrites
```

位置：[GategayState.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/gateway/GategayState.cpp:295)。

DataNode 实际在每个 Chunk 的流开始时增加 `activeWrites`，在异步响应完成时才减少；
但它只每 8 秒上报一次心跳：[datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:399)、
[datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:492)。节点注册的
`maxConcurrentWrites` 固定为 2：[datanode_main.cpp](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:410)。

第一次并发写入开始时，Gateway 尚未收到反映负载的心跳，所以发出了超过 2 的路由；随后
心跳上报了忙碌值，Gateway 在下一次 routes 请求中把两个节点都过滤掉。完成后的真实
写入计数尚未来得及由下一次心跳清零，形成最长约 8 秒的“无可用节点”窗口。

这是 V2 Placement 的正确性问题，不是性能数值。测试工具应把它报告为失败，不能把第二轮
的零吞吐纳入性能中位数。

短期修复应在 Gateway 的 PlacementContext 中维护和释放租约/commit 对应的预留写槽，
或至少不把低频 `activeUploads` 心跳当成精确硬闸门；DataNode 则必须真正拒绝超过自身上限
的写流。两者要一起设计，不能仅提高 `maxConcurrentWrites` 掩盖问题。

### 问题 1：I/O loop 同时承担网络、hash、写盘与转发

上传 body 进入 [ChunkUploadStream::consume](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/datanode_main.cpp:174)，
同步调用 [WriteSession::append](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:43)。
其中包含 `EVP_DigestUpdate` 与 `pwrite`。因此磁盘慢、hash 重或副本背压会延迟这个 loop
上的其他连接处理。

这是 V2 的有意简化，不是“epoll 本身慢”。epoll 只负责就绪通知；问题在于就绪回调里做了
可能阻塞或耗 CPU 的工作。

### 问题 2：副本转发虽流式，但仍有应用层复制

[ReplicaUploadPipe::push](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/ReplicaUploadPipe.cpp:87)
将当前 body 段送给异步请求；[AsyncHttpRequest::writeInLoop](/home/ubuntu/miniKV/v1.0/miniKVCine/src/http/AsyncHttpClient.cpp:131)
再调用 `TcpConnection::send`。当前
[TcpConnection::send(const char*, size_t)](/home/ubuntu/miniKV/v1.0/miniKVCine/src/network/TcpConnection.cpp:213)
会构造 `std::string`，再进入 EventLoop lambda 和 output buffer。已有 profile 中 `memmove`
自身约为 6.49%。

流式的意义是内存窗口不会随 4 MiB Chunk 或文件总大小线性增长；它不等于零拷贝。

### 问题 3：下载路径仍是“整 Chunk 读入用户态”

[FastDataStore::get](/home/ubuntu/miniKV/v1.0/miniKVCine/src/DataNode/FastDataStore.cpp:155)
先 `pread` 到完整 `std::string`，再由 DataNode 重算 SHA-256。下载并发提高时，每个连接都争用
磁盘、页缓存、CPU hash 和用户态内存复制，因此单文件下载速度从约 177 MiB/s 降到约 65 MiB/s。

后续可用 manifest hash 交给客户端下载校验；对已落盘的健康 extent，服务端改为流式读取或
`sendfile`。这不是现在删除 hash 校验，而是把校验责任和时机重新安排。

### 问题 4：没有统一指标，无法解释尾延迟

当前有 `pause/resume` 的功能与调试日志，但没有 `outputBuffer` 峰值、pause 总时间、
EventLoop lag、每阶段 Chunk 时间。现阶段看到四并发第三轮变慢，只能确定它是可复现的
性能现象，不能用现有证据断言唯一根因是“磁盘”或“网络”。

## 7. V2 与 V4 的边界

### V2 应补齐的观测和测试

1. 基准工具：文件并发 `1/2/4/8`，输出每轮聚合吞吐与每文件结果。
2. Histogram：Chunk、route、commit、manifest 的 P50/P95/P99。
3. Gauge/Counter：背压和 EventLoop lag 指标。
4. 本机、Tailscale、浏览器三种环境的报告分开保存。
5. 长稳测试和故障注入脚本，只在隔离环境执行。

这些改变让 V2 能准确知道瓶颈，但不改变数据面线程模型。

### V4 才处理的性能结构问题

1. 多 I/O EventLoop，连接固定归属。
2. 有界 disk worker pool，把 `pwrite`、`pread`、重 hash 从 I/O loop 移出。
3. 可控 Chunk 窗口和按节点限额。
4. 减少复制的块所有权/iovec/writev 设计。
5. extent + 流式读或 `sendfile` 下载路径。
6. 批量 metadata commit 与连接池。

## 8. 运行顺序

```text
1. 本机 A/B：先得到 CPU/内存/线程模型的基线
2. 本机 D/F：验证读写争用和背压上界
3. Tailscale A/B：分离真实网络瓶颈
4. 控制面 E：确定 Gateway 的 QPS/尾延迟
5. 长稳 G：验证不会越跑越慢或越跑越占内存
6. 根据指标决定 V4 的第一项优化，而不是先猜测
```

在 2C2G 节点上，开始时限制为 64 MiB 文件、最多 4 文件并发、总缓冲不超过既有水位线。
只有 RSS、swap、I/O wait 和 EventLoop lag 都可接受时，才尝试 8 并发或 1 GiB 长稳测试。
