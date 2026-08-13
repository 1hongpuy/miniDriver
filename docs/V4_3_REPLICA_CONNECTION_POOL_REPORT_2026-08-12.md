# V4.3 副本 HTTP 长连接池：实现与性能验证（2026-08-12）

## 结论

本轮已将 DataNode 的链式副本转发从“每个请求创建并关闭一个
`AsyncHttpRequest`/`TcpClient`”改为优先使用有界 HTTP/1.1 Keep-Alive 会话。实现没有改变
Chunk SHA-256、写入准入、双副本确认或 Gateway commit 语义；池不可借、失效或启动失败时，仍安全
回退到原短连接路径。

在双 DataNode、双副本、16 MiB、`MINIKV_V4_IO_THREADS=2`、真实 `/data` loopback 环境下：

- upload-only 并发 1 的文件 P50 为 **330.589 ms / 48.40 MiB/s**；并发 2 为
  **253.725 ms / 63.06 MiB/s**，均 100% 成功。
- 相对于 V4.2 同口径历史基线（344.622 ms、277.360 ms），P50 分别下降约 **4.1%** 与
  **8.5%**。样本仅各 3/6 个，不应把该差异宣称为严格因果收益。
- 新的真实复制日志至少记录到一次连接创建后一次同目标会话复用，证明生产复制路径已实际使用
  Keep-Alive，而非只停留在单元测试。
- 混合负载也全部完成：concurrency 2 为 6/6，concurrency 4 为 12/12。

长连接主要目标是副本建立/关闭的固定开销及 P95/P99；在本机 loopback 下，Body/SHA 仍是主要
稳定成本，因此它不是吞吐翻倍的机制。

## 实现内容

### 1. 持久 HTTP 会话

新增 `PersistentHttpSession`：一个会话拥有一个 `TcpClient` 和一个 `TcpConnection`，但同一时刻
只执行一个 HTTP/1.1 请求，不做 pipelining。

```text
Idle → Connecting → WritingBody → WaitingResponse → Idle
                                      │
                         timeout / EOF / 协议错 / cancel
                                      ↓
                                    Closed
```

- 请求发送 `Connection: keep-alive`；响应必须有明确且合法的 `Content-Length`。
- Body 字节数必须恰等于请求 `Content-Length`。
- 收到完整响应且对端未声明 `Connection: close` 时，清理所有请求态后转回 `Idle`。
- 连接关闭、超时、解析错误、取消、Body 长度错误都会关闭会话，避免把不确定的响应边界交给下一请求。

原 `AsyncHttpRequest` 保持一次性短连接语义，仍作为回退实现被其他模块继续使用。

### 2. EventLoop 分片连接池

新增 `ReplicaConnectionPool`，键为：

```text
nodeId + address + port + owner EventLoop
```

每个 pool 只在它的 owner EventLoop 中操作；不会把 Loop A 创建的 `TcpConnection` 交给 Loop B。
默认每目标最多 2 条、每 shard 最多 4 条会话。一条会话被借出期间不可再次借出；完成且健康时归还
idle 队列，错误或非 200 副本确认时 discard 并关闭。

```text
ChunkUploadStream（属于 I/O Loop 1）
  → Loop 1 的 ReplicaConnectionPool
  → key(bench-c, 127.0.0.1, 19423)
  → idle PersistentHttpSession 或新会话
  → 完整副本响应后 release / 异常时 discard
```

当池已满或不可用，`ReplicaUploadPipe` 会启动已有 `AsyncHttpRequest` 短连接 fallback；不会建立
无限等待队列，也不会突破现有 `pendingBlocks_` 与 `pauseRead()` 背压上限。

### 3. 生命周期与日志

- DataNode 在 `server.start()` 后为 base/I/O EventLoop 建立 pool shard；实际上传连接只会命中
  其所属 I/O Loop 的 shard。
- 退出 EventLoop 后关闭该 DataNode 的所有 pool 会话。
- 新增低基数结构化事件：`replica_connection_created`、`replica_connection_reused`、
  `replica_pool_borrow_rejected`、`replica_connection_discarded`。

## 测试

新增：

| 测试 | 覆盖 |
|---|---|
| `persistent_http_session` | 同一 loopback TCP 连接顺序执行两个 PUT；服务端 accept=1 |
| `replica_connection_pool` | 每目标/全局容量、同一 session 不重复借出、归还后复用、discard、shutdown 指标 |

相关回归通过：`persistent_http_session`、`replica_connection_pool`、
`replica_upload_metrics`、`chunk_disk_write_pipeline`。

完整 CTest：**47/48 通过**。唯一失败仍为 UI 测试缺少 Python `playwright`，与本次数据面无关。

## 性能方法

- 构建：`cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo`，`cmake --build build -j2`。
- 主机：8 vCPU、约 2.8 GiB 内存，`/data` ext4；同机 loopback。
- 部署：Gateway 1、DataNode 2、链式双副本；每节点上传槽 2、下载槽 8；I/O worker Loop=2。
- 对象：16 MiB（4 MiB Chunk）；每档 3 轮。
- 原始结果：`/data/minikv-v2/v4-pool-20260812/`。

命令形式：

```bash
MINIKV_V4_IO_THREADS=2 tools/start_v2_local_benchmark_cluster.sh
./build/bin/minikv_v2_bench local --gateway 127.0.0.1:<port> \
  --work-dir /data/minikv-v2/v4-pool-20260812/<case> \
  --sizes 16MiB --runs 3 --concurrency <n> --mode <upload|mixed>
```

## 文件级结果

P50/P95 均为成功文件的完成时间；MiB/s 按 `16 MiB / P50` 换算。

| 模式 | 文件并发 | 成功/请求 | 上传 P50 / P95 | 下载 P50 / P95 |
|---|---:|---:|---:|---:|
| upload-only | 1 | 3/3 | 330.589 / 380.050 ms；48.40 MiB/s | — |
| upload-only | 2 | 6/6 | 253.725 / 288.566 ms；63.06 MiB/s | — |
| mixed | 2（1U+1D） | 6/6 | 201.477 / 311.842 ms；79.41 MiB/s | 186.506 / 198.253 ms；85.79 MiB/s |
| mixed | 4（2U+2D） | 12/12 | 217.913 / 264.597 ms；73.42 MiB/s | 213.476 / 268.464 ms；74.95 MiB/s |

与历史 V4.2 双节点 `io_threads=2` upload-only 基线的比较：

| 并发 | 历史 P50 | 本轮 P50 | 差异 |
|---:|---:|---:|---:|
| 1 | 344.622 ms | 330.589 ms | -4.1% |
| 2 | 277.360 ms | 253.725 ms | -8.5% |

两轮均为小样本且共享同一台机器，受页缓存、CPU 频率、后台进程影响。表格只能说明本轮没有性能
回归、存在正向信号；不能独立证明全部改善都由连接池造成。

## 数据面与连接复用证据

全矩阵 108 个主副本 Chunk：失败 0。

| 指标 | P50 | P95 | P99 |
|---|---:|---:|---:|
| Chunk total | 43 ms | 67 ms | 70 ms |
| Body receive | 37 ms | 64 ms | 70 ms |
| SHA update | 24.916 ms | 40.386 ms | 44.911 ms |
| pwrite | 4.980 ms | 11.273 ms | 12.226 ms |
| Replica wait | 0 ms | 21 ms | 29 ms |

独立复用证明运行位于：

```text
/data/minikv-v2/v4-pool-20260812/cluster-reuse-proof/node-a/logs/datanode-bench-a.log
```

其中实际出现两个 `replica_connection_created` 和一个
`replica_connection_reused`。该 16 MiB 输入有 Chunk 去重，且不同 I/O Loop 使用不同 shard，
因此“客户端的四个 Chunk”不与“某一 shard 的副本请求数”一一对应。不能期待每个文件固定只有一次
创建、三次复用；但该日志已经直接证明同一 `nodeId/address/port/Loop` 的会话被归还并复借。

资源快照仍显示：BlockPool 生命周期峰值约 3.81 MiB，磁盘 ready queue 峰值 2 tasks，I/O Loop
timer lag 最大 2 ms。连接池没有使网络/磁盘队列失控。

## 当前限制与下一步

本次实现是可验证的最小连接池，尚未具备以下产品级策略：

- idle TTL sweep、按目标失败指数退避和半开探测；
- 跨 DataNode、真实网络 RTT/丢包下的 P95/P99 矩阵；
- pool 指标进入每秒 `resource_snapshot` 的聚合计数；
- HTTP pipelining（本轮明确不实现）。

下一步不应继续增加 I/O Loop 或池大小。优先做 **SharedBodyBlock**：在固定上限下使一次网络输入的
Block 同时供 SHA、磁盘写和副本发送使用，减少 `inputBuffer → BlockPool → 副本 pending` 的重复复制；
随后才以 BlockPool、磁盘队列、连接池预算为约束测试 `Chunk window=2`。
