# V4.3 阶段 0：Multi-Reactor EventLoop Lag 观测报告（2026-08-11）

## 结论

本轮为每个 DataNode 的 acceptor Loop 和全部 I/O Loop 添加 1 秒 timer probe，并输出
`event=event_loop_snapshot`。在双节点、双副本、mixed 并发 4、3 轮负载下，四个 I/O Loop
的最大 timer lag 均为 **1 ms**，两个 acceptor Loop 最大为 **2 ms**。

因此，本次样本没有显示 EventLoop 被 SHA、pwrite 回调或跨线程任务持续阻塞；当前
`Body receive` 的 43 ms P50 和 66 ms P95 不能归因于“EventLoop 已经卡住”。Multi-Reactor
没有明显的调度延迟问题，但也不意味着它会自动提升吞吐：实际数据面仍主要受 SHA、复制、
内存搬运和磁盘写入影响。

## 实现

`EventLoop::Metrics` 以原子计数记录：

- `loopIterations`；
- `pendingFunctorsQueued` / `crossThreadQueued` / `pendingFunctorsExecuted`；
- timer 回调数、晚到回调数、累计与最大晚到毫秒数。

DataNode 启动 `HttpServer` 后，为 base Loop 和每个 worker Loop 注册独立 1 秒空 timer probe。
DataNode 的 base Loop 每秒读取所有 Loop 的原子快照并写入：

```text
event=event_loop_snapshot loop_index=... loop_role=acceptor|io
loop_iterations=... pending_queued=... cross_thread_queued=...
pending_executed=... timer_callbacks=... timer_late_callbacks=...
timer_lag_total_ms=... timer_lag_max_ms=...
```

这不跨线程访问 `TcpConnection` 或 HTTP context，只读取原子累计计数。

## 环境与原始产物

- 双 DataNode、Gateway 1 个、同机 loopback、链式双副本；`MINIKV_V4_IO_THREADS=2`。
- 16 MiB 文件、4 × 4 MiB Chunk；mixed 并发 4（每轮 2 上传 + 2 下载），3 轮。
- 成功：12/12 文件操作；主副本 Chunk：48/48 成功。

```text
/data/minikv-v2/v4-eventloop-20260811/mixed-c4/summary.csv
/data/minikv-v2/v4-eventloop-20260811/mixed-c4/observability.json
/data/minikv-v2/v4-eventloop-20260811/cluster/{node-a,node-c}/logs/
```

Multi-Reactor 生命周期回归：`event_loop_thread`、`event_loop_thread_pool`、
`tcp_server_multi_reactor`、`http_server_multi_reactor` 为 4/4 通过。

## Chunk 与 EventLoop 结果

| 项目 | 结果 |
|---|---:|
| 上传 P50 | 272.597 ms / 58.70 MiB/s |
| 下载 P50 | 194.959 ms / 82.07 MiB/s |
| Chunk total P50 / P95 / P99 | 51 / 71 / 73 ms |
| Body receive P50 / P95 / P99 | 43 / 66 / 71 ms |
| Replica wait P50 / P95 / P99 | 2 / 23 / 31 ms |
| I/O Loop 最大 timer lag | 1 ms |
| acceptor Loop 最大 timer lag | 2 ms |

文件吞吐在独立短样本间会波动；本报告的用途是建立 EventLoop 与 Chunk 时延关联，不能取代
完整矩阵的吞吐基线。

## 各 Loop 快照

以下为每个 Loop 的累计最大值。`pending_queued` 和 `cross_thread_queued` 是**累计任务数量**，
不是当前队列长度；不能把约 2,500--2,900 误读为队列积压。真正的队列 backlog 还需要单独的
队列深度峰值指标。

| DataNode | Loop | 角色 | 快照数 | 最大 lag | 累计跨线程投递 |
|---|---:|---|---:|---:|---:|
| bench-a | 0 | acceptor | 15 | 2 ms | 64 |
| bench-a | 1 | I/O | 15 | 1 ms | 2,895 |
| bench-a | 2 | I/O | 15 | 1 ms | 2,497 |
| bench-c | 0 | acceptor | 15 | 2 ms | 56 |
| bench-c | 1 | I/O | 15 | 1 ms | 2,660 |
| bench-c | 2 | I/O | 15 | 1 ms | 2,772 |

I/O Loop 的大量跨线程投递符合当前架构：磁盘 executor 与异步 HTTP/副本完成后，需要回到
连接所属 Loop 更新状态。四个 I/O Loop 的投递量较均衡，且没有伴随明显 timer lag，说明
当前 Multi-Reactor 的连接归属与回调投递没有显示出单个 Loop 热点。

## 指标边界

这个 timer probe 能测到“EventLoop 未能按期处理 1 秒 timer”的调度晚到，适合发现持续阻塞、
长回调、严重 CPU 争用或 I/O Loop 卡死；它有以下边界：

- 分辨率为毫秒，短于 1 ms 的抖动不可见；
- 1 秒 probe 不能构成完整 P95/P99 lag 直方图；
- 它不直接测 socket output queue，也不能指出具体是 hash、pwrite 还是某个回调占用了 CPU；
- 累计投递数不等于待执行队列深度。

所以结论应限定为：**此负载下未发现持续 EventLoop 调度延迟**，而不是“EventLoop 永远不是瓶颈”。

## 下一步

可观测性阶段的下一项是 `TcpConnection::outputBuffer` 的连接级高水位与进程级峰值，以及
pending functor 的当前/生命周期峰值深度。完成后可以区分：

```text
网络下游慢 → outputBuffer 增长 / 写事件滞后
跨线程回调积压 → pending functor 深度增长
磁盘慢 → DiskWriteExecutor / BlockPool / pause 增长
CPU/hash 成本 → 三者不高但 Body receive 仍高
```

有了这组区分证据后，才应进入副本长连接池和 SharedBodyBlock 的实现优化。
