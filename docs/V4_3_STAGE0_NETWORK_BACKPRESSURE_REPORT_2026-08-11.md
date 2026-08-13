# V4.3 阶段 0：网络输出缓冲与回调队列观测报告（2026-08-11）

## 结论

本轮补齐 `TcpConnection::outputBuffer` 与 EventLoop pending functor 深度观测。在双节点、
双副本、mixed 并发 4、3 轮测试中：

- DataNode 进程级 `outputBuffer` 生命周期峰值为 **0 bytes**，高水位事件为 **0**；
- 四个 I/O Loop 的 pending functor 生命周期峰值均为 **2**；
- I/O Loop timer lag 最大 1 ms，acceptor 最大 2 ms；
- mixed 负载 12/12 成功。

这不是“网络没有传数据”，而是下载使用 `sendfile`，文件内容直接在内核文件页缓存与 socket
缓冲之间传输，不进入 `TcpConnection::outputBuffer`；上传为入站数据，也不使用该输出缓冲。
因此本样本没有发现 DataNode 用户态输出队列或跨线程回调队列导致背压。

## 实现

`TcpConnection` 增加线程安全的输出统计：当前缓冲字节、连接生命周期峰值、高水位跨越次数。
每个 `TcpServer` 给所有连接共享一份进程级原子统计；只有在 `sendInLoop` 写不完、数据进入
`outputBuffer_` 时增加，`handleWrite` 消费或连接关闭时减少。

`EventLoop` 增加当前 pending functor 深度及生命周期峰值。它与累计 `pending_queued` 分开：

```text
累计投递高，不等于队列积压。
只有 pending_peak_depth 高，才说明某段时间回调来不及消费。
```

DataNode 每秒的 `resource_snapshot` 现包含：

```text
output_buffer_current_bytes=...
output_buffer_peak_bytes=...
output_buffer_high_water_events=...
```

每个 `event_loop_snapshot` 包含 `pending_depth` 与 `pending_peak_depth`。

## 环境与原始结果

- Gateway 1、DataNode 2、同机 loopback、链式双副本；`MINIKV_V4_IO_THREADS=2`。
- 16 MiB 文件、4 × 4 MiB Chunk；mixed 并发 4（2 上传 + 2 下载），3 轮。
- 原始 JSON：`/data/minikv-v2/v4-output-20260811/mixed-c4/observability.json`。

| 指标 | 结果 |
|---|---:|
| 文件成功/请求 | 12/12 |
| DataNode outputBuffer 当前最大值 | 0 bytes |
| DataNode outputBuffer 生命周期峰值 | 0 bytes |
| outputBuffer 高水位事件 | 0 |
| 每 Loop pending 深度采样最大值 | 0 |
| 每 Loop pending 生命周期峰值 | 2 |
| I/O Loop 最大 timer lag | 1 ms |
| acceptor Loop 最大 timer lag | 2 ms |
| BlockPool 生命周期峰值 | 3.81 MiB / 8 MiB |
| Disk executor 生命周期峰值队列 | 2 tasks |

I/O Loop 累计跨线程投递约 2,430--2,625 次，分布均衡。它们主要来自磁盘执行器与异步网络
完成回到连接所属 Loop；峰值深度仅 2，说明这些任务被及时消费。

## 边界与优化判断

当前结论只适用于本地 loopback、sendfile 下载和 16 MiB mixed 负载：

- 慢客户端、跨机网络或服务端主动 `send()` 大响应时，`outputBuffer` 仍可能增长；需另设限速
  客户端测试验证高水位回调。
- sendfile 的内核 socket 缓冲、NIC 队列不在该指标中；`outputBuffer=0` 不代表网络链路没有排队。
- pending peak=2 不代表任意负载下都不会积压，只证明当前样本没有。

综合阶段 0 的证据，当前不建议继续增加 EventLoop 数、BlockPool 或磁盘 worker。下一步应
进入有明确收益假设的实现优化：优先验证副本长连接池对高并发/跨机尾延迟的影响，随后实现
有界 `SharedBodyBlock` 以减少本地写入与副本转发的重复数据复制。
