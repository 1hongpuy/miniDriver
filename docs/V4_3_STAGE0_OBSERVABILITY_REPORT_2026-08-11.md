# V4.3 阶段 0：数据面观测基线（2026-08-11）

## 目标与结论

本轮为 V4 性能测试补充了可复现的结构化日志聚合，而不是直接猜测 Multi-Reactor、磁盘或
复制路径谁是瓶颈。新增 Gateway 的会话查询与文件 manifest 耗时日志，并提供
`tools/summarize_v4_observability.py`，从 Gateway/DataNode 日志产出 JSON。

在双节点、双副本、16 MiB、mixed 并发 4、3 轮的第一份样本中，12/12 文件操作成功，
48 个**主副本** Chunk 均成功。Chunk 完成 P50 为 42 ms、P95 为 77 ms；Gateway 路由和
Chunk commit 的 P95 分别为 61 µs 和 157 µs。因此此负载下控制面并不是主要时延来源。

## 本轮实现

- Gateway 记录：`upload_session_get`、`file_manifest` 的 `found` 和 `elapsed_us`。
- DataNode 的 `chunk_complete` / `chunk_failed` 记录 `role=primary|replica`。
  链式副本节点以前也会写同名完成事件；聚合器现在只将 `role=primary` 纳入客户端可见的
  Chunk 延迟，避免将每个双副本 Chunk 重复计算。
- 聚合脚本提取：控制面延迟/QPS 窗口、Chunk P50/P95/P99、SHA/pwrite/index 分段时间、
  replica 等待、复制 pipe pending/pause、磁盘队列峰值和磁盘 pause。

脚本使用方式：

```bash
python3 tools/summarize_v4_observability.py \
  --logs '<gateway-log-glob>' '<datanode-log-glob>' \
  --output /absolute/path/observability.json
```

## 环境与命令

- 构建：`RelWithDebInfo`，GCC 11.4，CMake 3.22。
- 主机：8 vCPU、约 2.8 GiB 内存；`/data` ext4。
- 部署：Gateway 1、DataNode 2、同机 loopback、链式双副本；`MINIKV_V4_IO_THREADS=2`。
- 治理：每节点上传槽 2、下载槽 8；每客户端上传槽 2、下载槽 4。
- 工作负载：16 MiB（4 × 4 MiB Chunk），mixed 总并发 4（每轮 2 上传 + 2 下载），3 轮。

原始产物：

```text
/data/minikv-v2/v4-observability-20260811/mixed-c4/runs.csv
/data/minikv-v2/v4-observability-20260811/mixed-c4/summary.csv
/data/minikv-v2/v4-observability-20260811/mixed-c4/observability.json
/data/minikv-v2/v4-observability-20260811/cluster/{gateway,node-a,node-c}/logs/
```

## 文件级结果

| mixed 并发 | 成功/请求 | 上传 P50 | 下载 P50 |
|---:|---:|---:|---:|
| 4（2 上传 + 2 下载） | 12/12 | 214.438 ms / 74.61 MiB/s | 185.799 ms / 86.11 MiB/s |

这是新的独立集群样本，不能直接取代上一份报告中多档并发的结果；它的用途是建立与
结构化指标一一对应的基线。

## Chunk 数据面指标

仅统计 `role=primary` 的 `chunk_complete`；48 个样本、0 个失败。

| 指标 | P50 | P95 | P99 | 最大值 |
|---|---:|---:|---:|---:|
| Chunk total | 42 ms | 77 ms | 144 ms | 144 ms |
| Body receive | 37 ms | 67 ms | 75 ms | 75 ms |
| Replica wait | 2 ms | 17 ms | 78 ms | 78 ms |
| Gateway chunk commit | 0 ms | 1 ms | 1 ms | 1 ms |
| SHA update | 22.043 ms | 37.313 ms | 47.205 ms | 47.205 ms |
| pwrite | 6.272 ms | 11.767 ms | 13.931 ms | 13.931 ms |
| index update | 18 µs | 39 µs | 50 µs | 50 µs |

观测窗口内主副本 Chunk 事件速率约 11.83/s。这是本次短测试的事件窗口速率，包含各轮之间
的间隔和 fixture 行为，**不是**可对外宣称的持续 QPS 上限。

## 控制面指标

`elapsed_us` 只覆盖 Gateway 内处理时间，不包括客户端到 Gateway 的网络往返或 DataNode
写入时间。所有状态均成功。

| 操作 | 样本 | P50 | P95 | 观测窗口速率 |
|---|---:|---:|---:|---:|
| 建会话 `upload_session_create` | 12 | 97 µs | 2254 µs | 3.05/s |
| 查会话 `upload_session_get` | 12 | 1 µs | 3 µs | 3.05/s |
| 分配路由 `route_plan` | 48 | 31 µs | 61 µs | 11.73/s |
| Chunk commit `chunk_commit` | 48 | 74 µs | 157 µs | 11.83/s |
| 文件 commit `file_commit` | 12 | 136 µs | 217 µs | 3.16/s |
| 文件 manifest `file_manifest` | 6 | 84 µs | 243 µs | 1.94/s |

单个 `upload_session_create` P95 受到 1 个 2.254 ms 样本影响；样本量仅 12，不能据此做
长期尾延迟结论。

## 背压与磁盘队列

| 指标 | 结果 | 解释 |
|---|---:|---|
| replica pipe `max_pending_bytes` | 0 | 此负载下副本短连接没有触发 pipe 内部待发送积压 |
| replica pipe pause 次数 | 0 | 未观察到该路径的上游暂停 |
| 磁盘队列峰值 | 1,113,847 bytes（约 1.06 MiB） | 小于 8 MiB BlockPool 总容量，但接近 pipeline 的约 1 MiB 高水位 |
| 单 Chunk 最大磁盘 pause 次数 | 6 | pause 计数是每条 Chunk pipeline 的局部指标，不能直接相加为全局次数 |
| 单 Chunk最大磁盘 pause 时长 | 51 ms | 说明并发 4 下已出现短暂的磁盘队列节流 |

当前证据更支持“SHA 与磁盘写入参与了 Chunk 成本、磁盘队列已发生短暂节流”，而不是“Gateway
控制面或副本 pipe 已饱和”。但这仍不足以量化 CPU、内核 socket 队列或 EventLoop 的影响。

## 已知缺口与下一项

本轮尚未导出：

- 每个 I/O EventLoop 的 lag、活跃连接数与跨线程投递量；
- `TcpConnection` outputBuffer 的全局/连接峰值；
- 磁盘执行器全局 `readyQueue` 峰值、BlockPool 最低可用量和全局 pause 聚合；
- 进程 CPU、RSS、上下文切换与火焰图；
- 长时间或跨机负载下的稳定性。

下一项最小实现应是为 `DiskWriteExecutor` 提供进程级快照，并在 DataNode 定时写入低频
`event=resource_snapshot`；这比先向每个 TcpConnection 加高频日志风险更低。之后将
resource snapshot 与同一 mixed/download 矩阵关联，再决定是否需要网络层 EventLoop/outputBuffer
仪表化。
