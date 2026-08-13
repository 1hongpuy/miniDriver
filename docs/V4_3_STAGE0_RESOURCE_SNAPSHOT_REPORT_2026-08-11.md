# V4.3 阶段 0：DiskWriteExecutor / BlockPool 资源快照报告（2026-08-11）

## 结论

本轮为 DataNode 增加低频进程级 `event=resource_snapshot`，并在相同的双节点 mixed
并发 4 负载下采集 30 个快照。结论是：当前 8 MiB BlockPool 没有耗尽，但写入过程存在
短时 burst 和局部磁盘背压。

- 生命周期峰值租出 **4,390,912 bytes**，约 4.19 MiB，即约 **67/128 块**；仍保留约一半池容量。
- 1 秒快照中最低可用块数为 **110/128**、瞬时在用最大 1.125 MiB；这与上述 4.19 MiB
  生命周期峰值不同，说明 burst 持续时间小于采样间隔，不能只看瞬时采样。
- 执行器生命周期峰值 ready queue 为 **2 个任务**；采样时队列刚好为 0，进一步证明只有
  快照而没有峰值计数会漏掉短暂积压。
- 单个 Chunk pipeline 已出现最多 6 次、最长 48 ms 的磁盘 pause，局部队列峰值约 1.06 MiB。

因此，当前 Body receive 的 P95 抖动不能归因于“BlockPool 不够”或“磁盘 worker 持续满载”。
更准确的描述是：SHA-256 和 pwrite 是稳定成本，有限并发下仍会出现毫秒级的短时任务/磁盘
积压。下一阶段应优先减少复制和优化副本尾延迟，而不是先扩大 BlockPool 或盲目增加 worker。

## 实现内容

`DiskWriteExecutor::Metrics` 新增：

- `availableBlocks`、`totalBlocks`；
- 当前与生命周期峰值 `queuedTasks`；
- 当前与总 worker 数、`completedTasks`；
- 已有的当前与生命周期峰值 leased bytes。

DataNode 每秒写入一次低基数日志：

```text
event=resource_snapshot active_uploads=... active_downloads=...
disk_queued_tasks=... disk_queue_peak_tasks=...
block_available=... block_total=... block_leased_bytes=...
block_peak_leased_bytes=... disk_active_workers=...
```

`tools/summarize_v4_observability.py` 同时输出 snapshot 的瞬时最大值/最小值和 executor
生命周期峰值。两类指标必须并列解释。

## 环境与原始产物

- 双 DataNode、链式双副本、Gateway 1 个、同机 loopback；`MINIKV_V4_IO_THREADS=2`。
- 每节点上传槽 2、下载槽 8；16 MiB 文件、4 个 4 MiB Chunk。
- mixed 总并发 4：每轮 2 上传 + 2 下载，3 轮；成功 12/12。

```text
/data/minikv-v2/v4-resource-20260811/mixed-c4/runs.csv
/data/minikv-v2/v4-resource-20260811/mixed-c4/summary.csv
/data/minikv-v2/v4-resource-20260811/mixed-c4/observability.json
/data/minikv-v2/v4-resource-20260811/cluster/{gateway,node-a,node-c}/logs/
```

相关测试：`disk_write_executor`、`chunk_disk_write_pipeline`、`node_resource_governor`、
`replica_upload_metrics` 均通过。

## 文件与 Chunk 结果

| 项目 | 结果 |
|---|---:|
| mixed 文件成功/请求 | 12/12 |
| 上传 P50 | 229.855 ms / 69.61 MiB/s |
| 下载 P50 | 177.540 ms / 90.12 MiB/s |
| 主副本 Chunk 数/失败数 | 48 / 0 |
| Chunk total P50 / P95 / P99 | 44 / 66 / 70 ms |
| Body receive P50 / P95 / P99 | 39 / 62 / 67 ms |
| Replica wait P50 / P95 / P99 | 0 / 15 / 26 ms |
| SHA update P50 / P95 | 25.251 / 37.591 ms |
| pwrite P50 / P95 | 7.010 / 10.929 ms |

本轮独立样本与上一轮有正常波动；它的重点是资源快照关联，不用于替代全矩阵基线。

## 进程级资源指标

快照值跨两个 DataNode 聚合：`max` 表示任一节点的最大值，`min_available_blocks` 表示任一
节点观测到的最低可用块数。因此 `max_active_uploads=1` 是单节点值；双副本的两条上传流在
两个节点上分布，不代表集群只有一个上传。

| 指标 | 结果 | 正确解释 |
|---|---:|---|
| resource snapshot 数 | 30 | 两节点进程的 1 秒低频观测总数 |
| 瞬时最小可用 Block | 110 / 128 | 采样点看到的最低可用量；不是全过程低水位 |
| 瞬时最大 leased bytes | 1.125 MiB | 采样点的最大在用内存 |
| 生命周期峰值 leased bytes | 4.19 MiB | executor 内部精确峰值，约 67 块；比快照更可信 |
| ready queue 瞬时最大 | 0 | 1 秒采样没有撞上短暂积压 |
| ready queue 生命周期峰值 | 2 tasks | 已发生但很短的执行器排队 |
| 快照中最大 active worker | 1 / 2 | 未证实 worker 持续饱和；短时满载仍可能被 1 秒采样漏掉 |
| 每节点已完成任务最大值 | 4,992 | 累计计数，说明磁盘任务粒度较细，不等于队列深度 |

## 与局部 Chunk 背压的关系

Chunk 日志报告的是每个 Chunk pipeline 的局部情况：

| 指标 | 结果 |
|---|---:|
| `disk_queue_peak_bytes` | 1,113,680 bytes（约 1.06 MiB） |
| 单个 pipeline 最大 `disk_pause_count` | 6 |
| 单个 pipeline 最大 `disk_pause_ms` | 48 ms |
| replica `max_pending_bytes` | 0 |

这与进程级结果并不矛盾：单个 pipeline 到达高水位时会暂停上游，随后任务很快被 worker 消化；
因此可以有 pause 与精确生命周期峰值，却在 1 秒采样时看到 queue=0。

## 当前可作出的优化判断

1. **不扩大 BlockPool。** 当前峰值约使用一半容量，扩大池只会容忍更多突发，不会减少
   SHA/pwrite 的固定耗时，也可能增加尾延迟和 RSS。
2. **不先增加 DiskWriteExecutor worker。** 当前没有“持续两个 worker 都满、队列长期增长”的证据；
   增 worker 可能增加同一磁盘上的竞争。应先采集更细粒度 worker busy 时间，或比较 1/2/3 worker 的受控实验。
3. **优先减少 Body 数据的重复复制。** SHA update 仍是 Body receive 的最大稳定分量；共享一份
   有界 BodyBlock 给本地写入与副本转发，比放大队列更有针对性。
4. **副本长连接池仍有价值，但主要改善尾部。** 本样本 Replica P99 已降至 26 ms，不能把它
   当成当前 P50 的主瓶颈；是否实现应以更高并发/跨机实验的 P95/P99 为依据。

## 仍未完成的观测

- EventLoop lag、每 Loop 活跃连接数与跨线程投递；
- `TcpConnection::outputBuffer` 的连接/进程峰值；
- worker busy 时间的高精度直方图；
- CPU、RSS、上下文切换和火焰图。

下一项若继续可观测性，应优先增加 **EventLoop lag 的低频直方图**，但必须覆盖每个 I/O
EventLoop，而不仅是主 accept loop；否则它不能解释 Multi-Reactor 的表现。
