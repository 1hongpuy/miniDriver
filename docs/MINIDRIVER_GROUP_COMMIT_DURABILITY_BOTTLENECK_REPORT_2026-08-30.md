# MiniDriver：Group Commit 持久化性能判断报告（2026-08-30）

> 结论：当前可靠写入的第一限制是 **同步持久化链路的长尾和单队列排队**，不是 SDK、SHA-256、CRC32C、Gateway
> 控制面或 CPU 饱和。本报告只覆盖同一 VM、同一 VHDX/SSD、loopback 下的诊断；不能外推为真实三主机的最终性能。

## 1. 要回答的问题

此前 `group_commit` 在 64 MiB、4 文件并发时约为 `120 MiB/s`，明显低于 buffered 写入。需要区分：

```text
是不是 2 ms 的组等待太短？
是不是 batch 没凑满？
是不是 CPU / CRC / SDK 先饱和？
是不是 LevelDB metadata sync 先饱和？
还是 fdatasync 与 durability 队列本身限制了 ACK？
```

## 2. 测试方法与边界

```text
节点：1 Gateway + 2 DataNode，RF=2，loopback
磁盘：同一台 VM 的 /data-ssd，ext4，VHDX；两个 DataNode 不具备独立物理盘
协议：opaque-chunk-id + CRC32C；SDK 上传；无上传端 SHA-256
写入：pwritev 256 KiB / 1 ms，GroupCommit 的 max pending = 64 MiB / 64 items
组参数：8 MiB / 8 items；只扫描 maxBatchDelay = 2 / 3 / 4 / 5 ms
负载：sustained-c4 = 64 MiB、4 文件并发、6 轮；mixed-c8 = 16 MiB、8 文件混合、6 轮
采样：DataNode chunk 结构化日志、vmstat 1
```

原始证据位于运行机：

```text
/data-ssd/minidriver-v3-p5-delay-diagnose-20260830/
```

扫描完成 7 个完整案例。最后一个 `mixed-c8/4ms` 在临时集群启动阶段中断，未生成结果；不能把这个缺项解释成负载失败。

## 3. 实现路径：当前哪里会等待

当前路径并非“每个网络包同步落盘”，而是：

```text
socket body
  → CRC32C + pwrite/pwritev（Disk Worker）
  → WriteSession finish
  → 一个 DataNode 级 DurabilityCoordinator 队列
  → fdatasync(dataFd)
  → 一次 LevelDB Write(sync=true) 写入该批物理 extent
  → 回调全部 batch item，允许 Chunk ACK
```

`DurabilityCoordinator` 只有一个 worker，因而对同一 `dataFd_` 的 group durable commit 是严格串行的。当前这是正确性边界：
对同一 append-only 数据文件随意并发 `fdatasync` 不会让持久化更快，且会使排序和失败语义更难解释。

它仍允许网络接收和 `pwritev` 与前一批同步阶段重叠；但 pending durable 队列有界，持续同步慢时会造成排队、背压和上传 P95 增长。

## 4. 关键结果

### 4.1 持续写：64 MiB × c4

| Group delay | 聚合吞吐 | 上传 P95 | Group queue P95 | Group commit P95 | fdatasync P95 | LevelDB P95 | Batch P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 ms | 126.3 MiB/s | 1769.9 ms | 125.9 ms | 81.9 ms | 74.8 ms | 15.1 ms | 8 MiB |
| 3 ms | 124.6 MiB/s | 1738.1 ms | 127.7 ms | 96.2 ms | 77.7 ms | 15.6 ms | 8 MiB |
| 4 ms | 125.2 MiB/s | 1712.0 ms | 126.4 ms | 103.7 ms | 77.7 ms | 12.5 ms | 8 MiB |
| 5 ms | 118.5 MiB/s | 1757.7 ms | 124.9 ms | 103.8 ms | 93.7 ms | 9.6 ms | 8 MiB |

`Group commit` 包含 data sync、LevelDB sync 和批处理本身；每一条 Chunk 日志会记录自己所属 batch 的同一个 commit 时长，
不能把同批 item 的时长相加。

### 4.2 混合负载：16 MiB × c8

| Group delay | 聚合吞吐 | 上传 P95 | Group queue P95 | Group commit P95 | fdatasync P95 | LevelDB P95 | Batch P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 ms | 221.8 MiB/s | 504.3 ms | 68.8 ms | 52.0 ms | 48.0 ms | 9.3 ms | 4 MiB |
| 3 ms | 220.9 MiB/s | 446.1 ms | 99.7 ms | 66.5 ms | 54.9 ms | 8.0 ms | 4 MiB |
| 5 ms | 208.9 MiB/s | 499.6 ms | 92.8 ms | 82.2 ms | 52.6 ms | 8.8 ms | 8 MiB |

混合负载在 2/3 ms 有时仅形成一个 4 MiB Chunk 的 batch；拉长到 5 ms 虽提高批大小，却没有提高吞吐，反而增加总等待。

### 4.3 节点级 CPU 与 I/O wait

以下是包含 benchmark、Gateway 与两个 DataNode 的整台 8 vCPU VM 平均值，不能归因到单一进程，但能判断是否 CPU 饱和：

| 持续写 delay | CPU user | CPU system | CPU idle | iowait | 运行队列 r | 阻塞任务 b |
|---:|---:|---:|---:|---:|---:|---:|
| 2 ms | 34.5% | 13.8% | 41.1% | 10.5% | 3.85 | 1.46 |
| 3 ms | 29.3% | 14.5% | 45.3% | 10.7% | 3.69 | 1.38 |
| 4 ms | 29.2% | 14.9% | 44.2% | 11.6% | 5.62 | 1.46 |
| 5 ms | 30.9% | 14.4% | 43.9% | 10.7% | 5.07 | 1.29 |

CPU 仍有约 41%～45% idle，且 `fdatasync` 长尾与 iowait/阻塞任务同时出现。因此 CPU 不足不是当前第一限制；
`vmstat` 也不能独自证明是哪一个进程在等待，后续必须加入 `pidstat` 与 `iostat -x`。

## 5. 已排除、已确认与尚未确认

### 已排除为第一瓶颈

```text
上传端 SHA-256：默认路径是 CRC32C，SDK 不预哈希。
CRC32C / CPU 饱和：VM 仍有大量 idle CPU；不是第一限制。
2 ms group delay 太短：持续写的 8 MiB batch 已经凑满；3/4 ms 无吞吐收益。
LevelDB sync：P95 约 8～16 ms，明显低于 data fdatasync 的 48～94 ms。
Gateway route / commit：既有测量为微秒级，不足以解释 100 ms 级 durable wait。
```

### 已确认的限制

```text
1. fdatasync(dataFd) 长尾：持续写 P95 约 75～94 ms。
2. 单 DurabilityCoordinator 串行提交：durability queue P95 约 125 ms。
3. 两 DataNode 共享同一 VM/VHDX：RF=2 测的是双副本协议，不是两块独立盘的真实扩展性。
4. 大对象包含多个 4 MiB Chunk；每个 Chunk 的 durable barrier 等待会反映为文件级约 1.7 s P95。
```

### 尚不能确认

```text
真实裸 SSD / NVMe 或三台独立机器上的 fdatasync 长尾。
宿主机 VHDX 缓存、虚拟化调度与 guest ext4 各自的占比。
不同 group byte 阈值能否显著提升 durable throughput。
多 data shard 在真正独立磁盘上是否能扩大并行 durable capacity。
掉电、VM crash、文件系统崩溃后的完整 durable 语义。
```

## 6. 工程判断

当前 `group_commit` 不是“坏实现”，而是已经做到了正确的基本模型：

```text
pwritev 与前一批 durability sync 可重叠；
多个 Chunk 共用一次 fdatasync；
data 先 durable，再同步发布 LevelDB 物理索引；
pending bytes/items 有上限，避免无界内存。
```

但其可扩展性当前受单 data file / 单 durable coordinator 以及 VM 同盘设备上限约束。不能通过给同一个 `dataFd` 增加多个
并发 `fdatasync` 线程解决；这通常只会增加抖动。

## 7. 推荐的下一步

### D1：先补齐观察，不先重写

在同一诊断脚本中增加：

```text
pidstat -dur 1：gateway、每个 DataNode、benchmark 的 CPU/RSS/上下文切换/block I/O；
iostat -x 1：/dev/sdc 的 util、await、aqu-sz、写 IOPS；
应用指标：每次 fdatasync 时长、每批 bytes/items、每批 data/index sync、队列长度和高水位停顿。
```

### D2：只扫描 group bytes，不扫描更长 delay

保持 `maxBatchDelay=2 ms`，比较：

```text
maxBatchBytes = 8 / 16 / 32 MiB
maxBatchItems = 足以覆盖对应字节数
文件并发 = 1 / 2 / 4 / 8
```

验收指标是 durable throughput、`fdatasync` 次数/GiB、batch fill、P95/P99 和 pending 队列，而不是单一峰值。

### D3：确认硬件边界后再做数据 shard

仅当 D2 显示单个 data file 的同步队列持续饱和，并且部署确实有独立物理 SSD/NVMe 时，再设计：

```text
disk0.data + durable coordinator 0
disk1.data + durable coordinator 1
...
```

Chunk 在创建时固定 shard；每 shard 内仍保持顺序 durable commit。不要在同一 VHDX 上用多个 shard 伪造磁盘并行。

## 8. 当前可用结论

```text
buffered：适合可接受异步 flush 的高吞吐导入；不是掉电 durable ACK。
group_commit：当前可靠写路径候选；开发机持续 c4 约 118～126 MiB/s，文件 P95 约 1.7 s。
chunk_sync：需要与 group_commit 在同一长时、同一硬件矩阵中重新比较，不能只用单轮数字判优。
默认调度：在当前开发机上，RF=2 的前台 durable upload 保守使用 c4；更高并发主要增加队列和尾延迟。
```

## 9. 与其他文档的关系

- [3.0 SDK 性能与 MinIO 对照](MINIDRIVER_3_0_SDK_PERFORMANCE_AND_MINIO_COMPARISON_REPORT_2026-08-29.md)：SDK、读路径和同日单盘对照；
- [高并发与高性能实施方案](MINIDRIVER_HIGH_CONCURRENCY_AND_PERFORMANCE_IMPLEMENTATION_PLAN_2026-08-25.md)：数据面长期设计与实施阶段；
- 本文：专门解释 durable ACK 为什么比 buffered 慢，以及下一步如何用证据确定优化方向。
- [V3.0 可靠写入流水线优化设计](MINIDRIVER_3_0_DURABLE_WRITE_PIPELINE_OPTIMIZATION_PLAN_2026-08-30.md)：本判断对应的实现边界、配置、任务与验收条件。
