# MiniDriver 3.0：可靠写入流水线优化设计与实施计划（2026-08-30）

> 状态：P7.1、P7.2、P7.3 已在开发 SSD 完成；真实三主机与故障恢复仍未完成。当前已有 `pwritev`、有界
> Disk Worker、`group_commit` 和一个 DataNode 级 `DurabilityCoordinator`；本计划完善其阶段隔离、批量策略、
> 观测与验证，不把未实现能力写成现状。

> 目标：在**不降低 durable ACK 语义**的前提下，让网络接收、CRC32C、`pwritev` 与 durable sync 尽可能重叠，
> 以有界内存换取更少的同步次数和更稳定的吞吐/P95。它不承诺消除底层 `fdatasync` 的物理延迟。

## 1. 背景与问题判断

当前开发机的 `RF=2 + group_commit` 写入受同步持久化限制：持续写 64 MiB、c4 时，`fdatasync` P95 约
`75–94 ms`、durability queue P95 约 `125 ms`，文件上传 P95 约 `1.7 s`。整台 8 vCPU VM 仍有约
`41–45%` idle CPU，故 CPU、SDK、上传端 SHA-256、Gateway 控制面和 2 ms group deadline 不是第一瓶颈。

详细证据见
[Group Commit 持久化性能判断报告](MINIDRIVER_GROUP_COMMIT_DURABILITY_BOTTLENECK_REPORT_2026-08-30.md)。

当前在一台 VM 的同一 VHDX 上运行两个 DataNode；它验证 RF=2 协议，不代表两块独立 SSD 的分布式写扩展性。

## 2. 目标与非目标

### 2.1 V3.0 目标

```text
1. Chunk Body 在写入 page cache 后可进入有界 durable pending 队列；
2. 当前 batch fdatasync 时，Disk Worker 仍能继续处理后续 pwritev；
3. durable ACK 仅在 data sync 成功、物理索引 sync 成功后返回；
4. pending 到达上限时连续背压到 socket/client，而不是无界占用内存；
5. 用实测选择 8/16/32 MiB batch；只在证据支持后引入自适应策略；
6. 能把文件 P95 上升归因到写入、batch formation、fdatasync、index sync 或队列等待。
```

### 2.2 非目标

```text
不对同一个 dataFd 并发执行多个 fdatasync；
不将 buffered ACK 伪装为 durable ACK；
不把完整 Chunk Body 长期保存在 durable 队列；
不因吞吐优化取消 CRC32C、副本确认或 data-before-index 顺序；
不在当前同一 VHDX 环境中宣称 multi-shard 一定提升性能；
不在没有独立盘证据时提前实现复杂多盘调度或 io_uring。
```

## 3. 正确性语义

每个 Chunk 的状态机：

```text
RECEIVING
  → WRITE_QUEUED
  → WRITE_READY                 # pwrite/pwritev 成功，可能仍仅在 page cache
  → DURABILITY_PENDING
  → DATA_DURABLE                # fdatasync(dataFd) 成功覆盖该 extent
  → INDEX_PUBLISHED             # LevelDB sync=true 发布物理 extent
  → ACKED

任意阶段失败 / abort
  → FAILED
  → 不发布可读 physical index
```

必须保持的顺序：

```text
data bytes durable
  before
physical extent index durable / visible
  before
Gateway Chunk Commit / client success ACK
```

重试必须复用同一 `sessionId + chunkIndex + chunkId + generation`；不得因排队、超时或重连生成第二个逻辑 Chunk。

## 4. 目标流水线

```text
                    ┌──────────────────────┐
HTTP / Replica Body →│ Disk Worker Pool     │
                    │ CRC32C + pwritev     │
                    └─────────┬────────────┘
                              │ WRITE_READY：只传 extent + checksum + callback
                              ▼
                    ┌──────────────────────┐
                    │ Durable Pending Queue│  bytes/items 有界
                    └─────────┬────────────┘
                              │ batch threshold / deadline / shutdown drain
                              ▼
                    ┌──────────────────────┐
                    │ Durability Sequencer │  每个 data shard 一个，顺序执行
                    │ fdatasync(dataFd)    │
                    │ LevelDB batch sync   │
                    └─────────┬────────────┘
                              ▼
                         ACK / Gateway Commit
```

关键并行关系：

```text
Batch N：DurabilitySequencer 正在 fdatasync
Batch N+1：Disk Worker 仍在 CRC32C + pwritev
Batch N+2：网络可继续接收，直到 pending 高水位触发背压
```

这里的“并行”不表示多个线程同时 sync 同一文件；它是将 CPU/网络/普通写与同步 barrier 重叠。

## 5. 队列、背压与公平性

### 5.1 队列只保存小型描述符

`DURABILITY_PENDING` 中保存：

```text
storageKey / identityScheme / chunkId / extent(offset,length)
checksum result / write sequence / callback / session identity
```

不保存完整 Chunk Body；Body Block 在 `pwritev` 和副本引用全部释放后必须归还 BlockPool。

### 5.2 高低水位

建议配置分为：

```text
batch target：        8 / 16 / 32 MiB（实验值）
batch item target：   覆盖上述字节数的最大 Chunk 数
batch deadline：      初始固定 2 ms
pending high water：  64 MiB / 64 items（现有边界，需实测）
pending low water：   high water 的约 50%～75%
```

达到 high water：

```text
Disk pipeline / replica slow
  → pauseRead()
  → TCP receive window 反压上游
  → client 的 chunk-window/global budget 自然受限
```

降到 low water 才 `resumeRead()`，避免在阈值附近频繁抖动。前台不同上传 session 领取 Chunk 时必须公平轮转，
不能让一个大文件长期占满所有 durable pending slots。

## 6. 批量策略

### 6.1 第一版：固定阈值实验

先保持 `maxBatchDelay=2 ms`，仅改变 `maxBatchBytes`：

```text
8 MiB / 16 MiB / 32 MiB
```

原因：当前 8 MiB 在持续写中已接近满 batch，扩大字节阈值比把等待从 2 ms 拉长到 5 ms 更值得验证。

估算只作为起点，不能替代测试：

```text
required pending bytes ≈ write-ready rate × fdatasync P95
≈ 126 MiB/s × 0.094 s ≈ 12 MiB
```

因此 16 MiB 是首个合理候选；32 MiB 是抖动余量候选，不是预设默认值。实际矩阵结果及最终开发默认结论见
[P7 durable-write 矩阵报告](MINIDRIVER_P7_DURABLE_WRITE_MATRIX_REPORT_2026-08-30.md)。

### 6.2 第二版：仅在固定实验成功后自适应

可选策略：

```text
targetBatchBytes = clamp(
  writeReadyRateEWMA × fdatasyncP95 × safetyFactor,
  8 MiB,
  32 MiB)
```

其中 `safetyFactor` 先取 1.5 左右；低负载仍受 2 ms deadline 保护，避免交互小对象为攒大 batch 长时间等待。

自适应参数变更必须限速、记录原因，并且具有固定阈值回退开关。

## 7. 线程模型

| 角色 | 当前/目标 | 可以并行吗 | 说明 |
|---|---|---:|---|
| 网络 Reactor | 多 EventLoop | 是 | 只负责 socket 与背压，不做同步 I/O。 |
| Disk Worker | 先测 2 / 4 / 8 | 是 | CRC32C、聚合 `pwritev`；不能无限增加。 |
| Durability Sequencer | 每 data shard 1 个 | shard 间可 | 单 shard 内保持 `fdatasync` 和索引发布顺序。 |
| LevelDB 发布 | durable batch 内一次 | 否（同 shard） | data durable 后 `Write(sync=true)`。 |
| 副本 pipe | 有界 | 是 | 不得绕过本地/远端 pending 高水位。 |

CPU 未满只说明可测试更多 Disk Worker 或更深 pipeline；它不能让同一个 `fdatasync` 更快。

## 8. 多 shard 的后续设计

本项不是当前 V3.0 默认实现，前提是同一 DataNode 确实拥有多块独立物理 SSD/NVMe：

```text
Chunk placement
  → shardId 固定于 ChunkRecord

shard 0: disk0.data + index namespace + coordinator 0 → SSD 0
shard 1: disk1.data + index namespace + coordinator 1 → SSD 1
```

每个 shard 独立 pending queue 和 durable sequencer，因此独立设备可以并行 sync。读写、删除、重启回收和 repair 都必须按
`shardId` 查找；不得事后随机迁移 extent。当前同一 VHDX 的两 DataNode 不满足此优化的前提。

## 9. 可观测性要求

### 9.1 应用指标

每个 Chunk / batch 至少记录：

```text
write_ready_wait_us
durability_queue_wait_us
batch_formation_wait_us
fdatasync_us
index_sync_us
batch_commit_us
batch_bytes / batch_items
pending_bytes / pending_items / high-water pause count
dataSync operations per GiB
```

### 9.2 系统指标

每次 durable matrix 同时保存：

```text
pidstat -dur 1      # Gateway、每 DataNode、benchmark 的 CPU/RSS/context switch/block I/O
iostat -x 1         # util、await、aqu-sz、w/s、wMB/s
vmstat 1            # r/b、CPU user/system/idle/iowait
```

这些证据必须同 workload、配置、磁盘挂载信息一起保存，不能只留一行峰值吞吐。

## 10. 实施阶段

### P7.1：观测补齐

- [x] 把 `group_wait_us` 拆成每请求 queue wait，以及单独的 batch formation、data sync、index sync 事件；
- [x] 增加 batch summary、pending 水位、同步期间 `pwrite` 数/字节和运行时 metrics dump；
- [x] benchmark 脚本在宿主机具备 `sysstat` 时保存 `pidstat -dur 1` 与 `iostat -x 1`，缺少工具时显式写入 unavailable 证据；
- [x] 加入确定性回归：人为延长一个 `fdatasync`，并确认另一请求的 `pwrite` 在同步期间完成且被计数；
- [x] 用 `/tmp` 非发布 smoke 采集首批运行时日志：已出现 batch 分段事件与同步期间 pwrite 计数；证据在 `/tmp/minidriver-v3-p7-durable-write-smoke-20260830a/`。
- [x] 用可写目标 SSD 的 P7 真实矩阵复核 batch 分位数与重叠量；所有 case 成功，且同步期间持续有后续 `pwrite` 完成。

**当前验证：** `test_fast_data_store_durability` 已覆盖指标与同步/pwrite 重叠；真实 SSD 矩阵已给出 P95 的写入、
batch formation、data sync、index sync 分解。`pidstat`/`iostat` 未安装，故本轮只有 `vmstat` 系统级 CPU/iowait，
没有每进程 CPU/RSS 或设备 await/util。

### P7.2：固定 batch-size 矩阵

- [x] 提供固定 2 ms、8/16/32 MiB、c1/c2/c4/c8 sustained 与 mixed c8 的隔离矩阵脚本；
- [x] 在 `/tmp` 完成一个 8 MiB、16 MiB c2 的非发布 smoke，验证上传成功、batch event、observability 汇总和环境标记；
- [x] 在可写目标 SSD 上实际比较 8/16/32 MiB；
- [x] 每档运行 c1/c2/c4/c8 的 64 MiB sustained upload，5 轮；
- [x] 保存 P50/P95/P99、throughput、success、queue、batch/sync 分解和系统级 `vmstat`；
- [x] 每档运行 16 MiB mixed c8，验证有界 pending 下的混合读写；
- [x] 选择开发默认：保持 `8 MiB / 2 ms / 2 Disk Worker`。它在 sustained c1/c4/c8 的上传 P95 最稳；32 MiB
  只在本轮 mixed c8 更好，尚不足以改写通用默认。

**验收结果：** 没有证据支持将通用默认从 8 MiB 改为 16/32 MiB；保持当前 8 MiB 回退路径。32 MiB 保留为
混合负载实验候选，而不是自动自适应结论。

### P7.3：Disk Worker 容量矩阵

- [x] 在 8 MiB 候选默认下比较 Disk Worker 2/4/8；
- [x] 未与 batch-size 改动混测；
- [x] 记录 `pwritev`、queue、`fdatasync` 分位数和系统级 CPU/iowait；缺少 `sysstat`，已留下 unavailable 证据；
- [x] 保持 2 Worker 默认：8 Worker 对 c4/mixed 有收益，但 c8 无稳定 aggregate 收益且 `pwrite` P95 明显上升。

**验收结果：** 线程增加不是无条件收益。开发默认保持 2；以显式实验配置提供 4/8，待真实三主机/独立设备重测后再调整。

### P7.4：自适应策略（可选）

- [ ] 仅在 P7.2 显示 workload 间最佳 batch 差异显著时实现 EWMA target；
- [ ] 变更幅度、变更频率、上下限、回退值可配置；
- [ ] 增加小对象/慢客户端/低负载回归；
- [ ] 默认关闭，先做 shadow metrics 或实验开关。

**验收：** 自适应优于固定策略的结论来自多负载、多轮数据；否则不合入默认配置。

### P7.5：真实机器与 durability 恢复

- [ ] 在独立 Linux 主机、独立 SSD/NVMe、真实 NIC 的三主机 RF=2 重跑胜出矩阵；
- [ ] WSL/VM 仅作为开发对照，数据目录使用 Linux ext4，不以 `/mnt/c` 成绩发布性能结论；
- [ ] 分别验证 buffered、group_commit、chunk_sync 的进程终止/重启恢复；
- [ ] 掉电/虚拟盘缓存语义在目标环境单独验证，不能由 `fdatasync` 调用次数推断。

**验收：** 给出真实部署的容量区间和 durable 语义；不将 loopback/VHDX 数据外推为生产 SLA。

## 11. 发布门槛与回退

任何实现改动必须通过：

```text
上传/下载 final checksum 正确；
RF=2 成功节点集合正确；
sync 或 index 失败不发布对象；
pending queue 永远有上限；
进程停止时 pending durable waiter 被 drain 或明确失败；
旧 buffered / chunk_sync / legacy SHA-256 模式不回归；
P95/P99、RSS 和 error rate 不因峰值吞吐提高而恶化到不可接受。
```

每个新行为均提供配置开关与现有实现回退路径。性能工程顺序固定为：

```text
观测 → 单变量实验 → 正确性回归 → 保留或回退 → 真实机器复测
```
