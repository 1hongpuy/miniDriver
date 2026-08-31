# MiniDriver 3.0：P7 可靠写入流水线 SSD 矩阵报告（2026-08-30）

> 状态：开发 SSD 的 P7.1/P7.2/P7.3 已完成。结论只适用于这台 VM 的单 DataNode、单 ext4 数据盘；不是三主机 RF=2
> 发布容量，也不是掉电恢复证明。

## 1. 环境与方法

```text
数据盘：/dev/sdc1 → /data-ssd，ext4，rw，约 71 GiB 空闲
服务：1 Gateway + 1 DataNode；opaque chunk ID + CRC32C；group_commit
对象：sustained 为 64 MiB；每对象 16 × 4 MiB Chunk
固定条件：group deadline=2 ms，pending=64 MiB/64 items，chunk-window=2
P7.2：batch=8/16/32 MiB，c1/c2/c4/c8 sustained、mixed c8，各 5 轮
P7.3：batch=8 MiB，Disk Worker=2/4/8；c4/c8 sustained、mixed c8，各 5 轮
```

每项均为 fresh cluster/data root；所有请求成功。原始数据位于：

```text
/data-ssd/minidriver-v3-p7-durable-write-20260830b8/
/data-ssd/minidriver-v3-p7-durable-write-20260830b16/
/data-ssd/minidriver-v3-p7-durable-write-20260830b32/
/data-ssd/minidriver-v3-p7-durable-write-20260830w4/
/data-ssd/minidriver-v3-p7-durable-write-20260830w8/
```

`pidstat` 与 `iostat` 在宿主机未安装；各 case 留有 `*.unavailable.txt` 和 `vmstat.log`。因此没有把每进程 CPU/RSS
或设备 await/util 伪装成已测数据。

## 2. P7.1：流水线是否真的重叠

答案是肯定的。`durability_batch_complete` 现在单独记录 batch formation、`fdatasync`、LevelDB index sync 与完整
batch commit；每个 Chunk 另记录 write-ready 和 durability-queue wait。

在 8 MiB/c8 sustained 的代表 case 中：

| 指标 | P95 |
|---|---:|
| batch formation | 106.6 ms |
| data sync | 32.2 ms |
| index sync | 3.8 ms |
| batch commit | 36.7 ms |
| durable queue wait | 105.0 ms |

同一 case 累计记录到约 1.94 GiB `pwrite` 在 active durable sync 期间完成。它证明 Batch N 同步时，后续的
`pwritev` 没有被同步屏障串行堵住；这不代表可以并发对同一 `dataFd` 做多个 `fdatasync`。

## 3. P7.2：batch 大小

下表为 upload 文件 P95（ms）；括号内为每轮 aggregate 平均 MiB/s。它们共同决定默认，而不是只看峰值。

| Batch | sustained c1 | sustained c2 | sustained c4 | sustained c8 | mixed c8 |
|---|---:|---:|---:|---:|---:|
| 8 MiB | 674.5 (55.3) | 840.9 (103.7) | **1064.4 (168.6)** | **1643.0 (228.9)** | 339.9 (273.8) |
| 16 MiB | 712.2 (58.3) | 814.5 (109.5) | 1213.3 (154.9) | 1648.4 (227.5) | 363.2 (259.7) |
| 32 MiB | 686.6 (58.2) | **803.2 (105.7)** | 1136.3 (167.5) | 1705.9 (236.4) | **283.5 (281.0)** |

结论：不存在全负载赢家。32 MiB 在 mixed c8 确有可重复收益，但持续上传的 c4/c8 P95 不如 8 MiB；16 MiB 也没有
稳定击败 8 MiB。因此开发默认维持 **8 MiB / 2 ms**，32 MiB 只是后续按 workload 选择的候选值。没有足够证据进入
自适应 batch 实现。

各 case 的 durable pending 均没有突破 64 MiB 上限，且没有失败；故本轮没有观察到内存无界增长。

## 4. P7.3：Disk Worker 数量

固定 batch=8 MiB，数值为 upload P95（ms）/ aggregate 平均 MiB/s：

| Worker | sustained c4 | sustained c8 | mixed c8 |
|---:|---:|---:|---:|
| 2 | 1064.4 / 168.6 | 1643.0 / 228.9 | 339.9 / 273.8 |
| 4 | 1037.0 / 177.7 | 1892.2 / 219.2 | 330.5 / 282.4 |
| 8 | **1011.0 / 179.4** | **1632.0 / 227.8** | **267.2 / 307.9** |

更多 Worker 并没有让持续 c8 线性增长：2、4、8 Worker 的 aggregate 分别约 228.9、219.2、227.8 MiB/s。与此同时，
`pwrite` P95 在 c8 约从 6.9 ms 增至 14.0 ms、31.6 ms，说明并发写入自身开始产生竞争。

`vmstat` 平均值进一步说明“CPU 还有余量”不等于“再加线程就更快”：c4 三档 idle 约 46–47%，c8 三档 idle 约
24–30%，iowait 约 4–6%；但单 shard 的 durable sequencer 仍维持同步/索引发布顺序。

结论：开发默认保持 **2 Disk Worker**。4/8 Worker 保留为实验配置；8 Worker 对 mixed/c4 有价值，但要在真实网络、
独立磁盘和目标 workload 上复测后才能成为默认。

## 5. 已完成与剩余

已完成：阶段化观测、同步与 `pwritev` overlap 回归、目标 SSD 的 8/16/32 MiB 矩阵、2/4/8 Worker 矩阵、正确性与有界
pending 验证。

尚未完成：真实三主机 RF=2、真实 NIC/RTT 慢读/混合负载、`pidstat`/`iostat` 完整系统证据、buffered/group_commit/
chunk_sync 的进程终止与掉电恢复对照，以及 verified Range sidecar。这些仍是 MiniDriver 3.0 发布门槛，不能由本报告勾选。
