# V4.3 四写槽 / 全局四 Chunk 容量实验（2026-08-12）

## 结论

本实验回答“能否让两个文件都以 window=2 上传”的问题。答案是：**可以，但必须同时提高
客户端全局预算与服务端写入准入；它适合作为吞吐容量档，不应直接替换当前默认的两槽配置。**

在隔离的双 DataNode、双副本集群中，临时设置每节点 `maxActiveUploads=4`、
`maxUploadsPerClient=4`，并将 benchmark `--global-chunk-budget` 设为 4。两个文件各自
window=2 的 upload-only 样本达到 6/6 成功，文件 P50 为 **107.550 ms / 148.77 MiB/s**，
三轮 aggregate 吞吐中位数为 **135.35 MiB/s**，高于同样四槽服务端但全局预算仍为 2 的
约 **80.48 MiB/s**。

但 mixed c8（4 上传 + 4 下载）虽 24/24 成功，却出现上传 P50 **664.151 ms**、P95
**945.215 ms**。独立复测中 BlockPool 精确生命周期峰值达到整个 **8 MiB**，Chunk total P95
为 **420 ms**，`pwrite` P95 为 **210.563 ms**，单 pipeline 最长 disk pause 为 **418 ms**。
这说明四槽已能提高低/中并发吞吐，但在读写混合高负载时把瓶颈推到了磁盘写入、有限 BlockPool
和背压；不应把“没有 503”误当成“默认容量应提高”。

## 实验设计

除本实验进程外，默认配置与代码均未改变。启动脚本只继承下面的临时环境变量：

```text
MINIKV_V4_MAX_ACTIVE_UPLOADS=4
MINIKV_V4_MAX_UPLOADS_PER_CLIENT=4
MINIKV_V4_IO_THREADS=2
```

Gateway 在节点注册时得到 `maxConcurrentWrites=4`，DataNode 本地 governor 同样允许四条上传；
因此 Gateway WriteLease 与节点本地 admission 仍一致。客户端的 `--global-chunk-budget 4` 才能
使两个文件各自最多两条 Chunk 请求同时在飞。只改客户端预算而不提高服务端槽位会正确得到
`503 + Retry-After`，不属于本实验的有效容量配置。

环境：8 vCPU、约 2.8 GiB RAM、`/data` ext4、同机 loopback；Gateway 1、DataNode 2、
链式双副本、16 MiB 文件（4 × 4 MiB Chunk）、每档 3 轮。

## upload-only 结果

所有行均使用四槽 DataNode；吞吐为 `16 MiB / 文件 P50`。aggregate 是一轮所有成功文件的总
完成吞吐，更适合评价集群能力。

| 文件并发 | 每文件 window | 全局预算 | 成功/请求 | 文件上传 P50 / P95 | 三轮 aggregate MiB/s | aggregate 中位数 |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 1 | 2 | 6/6 | 245.373 / 263.326 ms; 65.21 MiB/s | 86.98, 88.66, 97.21 | 88.66 |
| 2 | 2 | 2 | 6/6 | 209.780 / 297.107 ms; 76.27 MiB/s | 80.03, 86.68, 80.48 | 80.48 |
| 2 | 2 | 4 | 6/6 | 107.550 / 158.031 ms; 148.77 MiB/s | 126.65, 142.21, 135.35 | 135.35 |
| 4 | 2 | 4 | 12/12 | 207.419 / 253.672 ms; 77.14 MiB/s | 173.30, 156.48, 165.97 | 165.97 |

第二行说明简单令牌调度仍有文件间竞争与小样本波动；第三行才是真正允许“两个文件各两个
Chunk”后的容量效果。第四行表明继续增加文件数仍能完成，但文件延迟开始回升。

## mixed c8：稳定性边界

mixed c8 为 4 上传 + 4 下载，window=2、全局预算=4。首次矩阵与一次独立复测都成功，未出现
hash 错误、异常 400 或 503，但上传尾延迟很高：

| 样本 | 成功/请求 | 上传 P50 / P95 | 下载 P50 / P95 | 轮 aggregate MiB/s |
|---|---:|---:|---:|---:|
| 矩阵样本 | 24/24 | 567.588 / 1313.506 ms | 284.871 / 331.956 ms | 270.47, 160.49, 85.39 |
| 独立复测 | 24/24 | 664.151 / 945.215 ms | 255.231 / 291.260 ms | 127.92, 156.49, 112.09 |

两次的绝对值受同机页缓存和后台环境影响而有波动，但共同特征是：下载仍完整，上传尾延迟显著恶化。
所以四槽可以作为经过 admission 保护的“可完成”容量，尚不能称为高质量 mixed 服务容量。

## 独立 mixed c8 观测

以下来自独立 c8 复测的 Gateway/DataNode 结构化日志，避免把前面案例的进程生命周期峰值混入：

| 指标 | 结果 | 判断 |
|---|---:|---|
| routes P50 / P95 | 28 / 80 µs | Gateway 仍不是瓶颈 |
| Chunk total P50 / P95 / P99 | 52 / 420 / 464 ms | 严重尾部排队 |
| Body receive P50 / P95 | 46 / 419 ms | 主要尾部发生在写入路径 |
| SHA update P50 / P95 | 24.038 / 33.590 ms | 稳定成本，不解释 400 ms 尾部 |
| pwrite P50 / P95 / P99 | 7.007 / 210.563 / 415.202 ms | 磁盘写入/页缓存抖动是主要证据 |
| Replica wait P50 / P95 | 0 / 108 ms | 副本尾部也被压力放大 |
| BlockPool 生命周期峰值 | 8 MiB / 8 MiB | 已精确达到池上限 |
| 快照最低可用块 | 57 / 128 | 1 秒快照漏掉瞬时满池，不能据此否定峰值 |
| 单 pipeline 最大 disk pause | 7 次、418 ms | 背压正在保护内存，而非失效 |
| Disk executor queue peak | 4 tasks | 有短 burst，不是长队列无限增长 |
| I/O EventLoop 最大 timer lag | 3 ms（acceptor 8 ms） | 不是主要阻塞点 |

`max_pending_bytes=0`，说明 SharedBodyBlock 与现有 pause/resume 没有形成无界副本用户态堆积；
问题是容量被安全地限制后，客户端等待与磁盘 pause 反映为上传尾延迟。

## 结论与下一步

1. **不要修改默认两槽。** 四槽在轻载和 upload-only 下有明显吞吐收益，但 mixed c8 的 P95/P99
   不满足默认服务质量要求。
2. **客户端先实现公平的按文件轮转调度。** 当前 `ChunkBudget` 是全局信号量；它限制总量但不保证
   A/B 文件交替获取令牌。公平队列可降低单文件延迟不均，且不增加服务端压力。
3. **再做磁盘写路径的受控实验。** 比较 1/2/3 个 DiskWriteExecutor worker 与 64/128/256 KiB
   任务粒度，固定四槽、window=2，记录 pwrite P95、pause、BlockPool 峰值和 RSS。当前证据不支持
   直接增加 worker 或 BlockPool。
4. **四槽仅保留为压测配置。** 只有 mixed 负载下的 P95/P99、pause 和内存峰值达到明确阈值后，才
   考虑让 DataNode 依据硬件配置选择 2 或 4；不能把它作为全机器固定常量。

## 原始产物

```text
/data/minikv-v2/v4-chunk-capacity-20260812/
  cluster-slots4/
  upload-c2-w1-b2/
  upload-c2-w2-b2/
  upload-c2-w2-b4/
  upload-c4-w2-b4/
  mixed-c8-w2-b4/
  cluster-mixed-c8-only/
  mixed-c8-w2-b4-isolated/{summary.csv,observability.json}
```
