# V4.3 当前性能全景分析与后续优化路线（2026-08-12）

## 一句话结论

MiniKVCine 已经从“单机 loopback 上能跑通的对象存储”进入了“有资源边界、可解释性能、能做
容量选择”的阶段：副本短连接、重复 Body 缓冲和单文件 Chunk 串行已经得到最小化优化；当前决定
高并发 mixed 服务质量的主要限制，是 **磁盘写入尾延迟与由其触发的 BlockPool/HTTP 背压**，不是
Gateway 控制面、EventLoop 数量或输出 Buffer。

当前在双 DataNode、双副本、16 MiB 条件下可选择两种已验证运行档：

```text
稳定默认档：每节点 2 上传槽，客户端全局 Chunk 预算 2
高吞吐实验档：每节点 4 上传槽，每文件 window=2，全局 Chunk 预算 4
```

第二档在 upload-only 下显著提高 aggregate 吞吐，但在 mixed c8 下造成上传 P95 约 0.9--1.3 s；
因此它仍是实验配置，不能替换默认档。

## 1. 范围、环境与可信边界

本报告汇总 V4.2--V4.3 已完成的同机真实磁盘实验。

- 主机：8 vCPU、约 2.8 GiB RAM、2 GiB swap；`/data` ext4，约 491 GiB。
- 部署：Gateway 1、DataNode 2、链式双副本、同机 loopback；DataNode I/O EventLoop=2。
- 对象：16 MiB 文件，4 × 4 MiB Chunk；多数矩阵每档 3 轮。
- 构建：`RelWithDebInfo`，GCC 11.4、CMake 3.22。
- 正确性：完整 CTest 为 **47/48 通过**；唯一失败为 UI 测试缺少 Python `playwright`，不是数据面失败。

这些数据不能代表跨机 RTT/丢包、真实 NIC、慢客户端、长时间 soak 或多 Gateway；尤其 loopback
上的 `sendfile` 下载不能外推为生产网络吞吐。

## 2. 当前已完成能力

```text
上传：4 MiB Chunk、SHA-256、路由 Token、断点续传、文件级 commit
可靠性：Gateway WriteLease、DataNode 本地 admission、503 + Retry-After、commit 幂等
副本：链式双副本、HTTP/1.1 Keep-Alive 池、失败时短连接 fallback
网络：Multi-Reactor、连接归属、跨线程回调保护、pause/resume、sendfile 下载
磁盘：有界 DiskWriteExecutor、8 MiB BlockPool、SharedBodyBlock、精确峰值指标
可观测性：Chunk/控制面时延、资源快照、EventLoop lag、outputBuffer、背压指标
客户端：网页已有每文件 window=2/全局 active Chunk=2；C++ benchmark 支持 window=1|2 与全局预算
```

这意味着后续不是重写架构，而是在已存在的资源治理边界内缩短写入服务时间、控制尾延迟并提升公平性。

## 3. 从基线到当前的实测变化

不同阶段均有机器状态和页缓存差异，以下用于说明趋势，不把每一次 P50 变化都归因于单一改动。

| 阶段 | 关键改动 | 可验证结果 |
|---|---|---|
| V4.2 | Multi-Reactor、双节点默认 2 槽 | 并发 2 upload P50 277.360 ms；更高请求得到可重试 503 |
| Stage 0 | 负载/资源观测 | 控制面微秒级；Body receive 与 replica tail 成为数据面重点 |
| 连接池 | 副本 Keep-Alive、按 Loop 分片 | 实际日志证明会话复用；upload c2 P50 253.725 ms |
| SharedBodyBlock | 磁盘与副本共享 64 KiB Block | 避免副本 pending string 复制；upload c2 P50 234.878 ms |
| Chunk window | 每文件最多 2 Chunk、全局预算 | 默认两槽下 c1 window=2 P50 127.236 ms；治理后不触发过载 |
| 四槽容量 | 2 文件 × window=2 × 全局 4 | upload-only aggregate 中位 135.35 MiB/s，6/6 成功 |

## 4. 当前性能画像

### 4.1 默认两槽档：正确性优先

双节点、双副本、每节点 2 上传槽时，理论即时文件容量为：

```text
节点数 × 每节点上传槽 ÷ 副本数 = 2 × 2 ÷ 2 = 2 个文件
```

超过容量时 routes 返回明确的 `503 + Retry-After`，未观察到 hash 损坏或无解释 400。这是服务端
在内存和磁盘压力发生前拒绝请求的正确语义。

### 4.2 高吞吐四槽档：upload-only 有效

四槽实验中，每节点/每客户端上传槽为 4，客户端 window=2、全局预算=4：

| 场景 | 成功/请求 | 文件上传 P50 | aggregate 吞吐中位 |
|---|---:|---:|---:|
| 2 文件、window=1、预算 2 | 6/6 | 245.373 ms | 88.66 MiB/s |
| 2 文件、window=2、预算 2 | 6/6 | 209.780 ms | 80.48 MiB/s |
| 2 文件、window=2、预算 4 | 6/6 | 107.550 ms | 135.35 MiB/s |
| 4 文件、window=2、预算 4 | 12/12 | 207.419 ms | 165.97 MiB/s |

这里的核心结论是：**window 解决单文件串行空洞；全局预算和服务端槽位决定集群能否真正并行。**
只提高每文件 window 而不提高全局预算/服务端槽位，无法提高总容量，甚至会引起预期 503。

### 4.3 mixed 高负载：当前质量边界

四槽 mixed c8（4 上传 + 4 下载）两次均完整成功，但上传尾延迟不稳定：

| 样本 | 上传 P50 / P95 | 下载 P50 / P95 | 结论 |
|---|---:|---:|---|
| 矩阵样本 | 567.588 / 1313.506 ms | 284.871 / 331.956 ms | 上传尾部显著恶化 |
| 独立复测 | 664.151 / 945.215 ms | 255.231 / 291.260 ms | 同一趋势复现 |

因此当前可说“系统能安全完成 c8”，不能说“c8 具有可接受的上传 SLA”。

## 5. 瓶颈定位：什么不是问题，什么才是问题

### 已排除为主要瓶颈

| 模块 | 证据 | 当前判断 |
|---|---|---|
| Gateway 控制面 | routes P50/P95 约 28/80 µs；commit 仍为微秒级 | 不应优先优化 |
| I/O EventLoop | 绝大多数 I/O Loop lag 1--3 ms；pending depth 很低 | 增加 epoll 线程没有依据 |
| 用户态输出 Buffer | 生命周期峰值 0，下载走 sendfile | 不是当前上传尾延迟来源 |
| 副本 pending 缓冲 | `max_pending_bytes=0` | SharedBodyBlock/背压没有失控 |
| BlockPool 容量本身 | 默认档约半池使用 | 扩容不是低负载优化；高负载满池是压力信号 |

### 已确认的主要问题

```text
1. 高 mixed 写入：pwrite/页缓存抖动 → DiskWriteExecutor 局部积压 → pauseRead
2. 满池时：SharedBlock 仍被磁盘/副本持有 → BlockPool 无可租借块 → 上游读暂停
3. pause 后：客户端和文件内 Chunk 等待 → 文件上传 P95/P99 被放大
4. 副本：连接池已削弱短连接成本，但高压下 replica wait 仍跟随磁盘路径一起恶化
5. 客户端：普通全局信号量只限制总量，不能保证不同文件公平推进
```

mixed c8 独立观测的直接证据：Chunk total P95/P99 = 420/464 ms，Body receive P95=419 ms，
`pwrite` P95/P99 = 210.563/415.202 ms，BlockPool 精确峰值 = 8/8 MiB，最长单 pipeline
disk pause = 418 ms。SHA update P50/P95 = 24.038/33.590 ms，是稳定成本，但不能解释数百毫秒的尾部。

## 6. 剩余优化工作：按收益和风险排序

### P0：补齐磁盘执行器的决策证据

在修改磁盘实现前，增加每个任务的 queue-wait、worker busy time、pwrite 直方图，并按任务类别
聚合。现有 1 秒快照不足以观察短 burst；精确峰值和任务级直方图必须并存。

验收：能够将 Chunk P95 分解为 SHA、排队、pwrite、replica 等部分，并在 c4/c6/c8 mixed 中稳定复现。

### P1：磁盘写任务粒度与 worker 数的单变量实验

当前每个 4 MiB Chunk 被拆为 64 KiB Block，即约 64 个磁盘任务。先不改默认，固定四槽、高吞吐
window=2，做下表矩阵：

| 变量 | 值 | 目的 |
|---|---|---|
| Block 大小 | 64 / 128 / 256 KiB | 降低任务/回调数，同时检查更大 burst 内存 |
| 磁盘 worker | 1 / 2 / 3 | 区分“worker 太少”与“同盘并发竞争” |
| 负载 | upload c2、mixed c4/c6/c8 | 同时看吞吐与混合尾延迟 |

选择标准：aggregate 吞吐提升，且 `pwrite P95`、Chunk P95、pause、BlockPool peak、RSS 不恶化。
如果增加 worker 只让 pwrite P95 变差，则保留较少 worker；如果 128 KiB 显著降低调度而仍有内存余量，
再考虑它作为默认粒度。

### P2：实现磁盘任务分类的公平调度

将 DiskWriteExecutor 中的工作分成前台上传、前台下载、链式副本、媒体后台任务；使用有界队列与
加权轮转，而不是一个 FIFO 让某类任务长期占满资源。

目标是：上传和下载都持续推进，后台任务只使用剩余能力。高水位时应暂停/限速低优先级任务，绝不
扩大无界内存队列。

### P3：客户端按文件公平的 Chunk 调度

将 `ChunkBudget` 从“谁先醒来谁拿令牌”的信号量改为就绪文件队列：每次完成后把文件放回队尾，
优先调度等待文件的下一个 Chunk。

这不提高服务端并发，也不替代 admission；它改善两个或多个文件之间的进度公平性，并要求新增：

- 每文件 Chunk budget wait P50/P95；
- 每文件完成时间与最大等待时间；
- 乱序完成、失败、取消、预算关闭的单元测试。

### P4：继续降低稳定数据路径成本

- SHA：确认 OpenSSL SHA-NI 指令路径，微基准 64/128/256 KiB `EVP_DigestUpdate`；研究将按顺序的
  Chunk hash 聚合成文件 hash，避免压测客户端或服务端不必要的整文件重复扫描。
- 副本发送：为 HTTP 头 + SharedBodyBlock 评估 `writev`，并完整处理 partial write/EAGAIN。
- 连接池：补 idle TTL、失败指数退避、半开探测和资源快照指标；跨机网络验证它对 P95/P99 的价值。

这些优化不应以删除 SHA-256 或改 MD5 为代价：SHA-256 是对象寻址与完整性协议的一部分。

## 7. 推荐测试体系

每个候选改动必须在相同硬件、构建参数、节点数、文件大小、轮数下对比；原始 CSV、日志和环境变量
必须保留。建议把以下矩阵作为固定回归：

| 类别 | 档位 | 目的 |
|---|---|---|
| upload-only | c1/c2/c4 | 单文件窗口与 aggregate 上限 |
| download-only | c1/c2/c4/c8 | sendfile/读取饱和点 |
| mixed | c4 (2U+2D)、c6 (3U+3D)、c8 (4U+4D) | 稳定服务区与过载边界 |
| 容量 | 2/3/4 DataNode、不同槽位 | 验证副本约束下的线性扩展 |
| 故障 | 副本连接关闭、超时、Node Offline、503 退避 | 可靠性不因优化退化 |

每档至少 5 轮，记录：aggregate MiB/s、文件/Chunk P50/P95/P99、HTTP 码、SHA 校验、控制面 QPS、
pwrite/queue wait、磁盘 pause、BlockPool 精确峰值、CPU/RSS、EventLoop lag、输出队列及连接池指标。

## 8. 建议的实施顺序

```text
P0 任务级磁盘观测
  ↓
P1 Block 大小 / worker 单变量矩阵并选出候选配置
  ↓
P2 磁盘读写/后台公平队列
  ↓
P3 客户端按文件公平 Chunk 调度
  ↓
P4 SHA、writev、连接池容错细化
  ↓
完整 mixed + 跨机 + soak 验证，再决定默认槽位
```

在 P1--P3 得到稳定 mixed c4/c6 结果前，不应继续增加 I/O EventLoop、BlockPool、Chunk window 或
默认上传槽；现有数据已经表明这些做法会把压力放大，而不是消除根因。

## 9. 非目标

本周期不将对象存储重构为多 Gateway、高可用元数据/WAL/GC/Repair、gRPC、跨地域一致性或 AI/RAG
业务平台。它们可以在数据面性能与可靠性基线稳定后，作为“多模态训练数据资产平台”的上层能力继续建设。

## 10. 证据索引

- [V4.2 容量与性能基线](V4_2_CLUSTER_CAPACITY_TEST_REPORT_2026-08-11.md)
- [V4.3 Stage 0 综合评估](V4_3_STAGE0_COMPREHENSIVE_PERFORMANCE_ASSESSMENT_2026-08-11.md)
- [副本连接池验证](V4_3_REPLICA_CONNECTION_POOL_REPORT_2026-08-12.md)
- [SharedBodyBlock 验证](V4_3_SHARED_BODY_BLOCK_REPORT_2026-08-12.md)
- [有界 Chunk window 验证](V4_3_CHUNK_WINDOW_REPORT_2026-08-12.md)
- [四槽容量边界实验](V4_3_FOUR_SLOT_CHUNK_CAPACITY_REPORT_2026-08-12.md)
