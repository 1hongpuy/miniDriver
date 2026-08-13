# V4.3 阶段 0：完整性能、可靠性与优化评估（2026-08-11）

## 最终摘要

MiniKVCine V4 当前已经具备可验证的 Multi-Reactor、上传写入准入、双副本链式复制、
有界磁盘写队列、BlockPool、HTTP pause/resume 与 `sendfile` 下载路径。在 8 vCPU、
约 2.8 GiB 内存、真实 `/data` ext4、同机 loopback 的测试机上，系统的数据正确性和
过载语义符合预期：成功对象完成 SHA-256 校验，写入满载时 Gateway 返回可重试的
`503 + Retry-After`，没有观察到异常 `400` 或 Chunk hash 错误。

当前主要限制不是 Gateway 控制面、BlockPool 容量、磁盘 worker 持续饱和、EventLoop lag
或用户态输出缓冲；最值得关注的是：

```text
上传 Chunk 的 Body receive
  ├─ SHA-256：最大稳定成本
  ├─ 内存复制与 64 KiB 磁盘任务粒度
  └─ pwrite 与短时磁盘队列 pause

副本链路
  └─ 当前主要影响 P95/P99，短连接仍是结构性开销
```

因此下一阶段不应继续堆叠 epoll 线程、BlockPool 或磁盘 worker，而应进入“副本连接复用 →
减少重复 Body 拷贝 → 受治理 Chunk 并行”的验证性优化周期。

## 1. 测试范围、环境与可信边界

### 环境

- 代码基线：`19f7b2e feat: add V4 multi-reactor datanode` 加本阶段未提交的 benchmark/观测改动。
- 构建：`RelWithDebInfo`，GCC 11.4，CMake 3.22。
- 主机：8 vCPU、约 2.8 GiB 内存、2 GiB Swap。
- 磁盘：`/data` ext4，约 491 GiB。
- 部署：Gateway 1 个、DataNode 2--4 个、同机 loopback；链式双副本。
- 对象：16 MiB，每个文件 4 个 4 MiB Chunk；每档通常 3 轮。
- 默认治理：每 DataNode 上传槽 2、下载槽 8；每客户端上传槽 2、下载槽 4。
- DataNode I/O：`MINIKV_V4_IO_THREADS=2`；V4.2 基线额外对比过 0 个 worker Loop。

### 边界

这些结论不代表跨机房、真实网络延迟、慢客户端、大对象、长时间 soak、故障恢复或多 Gateway
场景。loopback 的下载、尤其 `sendfile`，无法替代真实 NIC、TCP 拥塞和机架网络测试。

## 2. 正确性与可靠性

| 项目 | 结果 |
|---|---|
| 当前完整 CTest | 45/46 通过；唯一失败是 Python 环境缺少 `playwright` 的 UI 测试 |
| Multi-Reactor 重点测试 | EventLoopThread、线程池、TcpServer、HttpServer 生命周期测试通过 |
| 上传/下载完整性 | 成功文件均在 benchmark 端重组并 SHA-256 校验 |
| 写入满载行为 | Gateway routes 返回 `503 + Retry-After` |
| 观察到的数据面异常 | 未见 hash 不匹配、复制损坏或无解释 `400` |

`playwright` 缺失是测试依赖问题，不是存储数据面失败；它仍应在 CI/开发环境补齐后纳入完整
UI 回归。

## 3. 集群即时上传容量

双副本下，每个上传文件需要链上的两个节点各占一个写入槽。实测与以下模型一致：

```text
集群即时文件上传容量 = 节点数 × 每节点上传槽 ÷ 副本数
```

在默认每节点 2 槽、双副本下，容量为 `N × 2 ÷ 2 = N`。

| DataNode 数 | 文件并发 | 成功/请求 | 结论 |
|---:|---:|---:|---|
| 2 | 2 | 6/6 | 稳定即时容量 2 |
| 2 | 4 | 6/12 | 每轮 2 个预期 503 |
| 3 | 3 | 9/9 | 稳定即时容量 3 |
| 3 | 4 | 9/12 | 每轮 1 个预期 503 |
| 4 | 4 | 12/12 | 稳定即时容量 4 |
| 4 | 5 | 12/15 | 每轮 1 个预期 503 |

因此“每节点只允许 2 个上传”是保守的节点保护策略，不是集群总容量只能为 2；扩充节点会在
双副本约束下线性提升即时可接纳文件数。

## 4. 文件级性能

### V4.2 双节点端到端基线

下表为 16 MiB、双节点、双副本、每档 3 轮的成功文件 P50。并发 4/8 的成功数受上传槽限制，
其 P50 只表示成功流。

| I/O worker Loop | 文件并发 | 成功/请求 | 上传 P50 | 下载 P50 |
|---:|---:|---:|---:|---:|
| 0 | 1 | 3/3 | 314.207 ms / 50.92 MiB/s | 146.587 ms / 109.15 MiB/s |
| 0 | 2 | 6/6 | 291.691 ms / 54.85 MiB/s | 176.796 ms / 90.50 MiB/s |
| 2 | 1 | 3/3 | 344.622 ms / 46.43 MiB/s | 150.872 ms / 106.05 MiB/s |
| 2 | 2 | 6/6 | 277.360 ms / 57.69 MiB/s | 168.296 ms / 95.07 MiB/s |

`io_threads=2` 仅在并发 2 上传中显示约 5% P50 改善；单流无收益。结合后续 lag 指标，不能
得出“更多 epoll 线程必然更快”的结论。

### download-only 与 mixed

download-only 使用计时前准备的已 commit 夹具，下载时间不混入上传；mixed 同时运行预置对象
下载者和新对象上传者。

| 模式 | 并发 | 成功/请求 | 结果 |
|---|---:|---:|---|
| download-only | 1 | 3/3 | 下载 P50 194.370 ms / 82.32 MiB/s |
| download-only | 2 | 6/6 | 下载 P50 176.847 ms / 90.47 MiB/s |
| download-only | 4 | 12/12 | 下载 P50 178.288 ms / 89.74 MiB/s |
| download-only | 8 | 24/24 | 下载 P50 279.334 ms / 57.28 MiB/s |
| mixed | 2（1 上传 + 1 下载） | 6/6 | 上传/下载 P50：297.428 / 298.961 ms |
| mixed | 4（2 上传 + 2 下载） | 12/12 | 上传/下载 P50：242.282 / 275.812 ms |
| mixed | 8（4 上传 + 4 下载） | 18/24 | 6 个上传在 routes 阶段预期 503；12 个下载校验成功 |

download-only 并发 8 仍可完成，但文件延迟上升；mixed 并发 8 的主要限制先是默认双节点写入
准入，而不是下载错误。

## 5. 控制面与 Chunk 数据面

在独立的双节点 mixed 并发 4 观测样本中，48 个 `role=primary` Chunk 全部成功：

| 数据面指标 | P50 | P95 | P99 | 判断 |
|---|---:|---:|---:|---|
| Chunk total | 42--51 ms | 66--77 ms | 70--144 ms | 用户可见 Chunk 完成时间 |
| Body receive | 37--43 ms | 62--67 ms | 67--75 ms | 当前主要成本 |
| Replica wait | 0--2 ms | 15--23 ms | 26--78 ms | P50 小，但尾部有抖动 |
| SHA update | 22--25 ms | 37--38 ms | — | Body receive 中最大的稳定分量 |
| pwrite | 6--7 ms | 10--12 ms | — | 次要但稳定的成本 |
| Gateway Chunk commit | 0 ms | 1 ms | 1 ms | 非瓶颈 |

Gateway 内处理延迟也较低：routes P50 约 31 µs、P95 61 µs；Chunk commit P50 74 µs、
P95 157 µs；文件 commit P50 136 µs、P95 217 µs。它们不包含客户端网络往返，但足以表明
当前 Gateway/LevelDB 控制面不是 Chunk 40--70 ms 延迟的主要来源。

`Body receive` 是首个 Body 字节到本地写完成的时间，包含网络输入进入处理后的 SHA、BlockPool
复制、磁盘任务排队、pwrite、最终 hash/index；它不是客户端到服务器的完整网络传输时间。

## 6. 背压、内存池与磁盘执行器

| 指标 | 实测 | 解释 |
|---|---:|---|
| BlockPool 容量 | 128 × 64 KiB = 8 MiB | 固定上限 |
| 生命周期 peak leased | 3.81--4.19 MiB，约 61--67 块 | 未耗尽；存在短 burst |
| 瞬时最低可用块 | 92--110 / 128 | 1 秒采样不能替代生命周期峰值 |
| executor 生命周期队列峰值 | 2 tasks | 有短时排队，未见长期增长 |
| 单 pipeline 磁盘队列峰值 | 约 1.06 MiB | 接近局部高水位 |
| 单 pipeline pause | 最多 6 次、48--51 ms | 已有短暂写入背压 |
| replica pending bytes | 0 | 本轮未见副本 pipe 用户态积压 |

这组数据不支持扩大 BlockPool 或立即增加磁盘 worker：两者可能增大 RSS 或磁盘竞争，却无法减少
SHA/pwrite 固定成本。它支持继续维持当前有界背压，并优化数据复制与任务粒度。

## 7. Multi-Reactor 与网络背压

每个 DataNode 包含 1 个 acceptor/base Loop 与 2 个 I/O Loop。每个 I/O Loop 都有独立
timer probe，测到：

| 指标 | 结果 |
|---|---:|
| I/O Loop 最大 timer lag | 1 ms |
| acceptor Loop 最大 timer lag | 2 ms |
| I/O Loop 累计跨线程投递 | 约 2,400--2,900 / Loop |
| pending functor 生命周期峰值 | 2 / Loop |
| DataNode `outputBuffer` 生命周期峰值 | 0 bytes |
| outputBuffer 高水位事件 | 0 |

I/O Loop 的投递量来自磁盘 executor、异步副本/HTTP 完成后回到连接所属 Loop；深度很低且 lag
很小，说明本轮没有回调积压。`outputBuffer=0` 不是网络没有传输，而是下载通过 `sendfile`
绕过用户态输出 Buffer，上传是入站流。慢客户端、跨机网络、主动 `send()` 大响应仍需单独测试。

## 8. 综合瓶颈判断

目前证据支持的优先级：

```text
优先级 1：上传 Body 数据路径
  SHA-256 + inputBuffer→BlockPool 复制 + 64 KiB 任务/队列 + pwrite

优先级 2：副本尾延迟
  每 Chunk 短连接、连接建立/关闭、异步回调抖动

优先级 3：受资源约束的并发度
  Chunk window、磁盘局部高水位、双副本写槽

当前非优先：Gateway 控制面、增加 EventLoop、扩大 BlockPool、增加 disk worker
```

要注意：SHA-256 是数据完整性必要成本，不能删除；优化目标是减少不必要的数据复制、调度和
重复缓冲，而不是取消校验或绕过背压。

## 9. 后续实施计划

### 阶段 1：副本长连接池

目标：降低副本短连接对 P95/P99 的影响，减少 TCP 建连、HTTP 解析与对象分配。

- 按 `nodeId + endpoint` 维护有界空闲连接池；一条连接同一时刻只服务一个副本请求。
- 处理连接超时、对端关闭、半开、失败剔除、退避重连和 DataNode shutdown。
- 测试：复用、并发借用上限、连接失效/重连、关闭生命周期、跨机或注入延迟下 P95/P99。
- 验收：hash/commit 语义不变；连接失败安全降级；Chunk/Replica P95/P99 或 CPU 有可重复改善。

### 阶段 2：有界 SharedBodyBlock 与减少复制

目标：网络输入复制到稳定块后，让本地落盘和副本发送共享该块，避免两个消费者各自缓存/复制。

- Block 必须引用计数、固定上限、可跨线程安全释放；副本 partial write 保存 `block + offset`。
- BlockPool、磁盘队列、副本连接池满时必须 pause 上游读；不得用无界 `shared_ptr` 队列换吞吐。
- 测试：部分写、`EAGAIN`、连接关闭、多个消费者、最后引用释放、ASan/LSan（可用时）。
- 验收：RSS、复制字节、outputBuffer、Chunk P99 不恶化，且吞吐/CPU 有可重复收益。

### 阶段 3：磁盘任务粒度与受治理 Chunk window

目标：比较 64/128/256 KiB Block 粒度，并仅在资源预算允许时把单文件窗口从 1 测到 2。

- 资源预算包括写槽、BlockPool、executor 队列、副本连接配额。
- 对比 `window=1/2` 与不同 Block 大小，不能直接放开所有 Chunk 并发。
- 验收：P95/P99、pause、BlockPool peak、RSS、队列峰值均在阈值内；无收益或尾部恶化则保留当前设置。

### 阶段 4：公平调度与系统级验证

目标：避免下载、上传、复制和媒体后台任务互相饿死。

- 磁盘任务按上传/下载/复制/后台分类，使用有界队列和加权/轮转策略。
- 运行 25/75、50/50、75/25 mixed，以及受控媒体任务。
- 采集 CPU/RSS、上下文切换、火焰图、跨机网络、长时间 soak；对 503 执行 `Retry-After` 退避重试。

## 10. 当前不做的事项

本周期不实现多 Gateway、高可用元数据/WAL/GC/Repair、gRPC、跨地域一致性、权限系统或
RAG/AI 业务。这些功能很重要，但不应与当前对象存储数据面性能优化混在同一验证周期。

## 11. 原始报告与产物索引

- [V4.2 集群容量报告](V4_2_CLUSTER_CAPACITY_TEST_REPORT_2026-08-11.md)
- [V4.3 下载与混合负载报告](V4_3_STAGE0_LOAD_TEST_REPORT_2026-08-11.md)
- [V4.3 Chunk/控制面观测报告](V4_3_STAGE0_OBSERVABILITY_REPORT_2026-08-11.md)
- [V4.3 DiskWriteExecutor/BlockPool 报告](V4_3_STAGE0_RESOURCE_SNAPSHOT_REPORT_2026-08-11.md)
- [V4.3 EventLoop 报告](V4_3_STAGE0_EVENT_LOOP_REPORT_2026-08-11.md)
- [V4.3 网络背压报告](V4_3_STAGE0_NETWORK_BACKPRESSURE_REPORT_2026-08-11.md)

所有 `/data` 原始 CSV、JSON 和进程日志路径均写入各子报告。性能报告中的独立短样本只用于
对应指标关联；跨版本或跨优化的比较必须使用相同节点数、I/O Loop、文件大小、轮数与负载模式。
