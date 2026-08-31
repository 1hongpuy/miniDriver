# miniDriver V4.4：控制面高可用、元数据一致性与副本自愈设计

> 日期：2026-08-17  
> 定位：在 V4.3 数据面优化基础上，补齐分布式状态管理、故障处理和负载感知放置。  
> 说明：当前代码已经具备 Multi-Reactor、Chunk 流式上传、链式副本、幂等 Commit、资源准入和背压；本文件描述下一阶段目标，不代表这些能力已经全部实现。

## 1. 目标

V4.3 解决的是“数据如何高效、正确地传输和写入”：

```text
客户端直传 → DataNode 流式写入 → 链式副本 → SHA-256/Commit → 有界背压
```

V4.4 解决的是“节点、控制服务或副本失败后，系统如何继续正确工作”：

- Gateway 多实例，单个 Gateway 退出后客户端仍能访问；
- 元数据不再只存在于单个 Gateway 的 LevelDB；
- DataNode 有明确的健康状态、故障检测和摘除流程；
- 副本不足时自动生成 Repair 任务并恢复目标副本数；
- 路由选择同时考虑稳定映射和节点实时负载；
- 所有对象可见性、版本和副本列表都有可解释的一致性语义。

## 2. 当前实现与缺口

### 2.1 已有能力

当前代码中已经存在以下基础：

- Gateway/DataNode 控制面与数据面分离；
- `GatewayState` 维护 Session、ChunkRoute、File Commit 和 WriteLease；
- DataNode 通过 `ChunkUploadStream` 接收 Chunk；
- `ReplicaUploadPipe` 将主副本流式转发到下游副本；
- 主副本等待本地写入与副本 HTTP ACK 后再执行 Gateway Chunk Commit；
- `FastDataStore` 使用 Chunk hash/长度做本地幂等写入；
- `IPlacementPolicy` 已有 RoundRobin、LeastUsed、DynamicScoring 扩展点；
- `NodeResourceGovernor` 已提供上传/下载本地准入；
- V4.3 已有 HTTP Keep-Alive 连接池、SharedBodyBlock、磁盘有界队列和性能指标。

### 2.2 必须补齐的能力

| 能力 | 当前状态 | V4.4 目标 |
|---|---|---|
| 数据完整性 | SHA-256、Chunk Commit | 保持不变 |
| Chunk 幂等 | 已有 | 纳入版本化元数据状态机 |
| RF=2 链式复制 | 已有 | 增加副本状态与修复任务 |
| 对象可见性 | 失败时可进入 `PROTECTING` | 明确 `COMMITTED/DEGRADED/FAILED` 语义 |
| Gateway 元数据 | 单实例 LevelDB | 3 节点 Metadata Raft |
| Gateway HA | 单实例 | 多 Gateway + 外部 L4/L7 负载均衡 |
| 节点检测 | 不完整 | Heartbeat、SUSPECT、OFFLINE、RECOVERING |
| 副本修复 | 无完整自动流程 | Coordinator 生成、DataNode 执行、幂等确认 |
| Placement | 有策略接口和准入 | 负载感知 + 稳定映射 + 故障隔离 |
| 扩缩容 | 无 | 先做新增节点修复，后做主动迁移 |

## 3. 关键设计原则

### 3.1 Gateway 可以转发，但不应该承担数据转发

从功能上说，Gateway 完全可以实现：

```text
Client → Gateway → DataNode
```

但这会产生额外的数据拷贝和网络带宽消耗，使 Gateway 成为所有大文件的中心瓶颈；Gateway 线程还会
同时承担路由、租约和元数据请求。V4.4 继续采用：

```text
Client → Gateway（控制请求）
Client → DataNode A（文件字节）
DataNode A → DataNode B（副本字节）
```

Gateway 只返回路由和能力令牌，不代理正常文件 Body。只有管理 API、故障切换、重新获取路由等控制
请求经过 Gateway。

### 3.2 Raft 复制小状态，不复制大文件

Raft 只负责以下小型、可序列化的元数据：

```text
object_id / file_id
object_key / size / file_hash
chunk_hash / chunk_index / chunk_size
replica_set / replica_state
object_state / version
placement_epoch
```

4 MiB 或 1 GiB 的文件字节仍走 DataNode 数据面。这样既保留 miniDriver 的高吞吐路径，也使 Gateway
可以从任意 Metadata Leader 获取一致的路由和状态。

### 3.3 先定义可见性，再定义故障恢复

对象对外可见必须由元数据状态决定，而不是由某个 DataNode 本地“看起来写完了”决定。

```text
ALLOCATED
  ↓ 本地写入 + 副本写入
REPLICA_ACKED
  ↓ Metadata Leader 提交状态
COMMITTED
```

异常分支：

```text
本地完成、副本失败
  → DEGRADED/PROTECTING（默认不可作为完整 RF 对象承诺）
  → Repair 成功
  → COMMITTED 或 HEALTHY

本地失败 / hash 不匹配
  → FAILED
  → 客户端重试或清理临时数据
```

V4.4 第一版建议保留当前 `PROTECTING` 兼容行为，但必须把它从“文件可用但副本不足”改成显式策略：

- **严格模式**：副本数未达到目标 RF，不对外报告成功；
- **降级模式**：允许已完成本地数据读取，但响应带 `DEGRADED`，并创建高优先级 Repair 任务。

默认使用严格模式，压测或恢复演示时显式开启降级模式。

## 4. V4.4 目标架构

```text
                         Client
                           │
                    L4/L7 Load Balancer
                       │            │
                 Gateway-1      Gateway-2
                       \            /
                        \          /
                    Metadata API / Coordinator
                     M1 ─── M2 ─── M3
                       \    Raft   /
                         Placement
                              │
                 ┌────────────┼────────────┐
                DN1          DN2          DN3
                 │   data + replica + repair │
                 └────── Heartbeat ─────────┘
```

### 4.1 组件职责

| 组件 | V4.4 职责 |
|---|---|
| Gateway | 无状态 HTTP 控制 API、令牌验证、向 Metadata Leader 转发状态请求 |
| Metadata Node | Raft 状态机、对象/Chunk/副本状态、Lease、Placement epoch |
| Coordinator | 节点健康检测、Repair 计划、任务幂等、故障转移协调 |
| DataNode | 原始数据、Chunk 写入、复制、Repair Source/Target、资源快照 |
| Load Balancer | Gateway 实例级健康检查和流量转发 |
| Repair Worker | 按 Coordinator 生成的任务从健康副本复制到新节点 |

第一版可以将 Coordinator 作为 Metadata Leader 内的模块，避免过早拆成独立服务；后续负载或代码规模
足够时再拆进程。

## 5. 元数据状态模型

### 5.1 对象和 Chunk 元数据

```text
ObjectRecord {
  objectId
  fileHash
  size
  chunkSize
  version
  state: UPLOADING | COMMITTED | DEGRADED | DELETING | DELETED | FAILED
}

ChunkRecord {
  objectId
  index
  chunkHash
  size
  desiredRf
  replicas: [ReplicaRecord]
  state: ALLOCATED | WRITING | REPLICA_ACKED | COMMITTED | REPAIRING | FAILED
}

ReplicaRecord {
  nodeId
  state: WRITING | HEALTHY | SUSPECT | OFFLINE | REPAIRING
  generation
  lastVerifiedHash
}
```

### 5.2 Commit 事务

当前代码的 `GatewayState::commitChunk()` 已验证 Session、Lease、Chunk hash、长度和成功节点列表，
再持久化 Session/Route。这是 V4.4 的良好入口，但要将写入改为 Metadata Raft command：

```text
CommitChunk(session, index, hash, size, successfulNodes, leaseId)
  → Leader 校验 Lease/epoch
  → append Raft log
  → majority apply
  → 返回 committed index/version
```

文件 Commit 只能引用已经处于合法状态的 Chunk。不能因为客户端重试或 Gateway 重启而重复增加副本、
重复释放 Lease 或创建不同版本的对象。

## 6. Metadata Raft 范围与实现策略

### 6.1 Raft 只承载什么

Raft 状态机至少包含：

- 节点注册、节点 epoch、Placement epoch；
- Session、Lease、ChunkRecord、ObjectRecord；
- `CommitChunk`、`CommitFile`、`MarkNodeOffline`、`CreateRepairTask`、`FinishRepair`；
- 对客户端可见的 leader/version 信息。

### 6.2 第一版不做什么

- 不让 Raft 日志承载文件 Body；
- 不一开始做跨地域 Raft；
- 不把每个 64 KiB Block 写入 Raft；
- 不承诺任意网络分区下同时可写；
- 不把 LevelDB 直接当作多节点一致性存储。

### 6.3 推荐实施顺序

1. 先抽象 `IMetadataStore`，把现有 `GatewayState` 的内存/LevelDB 读写从 HTTP Handler 中分离；
2. 实现单节点 `MetadataStateMachine`，保证命令和快照可重放；
3. 再实现 Raft 的 Leader election、term、log replication、commit index、snapshot；
4. 让 Gateway 只接受 Leader 写入，Follower 的写请求返回可重试的 Leader 地址/epoch；
5. 迁移现有 commit 和 lease API，最后再接 DataNode heartbeat 和 Repair。

如果目标首先是面试展示，可以先实现一个明确限制的 3 节点 Raft：支持单故障、稳定网络和重新选主，
并通过故障实验说明不覆盖的网络分区边界；不要宣称“生产级 Raft”。

## 7. 节点健康检测与故障转移

### 7.1 状态机

```text
JOINING → ONLINE → SUSPECT → OFFLINE
                    ↑          ↓
                 RECOVERING ← 节点重新上线
```

推荐初始参数：

- DataNode 每 2 秒发送 heartbeat；
- 连续约 5 秒无响应进入 `SUSPECT`；
- 约 10 秒确认 `OFFLINE`；
- 状态变化必须由 Metadata Leader 写入 Raft；
- 故障节点的旧写请求带旧 epoch 时必须被拒绝，防止恢复后的 stale writer 覆盖新状态。

### 7.2 Gateway 故障

Gateway 尽量无状态化：

- Session、Lease、Route 和 Commit 状态全部来自 Metadata Cluster；
- Gateway 本地只保留短期缓存，缓存必须带 metadata version/epoch；
- Load Balancer 健康检查失败后摘除 Gateway；
- 客户端遇到连接失败可重试另一个 Gateway，不能重新创建不同 Session。

## 8. Replica Repair / Self-Healing

### 8.1 修复流程

```text
Heartbeat 检测 DN1 OFFLINE
  → Coordinator 扫描 ChunkRecord
  → 找到 replicas=[DN1, DN2] 且 desiredRf=2
  → 选择 DN3 作为目标
  → 创建唯一 repair_task(object, chunk, source, target, generation)
  → DN2 → DN3 流式复制
  → DN3 校验 chunkHash/size
  → Metadata Leader 提交 FinishRepair
  → replicas=[DN2, DN3]，状态 HEALTHY
```

### 8.2 幂等与安全

- Repair task 使用确定性任务键，重复扫描不会创建无限任务；
- Target 先写临时 extent，hash/长度确认后再切换为可见副本；
- Source 必须是健康副本，不能从 `SUSPECT/OFFLINE` 节点读取；
- Repair 期间保留 generation，旧节点复活后不能覆盖新副本列表；
- Repair 失败可退避重试，超过次数进入 `REPAIR_BLOCKED` 并报警；
- Repair 不应占满前台上传槽，使用独立且有界的后台预算。

## 9. Load-aware Placement

### 9.1 节点快照

Metadata/Coordinator 周期性维护：

```text
nodeId
healthState
activeUploads / activeDownloads
diskQueueDepth / diskPauseMs
freeSpace
eventLoopLag
placementEpoch
```

### 9.2 选择策略

第一版可采用：

```text
eligible = ONLINE 且容量、磁盘空间、能力满足要求的节点
score = α*activeUpload
      + β*diskQueueDepth
      + γ*normalizedDiskUsage
      + δ*eventLoopLag
      + ε*recentErrorRate
```

选择不同故障域中得分较低的节点，再用 `objectId/chunkIndex` 做稳定扰动，避免所有请求集中到同一节点。

后续再考虑 Rendezvous Hashing：先保证对象到节点的稳定映射，再在允许范围内用负载权重调整。
不能只凭瞬时 active upload 选择节点，否则会在高并发下发生抖动；必须使用带时间窗口的快照和
Placement epoch。

## 10. 关键故障实验

### 实验 A：Gateway HA

```text
启动 Gateway-1/Gateway-2 + Metadata 3 节点
客户端创建 Session
停止 Gateway-1
客户端通过 Gateway-2 继续 routes/chunk commit/file commit
验证 Session、Lease、Chunk 状态不丢失
```

### 实验 B：Metadata Follower 故障

```text
停止 Follower-1
继续写入和读取
验证 Metadata majority 仍能提交
停止 Leader
等待重新选主
验证新 Leader 继续提供写入
```

### 实验 C：DataNode 副本修复

```text
RF=3 对象写入 DN1/DN2/DN3
停止 DN1
Heartbeat → SUSPECT/OFFLINE
Coordinator 创建 DN2 → DN4 repair task
DN4 校验 hash 后提交 FinishRepair
验证副本集合恢复为 DN2/DN3/DN4 中的目标 RF
```

### 实验 D：负载感知放置

```text
人为让 DN1 的磁盘写入变慢
比较 RoundRobin 与 Load-aware Placement
记录各节点 active upload、disk queue、pwrite P95、Chunk P95
验证新上传逐步减少进入 DN1，而不是承诺立即迁移已有对象
```

## 11. 指标与验收标准

### 一致性

- Commit 后对象的元数据版本在 Gateway 重启/切换后不丢失；
- 重复 Commit、重复 Repair、重复 Heartbeat 不产生重复记录；
- 严格模式下副本未达到目标 RF 不报告完整成功；
- 所有可见 Chunk 的 hash/size 与 DataNode 实际数据一致。

### 高可用

- 单 Gateway 故障时新请求可切换；
- Metadata 3 节点允许单节点故障继续提交；
- Leader 重新选举后不产生两个可写 Leader；
- stale epoch 的写入被拒绝。

### 自愈

- Node OFFLINE 后在检测窗口内生成 Repair；
- Repair 完成后 RF 恢复；
- 修复任务有界、可重试、可观测，不饿死前台上传/下载。

### 性能

- 保留 V4.3 的 Body、SHA、pwrite、Replica wait、BlockPool、pause 和 EventLoop 指标；
- 新增 Metadata command latency、Raft apply latency、heartbeat lag、repair throughput/lag、placement score；
- 对比 V4.3 基线，不允许控制面 HA 让数据面出现无界队列或异常 400。

## 12. 实施顺序

```text
Phase 0：抽象 MetadataStore + 明确状态机/版本/epoch
    ↓
Phase 1：单节点 MetadataStateMachine + snapshot/replay
    ↓
Phase 2：3 节点 Raft + Gateway 无状态化 + Gateway failover
    ↓
Phase 3：Heartbeat + Node health state + stale epoch 拒绝
    ↓
Phase 4：Repair task + DataNode 流式修复 + RF 恢复
    ↓
Phase 5：Load-aware Placement + 负载/故障/HA 综合实验
```

每一阶段都必须先有单元/集成测试，再做故障压测；不要一开始同时修改 Gateway、DataNode、Raft 和
Placement，否则出了问题无法判断是状态机、网络还是副本数据路径的问题。

## 13. 与 Codojo 学习流程的衔接

当前 S3 学习在 1.1 的实践环节暂停，进度文件保留不变。V4.4 设计完成后建议这样继续：

1. 先回到 S3 完成 1.1 实践；
2. 学完 V4.3 现有数据面模块，直到能解释当前 Commit/Replica/Backpressure；
3. 将本文件作为新的模块加入学习计划，重点学习 Metadata State Machine、Raft、Heartbeat、Repair 和 Placement；
4. 学习完成后再进入 S4，按 Phase 0 → Phase 5 实际改造代码。

这样可以避免在还没有完全掌握现有数据面时，直接把一致性协议叠加进去。

## 14. 最终定位

V4.3 的定位是：

> 高性能、可观测、有资源边界的分布式对象数据面。

V4.4 的定位是：

> 具备元数据一致性、Gateway 高可用、节点故障检测和副本自愈的分布式对象存储系统。

这不是简单增加几个 API，而是从“多节点协同传输”进入“分布式状态可恢复”的阶段。
