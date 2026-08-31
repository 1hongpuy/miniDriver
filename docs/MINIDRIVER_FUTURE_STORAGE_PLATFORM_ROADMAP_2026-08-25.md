# MiniDriver：对象存储与数据基础设施后续设计路线（2026-08-25）

> 项目定位：C++ 实现的学习型分布式对象存储。它独立提供可靠的对象写入、读取、版本、复制和元数据一致性；
> 不把 AI 模型、向量库或 RAG 逻辑放进存储系统。
>
> 当前工作主线：V3-Lite（`1 Gateway + 3 Metadata/Raft + 3 DataNode`）。
> 起始基线：V2.0.1 数据面；它只是迁移起点，不是正在发布的 V3 版本。

## 1. 最终目的

MiniDriver 要回答的是一个比“能上传文件”更基础的问题：

```text
一个大对象如何被可靠地写入、验证、复制、定位、读取并在节点故障后恢复？
```

它可以服务网页、C++ 客户端、Python 数据处理系统或未来的 S3 Adapter；但它不应以某一个上层业务为前提。

```text
Browser / CLI / CineLake / future business
                  │
                  ▼
          MiniDriver Gateway
                  │ control metadata
                  ▼
       Metadata Raft StateMachine
                  │ route / replica truth
                  ▼
        DataNode replicated object bytes
```

### 1.1 MiniDriver 的数据契约

上层系统只依赖已提交对象的稳定描述，而不是 DataNode 的磁盘路径：

```text
objectId          逻辑对象身份，重命名不改变它
objectVersion     内容版本；内容重建后递增
metadataVersion   元数据状态版本；用于缓存和条件读取
contentHash       内容完整性校验
state             UPLOADING / COMMITTED / DEGRADED / DELETED ...
read capability   受限、短期的读取授权（目标能力）
```

因此未来 CineLake、备份工具或 S3 Gateway 都应通过 Gateway/Storage API 读取对象，不能猜测
`data/files/...` 的本地路径。

## 2. 已有 V2 数据面能力与真实边界

V2 已经提供了值得保留的高性能数据面基础：

- Gateway 控制请求与 Client → DataNode 文件 Body 直传；
- 4 MiB Chunk、断点续传、SHA-256 与 HMAC Upload Capability；
- DataNode → DataNode 链式副本；
- Multi-Reactor、HTTP Keep-Alive、`SharedBodyBlock`；
- 有界 DiskWriteExecutor、64 KiB BlockPool、socket pause/resume；
- 基础 heartbeat、Admission 和新写入 Placement；
- File Commit 后的 LevelDB Outbox → Redis Stream 事件。

它仍有明确限制：Gateway 的 `GatewayState + LevelDB` 仍是控制状态中心；副本失败路径可能降级；没有
Raft 多数派元数据、严格 fencing、自动 Repair 或 Gateway 故障切换。V3 的工作不是推翻高速 Body 路径，而是把
“谁拥有元数据真相”从单进程搬到可复制的控制面。

## 3. 分层设计：让协议、业务和存储各自演进

HTTP/1.1 并不是问题。浏览器上传、大文件流式 Body、DataNode 直连都适合继续使用 HTTP。真正要避免的是
HTTP Handler 直接拥有业务状态，导致以后切换内部 RPC 或添加 S3 兼容时必须重写规则。

```text
Transport Layer
  HTTP/1.1 public API | internal HTTP RPC | future gRPC/S3 adapter
          │ DTO / header / authentication translation
          ▼
Application Layer
  UploadService | MetadataClient | NodeControlClient | ObjectReadService
          │ stable C++ request/result interfaces
          ▼
Domain Layer
  MetadataCommand | MetadataStateMachine | placement | repair policy
          │ deterministic business rules
          ▼
Infrastructure Layer
  RaftAdapter | LevelDB/WAL/snapshot | TCP/HTTP client | DataNode disk
```

关键规则：

- `MetadataStateMachine` 不知道 HTTP header、socket、MySQL、Redis 或 DataNode 文件路径；
- Raft 只复制小型 `MetadataCommand`，绝不复制 Chunk Body；
- Redis 是 AI/媒体等异步事件媒介，不是对象或副本元数据的最终真相；
- HTTP API 在 V3-Lite 保持兼容；未来 gRPC 是内部传输替换选项，不会自动提供一致性；
- 如需 S3，做 `S3Gateway/Adapter → Application Layer`，而不是把 S3 语义散入 DataNode。

### 3.1 当前 I/O 耦合审计：先治理控制面，不重写高速 Body 路径

当前上传路径的真实协作关系如下：

```text
Browser HTTP PUT
  → TcpConnection / HttpContext
  → ChunkUploadStream（当前位于 datanode_main.cpp）
      ├─ ChunkDiskWritePipeline
      │    → DiskWriteExecutor
      │    → FastDataStore::WriteSession
      ├─ ReplicaUploadPipe
      │    → HTTP Client → Replica DataNode
      └─ GatewayControlClient
           → /internal/v2/chunk-commits → GatewayState::commitChunk
```

这不是“所有耦合都必须消灭”。上传过程中让同一块 Body 同时进入本地写盘和下游副本，且任一队列饱和时暂停
上游 socket，是已有 V2 背压语义的核心：

```text
SharedBodyBlock
  → local disk queue
  → replica network queue
  → high watermark
  → pauseRead()
  → drained
  → resumeRead()
```

以下判断决定改造顺序：

| 位置 | 当前判断 | V3-Lite 动作 |
|---|---|---|
| `TcpConnection` ↔ HTTP | 低耦合；网络层通过 callback 工作 | 保持不动 |
| `HttpContext` ↔ 流式 Body pause/resume | 有意的中等耦合；保证背压 | 保持语义和测试不动 |
| `DiskWriteExecutor` ↔ `FastDataStore` | DataNode 本地实现耦合，当前合理 | 不为“可替换”提前抽象 |
| `ReplicaUploadPipe` ↔ HTTP Client | 副本传输协议耦合较高 | 后续用 ReplicaTransport Adapter 包装；本轮不换协议 |
| `ChunkUploadStream` ↔ 磁盘/副本/Gateway/CORS | 上传协调职责过重 | 先迁出 `datanode_main.cpp` 为 Coordinator，不改算法 |
| Gateway HTTP Handler ↔ `GatewayState` | 高耦合 | **本轮最高优先级治理** |
| `GatewayState` ↔ 元数据/Placement/媒体任务/Outbox | 全能状态类，耦合最高 | 用 MetadataStateMachine/Facade 分步剥离 |

因此 V3-Lite 的原则是：**先抽控制面边界，后整理 DataNode 协调器；不为了“架构漂亮”而重写已验证的零拷贝、
有界队列和 pause/resume 数据面。**

### 3.2 目标接口边界

Gateway 的 public HTTP Handler 只负责协议适配：解析 JSON/header、验证外部 Capability、调用应用服务、把领域结果
映射成 HTTP 状态码。它不应直接决定元数据写入。

```text
HTTP Handler
  → HTTP DTO / JSON translation
  → MetadataFacade（协议无关）
  → LegacyGatewayStateFacade（过渡实现）
  → future MetadataClient → Raft leader
```

第一批 `MetadataFacade` 只覆盖 V3-Lite 所需控制操作：

```text
createSession / preflight
reserveLease / planRoutes
commitChunk / commitFile
registerNode / heartbeat
queryObject / queryManifest
```

DataNode 的控制面已有正确起点：`GatewayControlClient` 是语义接口，`ControlRequestCodec` 才把它编码为当前的
HTTP method/path/body。Raft 接入后，逐步将其语义改为 `MetadataControlClient` 或经 Gateway 转发，但不让
`ChunkUploadStream` 直接拼接 Metadata HTTP 请求。

DataNode 的后续形态为：

```text
HTTP PUT Adapter
  → ChunkUploadCoordinator
      → LocalChunkWriter（现有 FastDataStore + DiskWriteExecutor）
      → ReplicaWriter（现有 ReplicaUploadPipe）
      → ControlPlaneReporter（现有 GatewayControlClient）
```

第一步仅把协调类从 `datanode_main.cpp` 移到独立模块并补回归测试；不改变其 shared block、队列水位线、磁盘写入或
副本 ACK 行为。未来需要 gRPC、QUIC 或专用副本协议时，只替换 `ReplicaWriter/ControlPlaneReporter` 的 Transport
Adapter，不改变上传业务和写盘规则。

## 4. 演进阶段

### S0：V2 数据面基线（已完成）

目标是可靠、高吞吐的单 Gateway 对象传输。保留现有压力/校验基线，并冻结 V2 语义作为 V3 回归对象。

### S1：V3-Lite MetadataStateMachine（已完成第一版）

先完成可确定重放的单节点业务状态机：

```text
同一初始快照 + 同一 MetadataCommand 日志顺序
  → 三个节点得到相同 Object / Lease / Replica / Node 状态
```

已引入/验证的核心概念：`commandId` 幂等、`metadataVersion`、`placementEpoch`、`nodeEpoch` 与 Repair
`generation` 的围栏语义，以及 snapshot/restore。此阶段尚未接入真正 Raft。

### S1.5：控制面边界治理（下一步，Raft 前的低风险重构）

目标是让 Gateway HTTP 与当前 `GatewayState` 解耦，同时不改变 V2 外部 API 和 DataNode Body 流。

```text
1. 定义 MetadataFacade 的协议无关 request/result；
2. 实现 LegacyGatewayStateFacade，把已有 GatewayState 作为临时后端；
3. 将 upload preflight/session/route/chunk commit/file commit/heartbeat Handler 迁到 Facade；
4. 对照现有 HTTP 集成测试，验证 JSON、HTTP 状态码、Capability 和背压行为没有变化；
5. 将 ChunkUploadStream 迁出 main，作为独立 Coordinator；只做文件/依赖整理与现有行为回归。
```

停止条件：若为了 Facade 改变了 Chunk Body 路径、取消了 socket pause/resume，或让某个 HTTP Handler 重新直接修改
Metadata 内存状态，则本阶段不通过。

### S2：三节点 Raft 控制面

采用成熟 C++17 Raft 库，通过薄 `RaftAdapter` 连接状态机：

```text
Gateway / Metadata RPC
  → Leader propose(commandId, encodedCommand)
  → 2/3 majority commit
  → ordered StateMachine apply
  → ApplyResult(term, logIndex, metadataVersion)
```

验收重点不是“启动了三个进程”，而是 Follower 停止仍可提交、Leader 停止可重新选主、重启可从 WAL/snapshot
恢复、相同 `commandId` 重试不重复写。

### S3：Gateway 接入一致性 Metadata

将 Session、Lease、Route、Chunk/File Commit 从 `GatewayState` 逐项改为 `MetadataCommand`；浏览器 URL 与 Body
直传路径尽量不变。Gateway 只做 HTTP 适配、Capability 签发和调用 Metadata，不再是对象状态唯一来源。

### S4：节点健康、严格副本与 Repair

```text
heartbeat → ONLINE / SUSPECT / OFFLINE / RECOVERING
OFFLINE  → 新写入排除节点 → 生成确定性 RepairTask
healthy source → target temporary extent → hash verify → FinishRepair(generation)
```

默认严格语义：只有所有 Chunk 达到 `desiredRf`，对象才是 `COMMITTED`；否则明确显示
`PROTECTING/DEGRADED/FAILED`，不伪装为完整双副本。

### S5：Docker Compose 与三主机演示

本机先用 Compose 管理 Gateway、3 Meta、3 DataNode 的独立 volume、端口、日志和启动顺序；通过后再按已有
Tailscale 三主机拆分部署。Docker 负责进程与配置可重复性，不能取代 Raft quorum、hash 校验或故障验收。

### S6：V3.1 多 Gateway 与用户边界（后续）

只在 V3-Lite 已验证后再引入：

```text
stable domain / L4-L7 LB
  → 3 stateless Gateway
  → same 3 Metadata Members

shared MySQL
  → users / password hash / refresh-token records / ownerUserId
```

MySQL 管用户身份与登录会话；Raft 管对象、Lease、Route、副本与 Repair。二者的可用性要求、数据模型和一致性模型不同，
不能互相替代。

### S7：可选标准接口与规模能力（按证据决定）

- Worker 短期 Read Capability 与对象删除 tombstone；
- S3-compatible Gateway 或独立 Adapter；
- Gateway 横向扩展、读路径 failover；
- 长期再平衡、节点安全移除、更多 failure domain；
- 可观测性、限流与容量管理。

Kafka、Kubernetes、跨机房、完整 S3 兼容不应抢占 S2～S4 的正确性工作。

## 5. 与 miniAI/LensAI 的接入边界

```text
MiniDriver File COMMITTED
  → durable Outbox Event（至少一次）
  → Redis Stream / future event adapter
  → miniAI durable IndexJob
  → 受控读取固定 objectId + objectVersion
```

MiniDriver 的责任止于：对象已提交、事件可最终发布、读取被授权、对象版本真实且可查询。缩略图、EXIF、向量、模型队列、
RAG、Agent 都属于 miniAI；AI 失败不能阻塞上传或损坏原对象。

## 6. 每一阶段的硬验收

| 阶段 | 最小证据 |
|---|---|
| S1 | 重放/快照/重复 commandId 测试通过，状态摘要一致 |
| S2 | 三节点唯一 Leader；任一 Meta 故障后 2/3 仍 commit；重启恢复 |
| S3 | Gateway 重启后可继续已有 Session/Commit，不读取旧本地真相 |
| S4 | DN 故障后新 Route 避开它；从健康副本修到第三节点；SHA-256 一致 |
| S5 | 一条可重复 Compose/三主机启动命令、独立 volume、故障日志和时间线 |
| S6 | 任意 Gateway 切换不丢认证上下文与 Metadata 业务幂等 |

## 7. 不应提前承诺的能力

MiniDriver 不是 MinIO 的替代品：当前没有完整 S3 生态、成熟 IAM、跨地域纠删码、生命周期策略、生产级监控或
大规模运维经验。它的价值是把这些底层问题中最关键、最可学习的部分做到可以验证：流式传输、背压、一致元数据、
fencing、复制和自愈。

相关的具体 V3-Lite 协议与字段见
[V3-Lite 控制面设计](MINIDRIVER_V3_LITE_CONTROL_PLANE_DESIGN_2026-08-22.md)。
高并发、资源预算、尾延迟与性能验收的并行任务见
[高并发与高性能数据面实施方案](MINIDRIVER_HIGH_CONCURRENCY_AND_PERFORMANCE_IMPLEMENTATION_PLAN_2026-08-25.md)。

## 8. 分阶段实施任务板

下面的顺序刻意遵循“先确定业务状态，再复制状态，再迁移入口，最后处理节点故障”的依赖关系。一个阶段没有通过
验收时，不应通过添加更多服务来掩盖问题。

### T0：V3 基线、配置与回归（已完成骨架）

**目的：** V3 的实验和运行目录不能污染 V2；后续每次重构都有可比较的数据面基线。

- [x] 在 `v3` 分支保留 V2.0.1 数据面作为迁移起点；
- [x] 建立 V3 独立配置、数据/日志目录和本机三 Meta 的端口约定；
- [x] 保存构建、CTest、上传/下载/mixed 与 SHA-256 基线；
- [x] 增加时钟、网络延迟/断线、进程停止等故障注入骨架；
- [ ] 固化单机与三主机启动/停止/日志收集脚本的统一入口；
- [ ] 在每次阶段验收中自动保存版本、配置摘要、命令、日志与结果 CSV。

**停止条件：** 无法复现 V2 基线，或 V3 使用了 V2 的运行目录时，先修复环境，不继续改控制面。

### T1：确定性 MetadataStateMachine（已完成第一版）

**目的：** 把“对象、Session、Lease、副本和节点状态如何变化”定义为可重放的纯业务规则；Raft 以后只负责复制
命令顺序。

- [x] 定义 `MetadataTypes`：Object、Session、Chunk、Replica、Lease、Node、RepairTask；
- [x] 定义带 `commandId/schemaVersion/epoch/generation` 的 `MetadataCommand` 与 `ApplyResult`；
- [x] 实现 CreateSession、RegisterNode、Heartbeat、MarkNodeHealth、ReserveLease、CommitChunk、CommitFile；
- [x] 实现确定性 `apply/query/snapshot/restore/stateDigest`；
- [x] 实现命令二进制编解码、重复 commandId 结果账本、版本/epoch/generation fencing；
- [x] 增加状态机、快照恢复、重放与 fencing 自动化测试；
- [ ] 在 Raft 接入后由 Raft WAL/snapshot 持久化状态机快照；不再引入第二份竞争命令日志。

**停止条件：** 固定命令日志重放、snapshot restore 或重复 Commit 有任一不一致，不能进入 Raft。

### T1.5：控制面边界治理（下一步）

**目的：** 让协议适配、控制面业务和当前 LevelDB 后端分离，为 Raft 后端替换做准备；不改变经过验证的 Body
传输和背压算法。

#### Gateway 任务

- [ ] 在 `include/control/` 定义协议无关的 `MetadataFacade`：请求/结果只表达 Session、Route、Commit、
  Node health、Object/Manifest query，不出现 HTTP header 或 JSON 字符串；
- [ ] 实现 `LegacyGatewayStateFacade`，把上述请求临时委托给 `GatewayState`；
- [ ] 按小批次迁移 Gateway Handler：`preflight/session` → `routes/lease` → `chunk/file commit` →
  `node register/heartbeat` → `object/manifest query`；
- [ ] 让 `gateway_main.cpp` 只保留路径分发、DTO 解析、外部 Capability 校验和 Facade result → HTTP response 映射；
- [ ] 维持现有 `/api/v2/...` URL、JSON 字段、HTTP 状态码和浏览器兼容性；
- [ ] 将媒体任务/AI Outbox 继续视为现有兼容模块，标记其未来从 `GatewayState` 迁到 committed Metadata event 的位置，
  但本阶段不重做 AI 流。

#### DataNode 任务

- [ ] 把 `ChunkUploadStream` 从 `src/DataNode/datanode_main.cpp` 提取为
  `ChunkUploadCoordinator` 独立头/源文件；
- [ ] 保持 `ChunkDiskWritePipeline`、`FastDataStore`、`DiskWriteExecutor`、`ReplicaUploadPipe` 与
  `GatewayControlClient` 的现有调用顺序不变；
- [ ] 将 HTTP Header/UploadCapability 的解析留在 HTTP PUT Adapter，将协调器输入收敛为已验证的 UploadContext；
- [ ] 保留 `GatewayControlClient + ControlRequestCodec` 作为 DataNode 控制语义与当前 HTTP 编码的边界；
- [ ] 对现有 `SharedBodyBlock`、高/低水位线、`pauseRead()/resumeRead()`、副本 ACK 增加明确回归断言。

**验收：** HTTP Handler 不再直接写 `GatewayState`；Chunk Body 不流入 Facade/Raft；上传/下载 SHA-256、503 背压和
队列上限与基线一致。

### T2：成熟 Raft Adapter 与三节点 Metadata（下一阶段）

**目的：** 让三个 Metadata Member 对同一命令日志达成多数派一致，并从故障中恢复。

- [ ] 对候选 C++17/CMake Raft 库完成最小 PoC：选主、2/3 提交、重启恢复、snapshot、持久化目录和许可证；
- [ ] 定义 `RaftAdapter`、`RaftConfig`、`LeaderHint`、proposal/read-index result；
- [ ] 启动独立 Meta-1/2/3 进程，每个进程拥有独立 WAL、snapshot 与数据目录；
- [ ] 将 `MetadataCommand` 编码后 propose；仅在 majority commit 的 apply callback 中调用 StateMachine；
- [ ] 实现 Follower 的 `NOT_LEADER + leaderHint + term`，并记录 leader/term/index/apply latency；
- [ ] 将 Raft 网络/磁盘线程回调投递到 Metadata EventLoop，禁止直接操作 HTTP `TcpConnection`；
- [ ] 注入 Follower 停止、Leader 停止、网络分区、proposal 超时、重复响应和进程重启；
- [ ] 用 `stateDigest`、snapshot restore 与 log index 证明三个 Member 最终一致。

**验收：** 停止任一 Follower 后仍能提交；停止 Leader 后可重新选主；少数派拒绝写；重启不会丢失已提交状态。

### T3：Gateway 接入 Raft Metadata（下一阶段）

**目的：** Gateway 成为无 Metadata 真相的 HTTP 入口；当前 `LegacyGatewayStateFacade` 被真正的
`RaftMetadataClient` 替换。

- [ ] 实现 `MetadataClient` 访问三 Metadata RPC endpoint，并缓存/刷新 Leader Hint；
- [ ] 将 T1.5 Facade 后端替换为 `RaftMetadataClient`，不改变 Handler 与浏览器协议；
- [ ] 对一次浏览器业务动作生成稳定 commandId；网络超时、Gateway 重试和 Leader 切换必须沿用同一个 ID；
- [ ] 将 Session、Lease、Route、Chunk/File Commit 的最终读写全部迁入 StateMachine；
- [ ] 需要线性一致性的 Session/Lease/Route/replica 查询走 Leader 或 ReadIndex；列表可返回带
  `metadataVersion` 的受控陈旧结果；
- [ ] 迁移/清理旧 Gateway 本地控制状态读取路径，但保留明确的 V2 数据迁移或兼容策略；
- [ ] 绑定 File Commit 与 AI Outbox 到已提交的 Metadata 状态，禁止未 Commit 对象产生索引事件。

**验收：** Gateway 重启后已有 Session/Commit 仍可按同一 commandId 查询或重试；对象可见性只由 Raft apply 决定。

### T4：Node Health、严格副本语义与 Repair（下一阶段）

**目的：** 将“DataNode 不可用”和“副本不足”变成可追踪、可重试且不会被旧请求覆盖的闭环。

- [ ] 扩展 DataNode 注册/heartbeat：`nodeEpoch`、空间、活跃上传/下载、磁盘队列、pause 时间、event-loop lag；
- [ ] Leader 定时生成经 Raft 提交的 `ONLINE → SUSPECT → OFFLINE → RECOVERING` 状态变化；
- [ ] OFFLINE 后从新 Placement 排除节点；SUSPECT 不立即触发 Repair；
- [ ] 将 CommitChunk 的完整副本数显式写入 Chunk/Replica state；默认达不到 RF=2 不得把对象标成 COMMITTED；
- [ ] 实现 RepairTask 的确定性 `taskKey`、attempt、retryAt、generation 和状态转换；
- [ ] 实现 DataNode repair source/target 内部接口：临时 extent、流式写入、size/SHA-256 校验、原子 finalize/abort；
- [ ] 为前台上传和 Repair 使用独立有界槽；失败指数退避，有限次数后进入 `REPAIR_BLOCKED`；
- [ ] `FinishRepair` 必须校验 taskKey、source/target health、nodeEpoch、generation、hash 与长度后再更新 ReplicaSet。

**验收：** 停止 DN-1 后新 Route 不选它；DN-2 能向 DN-3 修复；旧 nodeEpoch 或旧 generation 的回调被 FENCED；
修复前后下载 SHA-256 相同。

### T5：Docker Compose、本机故障矩阵与三主机演示（下一阶段）

**目的：** 把已经正确的进程部署变成可重复的运行方式，而不是用容器掩盖一致性错误。

- [ ] 编写 `Dockerfile.minidriver`，固定运行用户、二进制、配置挂载和健康检查；
- [ ] 编写 `compose.v3.local.yaml`：Gateway、Meta-1/2/3、DN-1/2/3，每个 Meta/DataNode 单独 volume；
- [ ] 提供 `up / status / logs / stop-node / start-node / clean-test-data` 的非破坏性脚本；
- [ ] 以容器 DNS 验证本机 Raft peer、Metadata RPC、DataNode route 与健康检查；
- [ ] 记录 Leader kill、Follower kill、DN kill、网络延迟/断线、Repair 的时间线和指标；
- [ ] 将同一配置拆为三台 Tailscale 主机上的 Compose，peer 使用 advertise address，持久目录不共享；
- [ ] 验证 AI/Redis 独立服务故障不影响 MiniDriver Metadata 与数据面。

**验收：** 一条文档化命令可启动本机集群；每个故障矩阵场景有日志、term/index、metadataVersion 与 SHA-256 证据。

### T6：V3-Lite 发布验收与 V3.1 准备（后续）

**目的：** 先对 V3-Lite 的单 Gateway 范围做诚实发布，再决定是否扩展入口、高层用户系统和标准协议。

- [ ] 执行 V2 数据面 upload/download/mixed 回归与 V3 控制面故障矩阵；
- [ ] 输出 Raft 开销、Repair lag、队列/背压、CPU/RSS 与未覆盖边界报告；
- [ ] 复查没有 UAF、无限队列、未解释 hash mismatch、过期 callback 覆盖或少数派写入；
- [ ] 冻结 V3-Lite API/MetadataCommand schema，并记录数据/快照兼容策略；
- [ ] 为 V3.1 写独立设计：3 stateless Gateway、LB/域名、共享 MySQL 用户系统、Gateway failover、
  Read Capability、可选 S3 Adapter；
- [ ] 仅在真实压力/运维痛点出现后评估 Kafka、k3s/Kubernetes、跨地域或更复杂再平衡。

**验收：** 可以清楚证明“一个 Metadata 或一个 DataNode 故障时系统如何保持一致或恢复”；不能证明的能力不写入发布说明。
