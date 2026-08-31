# miniDriver V3：控制面高可用、元数据一致性与副本自愈实施计划

> 版本目标：在已发布的 V2 数据面基础上，补齐控制面 HA、元数据一致性、节点故障检测、自动副本修复和负载感知放置。  
> 当前状态：计划阶段，尚未实现；V2 数据面保持冻结，V3 只增加控制面和自愈能力。  
> Git 约定：V2 保持 `v2.0.0` 不变；所有 V3 实现进入 `v3` 分支，全部验收通过后再创建 `v3.0.0`。

## 1. V3 要解决什么问题

V2 已经解决了“文件如何高效传输和保存”：

```text
Client → Gateway（控制请求）
Client → DataNode A（文件 Body）
DataNode A → DataNode B（链式副本）
```

但 V2 的控制状态仍主要由单个 Gateway 的 `GatewayState + LevelDB` 管理，副本故障后的自愈流程也
不完整。V3 要解决的是：

```text
Gateway 挂掉怎么办？
Metadata 节点之间谁说了算？
DataNode 离线后如何摘除？
副本不足如何恢复？
新增节点如何承接数据？
节点负载变化时如何选择新写入？
```

V3 的一句话目标：

> 让 miniDriver 从“可进行多节点协同传输的对象存储”升级为“控制面可恢复、节点可检测、
> 副本可自愈的分布式对象存储学习系统”。

## 2. V2 基线与当前真实语义

### 2.1 已有能力

- Gateway/DataNode 控制面和数据面分离；
- 4 MiB Chunk、断点续传、SHA-256、HMAC 能力令牌；
- Gateway WriteLease 与 DataNode 本地 Admission；
- 链式双副本和 HTTP ACK；
- Multi-Reactor、HTTP Keep-Alive 连接池、SharedBodyBlock；
- 有界 DiskWriteExecutor、64 KiB BlockPool、socket pause/resume；
- DataNode heartbeat、基础节点状态和负载感知初始放置；
- Redis Streams 媒体任务和异步日志。

### 2.2 必须明确的副本确认语义

当前代码的正常路径确实等待：

```text
本地 FastDataStore 完成
       +
下游 ReplicaUploadPipe 返回成功
       ↓
Gateway Chunk Commit
```

但当前失败路径是降级语义：副本请求发生错误时，`replicaCompleted_` 仍会结束等待；主副本可能以
`200 + replicaWarning` 返回，Gateway 记录的 `successfulNodes` 可能少于目标副本数。

因此 V3 不能笼统地宣称“当前一定是双副本成功”，必须把以下状态显式化：

```text
COMMITTED       达到目标副本数，可作为完整对象承诺
DEGRADED        本地数据可用，但副本数不足，存在 Repair Task
PROTECTING      正在等待副本确认或修复，默认不对外承诺完整 RF
FAILED          本地 hash/长度/写入失败
```

V3 默认采用严格模式：未达到目标 RF 不进入普通 `COMMITTED`。演示和故障恢复测试可以显式开启降级
读取模式，但响应必须带状态，不得伪装成完整副本。

### 2.3 当前“负载均衡”的准确定位

V2 的 `GatewayState::selectPlacementLocked()` 会根据空闲空间、CPU、内存、磁盘 I/O、网络和 active
upload 等数据为新 Chunk 选择候选节点。这是：

```text
写入前的 Placement + Admission
```

它不是完整 Load Balancer，因为目前没有统一实现：

- Gateway 实例流量分发和故障摘除；
- 活跃请求的安全重路由；
- 长期热点和数据倾斜再平衡；
- 节点新增后的主动迁移；
- 故障后的副本替换；
- placement epoch 对旧路由的 fencing。

V3 文档中将三者严格区分：

```text
Admission       防止资源超限
Placement       决定新数据初始放置
Load Balancing  持续分发、摘除、切换和再平衡
```

## 3. V3 目标拓扑

```text
                              Client
                                │
                         L4/L7 Gateway LB
                    ┌──────────┼──────────┐
                    │          │          │
                 Gateway-1  Gateway-2  Gateway-3
                    │          │          │
                    └──────────┼──────────┘
                         Metadata Client API
                    ┌──────────┼──────────┐
                    │          │          │
                 Meta-1     Meta-2     Meta-3
                    └────── Raft ────────┘
                         Coordinator
                    ┌──────────┼──────────┐
                    │          │          │
                  DN-1       DN-2       DN-3
                    └──── heartbeat/repair ────┘

正常数据路径：Client → DataNode → DataNode
控制状态路径：Gateway → Metadata Leader → Raft majority
```

### 3.1 三 Gateway

Gateway 尽量无状态：Session、Lease、Route、Commit 状态都存入 Metadata Cluster；Gateway 本地只保留带
`metadataVersion`/`placementEpoch` 的短期缓存。Gateway 故障时，客户端通过 LB 或备用地址访问另一实例，
不能重新创建一套不同的 Session。

### 3.2 三 Metadata

三个 Metadata 节点组成 Raft 组，至少两节点确认后才提交写命令。V3 主线不手写 Raft 协议，而是接入
成熟 Raft 库；我们自己实现 `MetadataStateMachine`、command 编码、状态校验、epoch fencing 和业务
幂等语义。

学习项目采用“嵌入 Gateway”部署：每个 Gateway 进程内包含一个 Metadata/Raft member。这样 Gateway
业务层调用本进程的 Metadata facade 不需要额外 RPC，但三个嵌入式 Raft member 仍然必须通过独立的
内部端口互相通信。以后如果要拆成独立 `metadata_node` 进程，只需替换 `MetadataClient`，不改状态机。

### 3.3 三 DataNode

默认 RF=2 时，三个 DataNode 可以容忍一个节点故障并使用第三节点进行 Repair；两个 DataNode 同时故障
时不能保证对象可用。三节点适合学习和故障演示，但必须为 Repair 预留独立资源预算。

### 3.4 元数据节点的通信与进程边界

V3 采用“嵌入 Gateway、独立 Raft 内部端口”的部署方式：

```text
Gateway-1 = HTTP :8080 + MetadataFacade + Raft Member :18001 + LevelDB(meta-1)
Gateway-2 = HTTP :8080 + MetadataFacade + Raft Member :18002 + LevelDB(meta-2)
Gateway-3 = HTTP :8080 + MetadataFacade + Raft Member :18003 + LevelDB(meta-3)
```

“嵌入”只表示 Gateway 业务层调用本进程 MetadataFacade 时不需要额外进程间 RPC；三个 Raft Member 仍
必须通过网络通信。每个 Member 使用独立持久化目录，禁止共享 LevelDB 文件，也禁止通过复制 LevelDB
文件来实现一致性。

Raft 内部通信由成熟库的 Transport 或适配器承载，至少包含：

```text
RequestVote       选主投票
AppendEntries     日志复制与心跳
AppendEntriesAck  日志确认、matchIndex、term
InstallSnapshot   Follower 落后时传输元数据快照
```

公共 HTTP 请求和 Raft 内部通信使用不同端口、鉴权配置和日志字段。Raft 通道只同步小型
`MetadataCommand`，不承载文件 Body。

### 3.5 一次控制面写请求

```text
Client
  → 任意 Gateway 的 HTTP Handler
  → 本地 MetadataFacade
  → 本地 Raft 是 Leader：直接 propose
  → 本地 Raft 是 Follower：根据 leaderHint 转发给 Leader
  → Leader 写入 Raft Log
  → 复制给其他两个 Member
  → 多数派确认后 commit
  → 三个节点依次 apply MetadataStateMachine
  → 返回 metadataVersion/result
```

Gateway 对客户端隐藏 Leader 位置变化；客户端不需要重新创建 Session。转发请求必须携带稳定的
`commandId`，以便网络重试、Gateway 切换或重复响应时保持幂等。

读请求分为两类：需要线性一致性的 Lease、Route、副本状态查询必须走 Leader 或 Raft ReadIndex；允许
短暂陈旧的列表和监控查询可以读本地状态机，但响应应带 `metadataVersion`。

## 4. 关键一致性原则

### 4.1 Raft 只复制小型元数据

Raft 日志只携带：

```text
Session / Lease / ChunkRoute / ObjectRecord
ChunkReplicaSet / NodeHealth / RepairTask
objectVersion / placementEpoch / metadataVersion
```

不把 4 MiB Chunk、64 KiB Block 或文件 Body 写进 Raft。大数据继续走 DataNode 数据面。

### 4.2 写入必须经过 Leader 和多数派

```text
Gateway → Metadata Leader
  → Leader 校验 command、lease、epoch
  → append Raft log
  → majority commit
  → apply StateMachine
  → 返回 metadataVersion
```

Follower 对写请求返回 `NOT_LEADER + leaderHint + term`，Gateway 再向 Leader 重试。不能让三个节点都
独立接受写入。

### 4.3 版本和 fencing

每个 Route、Lease、Repair Task 都带：

```text
term / metadataVersion / placementEpoch / generation
```

DataNode 或旧 Gateway 使用过期 epoch 发送写入、Commit 或 FinishRepair 时，Metadata Leader 必须拒绝，
防止旧节点恢复后覆盖新副本列表。

## 5. 目标元数据模型

```cpp
struct ObjectRecord {
    std::string objectId;
    std::string fileHash;
    uint64_t size = 0;
    uint32_t chunkSize = 0;
    uint64_t version = 0;
    ObjectState state; // UPLOADING/COMMITTED/DEGRADED/DELETING/DELETED/FAILED
};

struct ChunkRecord {
    std::string objectId;
    uint32_t index = 0;
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredRf = 2;
    ChunkState state; // ALLOCATED/WRITING/ACKED/COMMITTED/REPAIRING/FAILED
    std::vector<ReplicaRecord> replicas;
};

struct ReplicaRecord {
    std::string nodeId;
    ReplicaState state; // WRITING/HEALTHY/SUSPECT/OFFLINE/REPAIRING
    uint64_t generation = 0;
    std::string verifiedHash;
};

struct RepairTask {
    std::string taskKey; // object/chunk/source/target/generation
    std::string sourceNode;
    std::string targetNode;
    uint64_t generation = 0;
    RepairState state;
    uint32_t attempts = 0;
    int64_t nextRetryAt = 0;
};
```

## 6. 代码框架

### 6.1 新增目录

```text
include/metadata/
  MetadataTypes.hpp
  MetadataCommand.hpp
  MetadataStateMachine.hpp
  IMetadataStore.hpp
  MetadataSnapshot.hpp

include/raft/
  RaftAdapter.hpp
  RaftConfig.hpp
  RaftCommandCodec.hpp

include/control/
  MetadataClient.hpp
  MetadataRpcCodec.hpp
  NodeHealthCoordinator.hpp
  RepairCoordinator.hpp
  PlacementCoordinator.hpp

src/metadata/
  MetadataStateMachine.cpp
  LevelDbMetadataStore.cpp
  MetadataSnapshot.cpp

src/raft/
  RaftAdapter.cpp
  RaftCommandCodec.cpp

src/control/
  MetadataClient.cpp
  NodeHealthCoordinator.cpp
  RepairCoordinator.cpp
  PlacementCoordinator.cpp

test/
  test_metadata_state_machine.cpp
  test_metadata_snapshot.cpp
  test_raft_election.cpp
  test_raft_replication.cpp
  test_gateway_failover.cpp
  test_node_health.cpp
  test_replica_repair.cpp
  test_load_aware_placement.cpp
```

### 6.2 现有文件的改造范围

| 现有文件 | V3 改造 |
|---|---|
| `include/gateway/GatewayState.hpp` | 从全能状态类拆出 Metadata command/query facade |
| `src/gateway/GategayState.cpp` | 保留 HTTP 兼容层，写操作改为提交 Metadata command |
| `src/gateway/gateway_main.cpp` | 改为无状态 Gateway，处理 Leader redirect/version |
| `include/DataNode/GatewayControlClient.hpp` | 增加 leader/epoch/retry 结果语义 |
| `src/DataNode/datanode_main.cpp` | heartbeat 带 epoch/资源快照；增加 repair source/target handler |
| `include/storage/IPlacementPolicy.hpp` | 增加 health、epoch、时间窗口负载快照 |
| `src/storage/IPlacementPolicy.cpp` | 稳定映射与负载评分分离 |
| `include/DataNode/FastDataStore.hpp` | 增加临时 repair extent、hash 验证和原子 finalize |
| `CMakeLists.txt` | 新增 metadata/raft/control 静态库及测试目标 |
| Node Agent 配置 | 增加 gateway/metadata/raft peer、节点 epoch、repair 预算配置 |

## 7. 分阶段实施方案

## Phase 0：冻结 V2、建立 V3 骨架

### 目标

保证 V2 可以回退，建立 V3 分支、配置和测试基线。

### 工作

1. 从 `v2.0.0` 创建 `v3` 分支；不修改 V2 标签；
2. 保存 V2 CTest、upload/download/mixed 和故障基线；
3. 增加 `MINIKV_V3_MODE=single|ha` 配置；
4. 增加可注入时钟、随机数和网络故障接口；
5. 建立 `docs/V3_*` 报告目录和测试产物格式。
6. 固定三节点本地开发拓扑：Gateway HTTP 端口、Raft 内部端口、元数据目录、DataNode 地址和节点 ID
   均由 YAML/启动参数提供，不写死在业务代码中；
7. 增加 `commandId/requestId` 贯穿 HTTP、MetadataFacade、Raft propose、apply 和响应日志；
8. 为 V2 与 V3 分别准备数据目录，V3 `single` 模式可以使用单节点 Metadata，但不宣称 HA。

### 验收

- V2 现有回归不减少；
- `single` 模式行为与 V2 一致；
- `ha` 模式未启用时不影响原数据面。
- 三节点进程可以按配置启动、停止和清理；端口冲突、重复 nodeId、不可写元数据目录会在启动阶段明确失败；
- 故障注入可以模拟连接断开、延迟、丢包和进程停止，并且不会修改 V2 运行目录。

## Phase 1：Metadata StateMachine 与持久化

### 目标

先把 GatewayState 中的状态变成可重放的命令状态机，不先写 Raft。

### 核心接口

```cpp
class MetadataStateMachine {
public:
    ApplyResult apply(const MetadataCommand& command);
    QueryResult query(const MetadataQuery& query) const;
    Snapshot snapshot() const;
    bool restore(const Snapshot& snapshot);
};

class IMetadataStore {
public:
    virtual ~IMetadataStore() = default;
    virtual bool append(const LogEntry& entry) = 0;
    virtual std::optional<LogEntry> read(uint64_t index) = 0;
    virtual bool writeSnapshot(const Snapshot& snapshot) = 0;
};
```

Phase 1 的状态机必须是确定性的：同一条命令、同一条日志顺序和同一份快照，在三个节点上得到相同的
状态和结果。HTTP Handler 不直接修改 `GatewayState`，所有写入先转换为 `MetadataCommand`，再由状态机
统一校验和应用。

每条命令至少包含：

```text
commandId / commandType / schemaVersion / objectId
expectedVersion / placementEpoch / nodeEpoch
payload / createdAt
```

`expectedVersion`、`placementEpoch` 和 `nodeEpoch` 不匹配时必须返回明确的 `STALE_VERSION` 或
`FENCED`，不能静默覆盖新状态。

### Commands

```text
RegisterNode
HeartbeatNode
CreateSession
ReserveLease
ReleaseLease
CommitChunk
CommitFile
MarkNodeHealth
CreateRepairTask
FinishRepair
FailRepair
```

### 必须测试

- 相同 commandId 重放不产生重复 Lease/Route；
- CommitChunk 校验 session、chunk hash、size、lease、epoch；
- 快照恢复后查询结果与原状态一致；
- 崩溃发生在 append 前/后/ apply 前/后的恢复结果明确；
- LevelDB 只作为单节点持久化，不宣称多节点一致性。

### 停止条件

如果 StateMachine 不能单独通过重放测试，不进入 Raft。

Phase 1 交付物：`MetadataStateMachine`、LevelDB log/snapshot store、命令编解码、重放工具、状态机单元
测试和一份可读的状态转换表。此阶段不修改文件上传 Body 路径。

## Phase 2：接入成熟 Raft 库

### 目标

接入一个与 C++17/CMake 兼容的成熟 Raft 库，组成单地域三节点 Metadata 集群；不在 V3 主线上重复实现
Leader election、AppendEntries 和日志持久化。我们负责把 Gateway 业务命令接入 Raft，并通过适配层保持
项目可测试、可替换。

### 选型门槛

候选库必须在实现前完成一个最小 PoC，确认以下能力，而不是只看“支持 Raft”的介绍：

```text
1. C++17/CMake 集成方式清晰，许可证与项目发布方式兼容；
2. 支持三节点选主、日志复制、节点重启恢复和快照/压缩；
3. 能在多数派提交后触发 apply 回调，并返回 term/index/leader 信息；
4. 能配置持久化目录、peer 地址、超时和内部网络端口；
5. 能安全处理重复 propose、超时、旧 term、少数派和网络分区；
6. Transport、Storage、StateMachine 回调可以被测试替换或注入故障。
```

若候选库缺少快照、恢复或必要的 fencing 扩展，不能为了赶进度把这些语义写进业务 Handler；应更换
适配器或明确缩小 V3 范围。

### 适配层

```text
Gateway HTTP Handler
  → MetadataFacade::propose(command)
  → RaftAdapter::replicate(serialized command)
  → majority commit
  → MetadataStateMachine::apply(command)
      → MetadataFacade 返回 version/result
```

建议将库封装成最小接口，避免项目代码依赖第三方库的具体类型：

```cpp
class RaftAdapter {
public:
    virtual ProposalResult propose(std::string commandId,
                                   std::string encodedCommand) = 0;
    virtual ReadResult readIndex() = 0;
    virtual bool isLeader() const = 0;
    virtual std::optional<LeaderHint> leaderHint() const = 0;
    virtual void shutdown() = 0;
};
```

`RaftAdapter` 只负责提交、查询 Leader、接收 apply 回调和生命周期；业务状态校验必须留在
`MetadataStateMachine`。第三方库的网络线程不能直接修改 HTTP 连接对象，apply 结果应投递到 Metadata
所属 EventLoop，再由原请求的连接线程完成响应。

每个 Gateway 进程包含：

```text
public HTTP port      8080
internal Raft port    18001/18002/18003
local LevelDB         每个 member 独立目录
MetadataStateMachine  与 Gateway 业务层通过进程内接口调用
```

三节点的目录和端口必须可配置，例如：

```yaml
node_id: meta-1
public_http: 0.0.0.0:8080
raft:
  listen: 0.0.0.0:18001
  peers:
    - id: meta-2
      address: 127.0.0.1:18002
    - id: meta-3
      address: 127.0.0.1:18003
storage:
  metadata_dir: /data/minidriver/v3/meta-1
```

实际部署中三个节点只替换 `node_id`、Raft 端口和元数据目录；DataNode 地址、HMAC 密钥和 Gateway LB
配置不能通过 Raft 日志动态覆盖，必须由受控配置或配置版本管理。

Raft member 之间仍然需要网络通信：

```text
Gateway-1/Meta-1 ⇄ Gateway-2/Meta-2 ⇄ Gateway-3/Meta-3
        RequestVote / AppendEntries / Snapshot
```

如果当前请求落到 Follower：

```text
Client → Gateway-2
       → 本地 MetadataFacade 发现 NOT_LEADER
       → 转发到 Leader Gateway-1 的内部 Metadata endpoint
       → Leader Raft majority commit
       → Gateway-2 返回客户端
```

这只是“业务层到本地 Metadata 不需要跨进程 RPC”，不是“节点之间没有通信”。

### Raft 不负责

- 文件 Body；
- 64 KiB Block；
- DataNode 磁盘队列；
- 直接修复文件内容。

### 我们必须自己实现和测试的部分

- `MetadataCommand` 序列化和版本兼容；
- `MetadataStateMachine::apply/query/snapshot/restore`；
- commandId、leaseId、repairTaskKey 的幂等；
- Leader redirect、term/version/epoch fencing；
- Gateway 业务请求与 Raft apply 回调的线程安全；
- 连接失败、超时、重复响应和重试边界。

### Phase 2 的实现顺序

1. 用内存 Transport 启动三个 Raft Member，验证 election、majority commit 和 apply 顺序；
2. 接入本地持久化目录，验证进程重启后 term、日志和快照恢复；
3. 接入 `MetadataCommand` 编解码，验证不同节点 apply 后状态摘要一致；
4. 增加 HTTP Handler → MetadataFacade → RaftAdapter 的同步/异步结果桥接；
5. 增加 Follower 的 `NOT_LEADER + leaderHint` 和内部转发；
6. 注入延迟、断线、重复响应和少数派，验证 commandId 幂等与 fencing；
7. 最后再把 GatewayState 的 Session/Lease/Route/Commit 写路径逐项迁移。

不要在这一步迁移 DataNode 文件 Body；V2 的客户端直传、链式副本、SharedBodyBlock、磁盘队列和
pause/resume 必须保持原路径，只将其控制状态改为 Metadata command。

### 内部 Metadata API 边界

嵌入式部署仍建议保留明确的内部 API 抽象，方便未来拆成独立 `metadata_node`：

```text
POST /internal/metadata/propose
GET  /internal/metadata/leader
GET  /internal/metadata/read-index
GET  /internal/metadata/snapshot-info
```

这些接口只处理小型命令和查询结果，不接收文件 Body。接口必须带内部节点认证、`term`、
`metadataVersion`、`commandId` 和超时；Follower 转发时要携带原始请求 ID，并设置 hop 上限，避免
Leader 地址错误造成循环转发。公共 HTTP 端口不得直接暴露这些接口。

### 线程与回调约束

成熟 Raft 库可能拥有自己的网络线程和持久化线程，不能直接操作现有 HTTP `TcpConnection`。统一规则是：

```text
Raft 网络/存储回调
  → Metadata EventLoop
  → apply MetadataStateMachine
  → 完成等待中的 Proposal
  → 原 HTTP 连接所属 EventLoop 回调响应
```

请求超时只取消等待者，不回滚已经提交的 Raft 命令；客户端稍后以同一个 `commandId` 重试时，必须返回
此前的 ApplyResult。这是 Gateway 故障切换和重复 Commit 正确性的基础。

### 停止条件

成熟库的选型、版本、持久化和故障边界必须记录在 V3 报告中；没有三节点故障集成测试，不宣称“实现了
生产级控制面”。如果库无法满足快照、恢复或 fencing 要求，先更换适配器，不把业务逻辑写进库内部。

## Phase 3：Gateway 无状态化与三 Gateway 故障切换

### 目标

让 Gateway1/2/3 共享同一 Metadata Cluster，任意单 Gateway 故障不丢 Session、Lease、Route 和 Commit。

### 工作

1. `GatewayMetadataClient` 查询/提交 Metadata Leader；
2. Follower 返回 `NOT_LEADER + leaderHint + term`；
3. Session/Lease/Route/Commit 全部迁移到 StateMachine；
4. Gateway 本地缓存绑定 metadataVersion；
5. 配置外部 LB；开发环境可先使用 Nginx/HAProxy，V3 不自研 L4 LB；
6. 客户端连接失败时重试备用 Gateway，但不重新创建 session；
7. Gateway 的健康检查只影响流量，不决定元数据状态。

### 必须测试

- 上传 Session 在 Gateway1 停止后由 Gateway2 继续；
- Chunk Commit 在 Gateway 切换后幂等；
- Gateway 本地缓存旧版本时拒绝覆盖新状态；
- 单个 Gateway 重启不改变对象可见性。

## Phase 4：DataNode Heartbeat、健康状态与 fencing

### 目标

从基础 heartbeat 升级为可驱动故障处理的状态机。

### 状态机

```text
JOINING → ONLINE → SUSPECT → OFFLINE
                    ↑          ↓
                 RECOVERING ← 重新注册
```

### 工作

- 每 2 秒发送 heartbeat；
- 约 5 秒无心跳进入 SUSPECT，约 10 秒进入 OFFLINE；
- 状态变化作为 Metadata command 写入 Raft；
- 旧 node epoch 的写入、Commit、Repair 完成请求被拒绝；
- 节点恢复先进入 RECOVERING，完成本地一致性检查后再 ONLINE；
- heartbeat 发送 active upload/download、disk queue、pwrite P95、free space 和 eventLoop lag。

### 必须测试

- heartbeat 丢失、延迟、恢复；
- SUSPECT 不立即删除副本；
- OFFLINE 摘除新 Placement；
- 旧 epoch 写请求拒绝；
- 节点恢复不覆盖新的副本列表；
- 正常节点不会因单次 heartbeat 抖动被错误摘除。

## Phase 5：严格副本可见性与 Repair Task

### 目标

补齐“副本失败之后如何恢复”的主闭环。

### Repair 流程

```text
DN1 OFFLINE
  → Coordinator 扫描 ChunkRecord
  → replicas=[DN1, DN2], desiredRf=2
  → 选择健康源 DN2 和目标 DN3
  → CreateRepairTask(taskKey, generation)
  → DN2 → DN3 流式读取/写入临时 extent
  → DN3 校验 hash/size
  → FinalizeRepair
  → Metadata Leader FinishRepair
  → replicas=[DN2, DN3], state=HEALTHY
```

### DataNode 内部接口

```text
POST /internal/repair/start
POST /internal/repair/{task}/body
POST /internal/repair/{task}/finish
POST /internal/repair/{task}/abort
```

Repair 必须：

- 使用确定性 `taskKey`，重复扫描不产生无限任务；
- 目标先写临时 extent，hash/size 成功后原子切换；
- 源节点必须处于 ONLINE；
- 使用独立、有界 Repair 槽，不占满前台上传槽；
- 失败指数退避，超过上限进入 `REPAIR_BLOCKED`；
- 旧 generation 的 FinishRepair 被拒绝；
- 修复完成后才更新 Metadata 副本列表。

### 严格/降级模式

```text
严格模式：副本不足 → DEGRADED/PROTECTING，不报告完整 COMMITTED
降级模式：允许读取已存在健康副本，但响应明确 DEGRADED，并创建 Repair
```

默认启用严格模式；降级模式只用于恢复演示和明确的测试配置。

## Phase 6：Load-aware Placement 与扩缩容

### 目标

让新写入避开健康但过载的节点，并支持新增节点通过 Repair/迁移承接数据。

### Placement 输入

```text
nodeId / healthState / freeBytes
activeUploads / activeDownloads
diskQueueDepth / diskPauseMs
eventLoopLag / recentErrorRate
placementEpoch / failureDomain
```

### 第一版评分

```text
eligible = ONLINE 且能力、空间、epoch 满足要求的节点
score = 0.35 * freeRatio
      + 0.15 * (1 - cpuUsage)
      + 0.15 * (1 - diskIoUsage)
      + 0.15 * queueScore
      + 0.10 * connectionScore
      + 0.10 * recentErrorScore
```

评分必须使用时间窗口或 EWMA，不能只根据瞬时 active upload；否则高并发时会频繁抖动。

### 扩缩容边界

V3 第一版先做“新增节点补足副本”，暂不做全量主动迁移：

```text
新增 DN4
  → 注册并完成健康检查
  → 新写入可选择 DN4
  → Coordinator 扫描副本不足/迁移候选
  → 有界 Repair/迁移
```

删除节点必须先迁移其全部健康副本，再从 Placement 成员中移除；禁止直接删除节点记录。

### 必须测试

- DN1 变慢时新写入逐步减少进入 DN1；
- 不把已有对象承诺为立即迁移；
- 新增节点后副本逐步均衡；
- 迁移不饿死前台上传/下载；
- 节点移除期间副本数不低于严格模式要求。

## Phase 7：三 Gateway / 三 Metadata / 三 DataNode 综合验收

### 故障矩阵

| 场景 | 预期 |
|---|---|
| Gateway1 故障 | Gateway2/3 继续处理相同 Session |
| Metadata Follower 故障 | 2/3 仍可提交 |
| Metadata Leader 故障 | 重新选主后继续提交，旧 Leader 写入被拒绝 |
| DataNode 单节点故障 | 新写入绕开；副本进入 Repair |
| 副本连接中途断开 | 任务失败可重试，不产生重复副本 |
| 节点恢复 | RECOVERING → 校验 → ONLINE，不覆盖新状态 |
| 网络分区少数派 | 拒绝写，不能双 Leader |
| 重复 Commit/Repair | 幂等，无重复元数据和无限任务 |
| 新节点加入 | 新写入可用，后台有界迁移/补副本 |

### 性能矩阵

保持 V2/V4.3 口径并新增控制面指标：

- upload-only、download-only、mixed c4/c6/c8；
- 16 MiB、4 MiB Chunk、双副本、每档至少 5 轮；
- 文件/Chunk P50/P95/P99；
- aggregate MiB/s、成功率、503 和重试次数；
- Metadata command latency、Raft apply latency、leader redirect 次数；
- heartbeat lag、Repair throughput/lag、placement score；
- pwrite、queue wait、BlockPool、pause、EventLoop lag、CPU、RSS；
- 所有成功下载的 SHA-256 和原始 CSV/日志。

## 8. Git 与发布流程

### 分支

```text
v2                 已发布 V2 基线，不再混入 V3 语义
v3                 V3 集成分支
feature/metadata-state-machine
feature/raft
feature/gateway-ha
feature/node-health
feature/repair
feature/placement
```

### 提交顺序

```text
feat: extract metadata state machine
feat: add metadata snapshot and replay
feat: integrate mature raft metadata adapter
feat: make gateway metadata-backed
feat: add node health fencing
feat: add idempotent replica repair
feat: add load-aware placement
test: add v3 failure matrix
docs: add v3 acceptance report
```

### 标签条件

只有满足以下条件才创建 `v3.0.0`：

- V2 回归全部通过；
- 3 节点 Raft 单故障测试通过；
- Gateway 故障切换不丢 Session/Lease/Route；
- DataNode 故障后副本 RF 在限定时间内恢复；
- stale epoch 和少数派写入被拒绝；
- 重复 Commit/Repair 幂等；
- 无 UAF、无限队列、无解释 400 或 hash 错误；
- 性能报告说明 HA 开销和未覆盖边界。

## 9. 推荐时间安排

按每天约 5 小时估计，Repair、fencing 和 Gateway 切换是主要风险项，建议按 9～11 周安排：

| 周期 | 阶段 | 交付 |
|---|---|---|
| 第 1 周 | Phase 0 | v3 分支、基线、故障注入接口 |
| 第 2 周 | Phase 1 | StateMachine、命令、快照/重放 |
| 第 3 周 | Phase 2 | Mature Raft adapter、三节点 Metadata 集成、apply 回调 |
| 第 4～5 周 | Phase 3 | 三 Gateway、Leader redirect、HA 测试 |
| 第 6 周 | Phase 4 | Heartbeat、健康状态、epoch fencing |
| 第 7～8 周 | Phase 5 | Repair Task、临时 extent、RF 恢复 |
| 第 9 周 | Phase 6 | Placement、加入节点、有限迁移 |
| 第 10～11 周 | Phase 7 | 故障矩阵、性能矩阵、V3 报告和发布 |

如果时间不足，优先完成：

```text
StateMachine → 受限三节点 Raft → Gateway failover → Heartbeat → Repair
```

Placement 和扩缩容迁移可以作为 V3.1，不应为了赶版本而伪造“完整负载均衡”。

## 10. gRPC 的取舍

V3 第一版继续使用现有 HTTP/TCP 控制面，原因是你要学习的是：

- Leader 转发和重定向；
- Raft 状态机；
- epoch/fencing；
- heartbeat 与 repair；
- 超时、重试和幂等。

gRPC 可以简化 IDL、流式 RPC 和超时，但不会自动提供 Raft、一致性、故障检测、Repair 或负载均衡。
等 V3 的语义稳定后，可以将 `RaftTransport` 或 `MetadataClient` 替换为 gRPC，作为独立对比实验，
而不是让 gRPC 掩盖分布式状态设计。

## 11. 最终 V3 定位

```text
V2：高性能客户端直传、链式副本、有界背压的数据面
V3：三 Gateway、三 Metadata Raft、DataNode 健康检测、自动 Repair、负载感知 Placement
V3.1：数据迁移、扩缩容、读路径故障切换、更多故障域
```

V3 的成功标准不是“组件数量更多”，而是能在演示中证明：

```text
控制节点故障 → 元数据仍一致
数据节点故障 → 新请求绕开且副本自动恢复
旧节点恢复 → 不会覆盖新状态
负载升高 → 新写入受控分配，不产生无界队列
```

## 12. V3 完整交付清单

每个阶段都必须同时交付代码、测试和证据，不能只完成接口或启动脚本：

| 阶段 | 代码交付 | 最小证据 |
|---|---|---|
| Phase 0 | V3 配置、故障注入、独立目录 | V2 回归基线与可重复启动命令 |
| Phase 1 | MetadataStateMachine、LevelDB 日志/快照、命令编解码 | 重放、快照恢复、幂等测试 |
| Phase 2 | Mature RaftAdapter、三节点 Transport、apply 桥接 | 选主、多数派提交、重启恢复、少数派拒写 |
| Phase 3 | 无状态 Gateway、Leader 转发、版本缓存 | 任意 Gateway 故障后 Session/Commit 继续 |
| Phase 4 | Heartbeat 状态机、node epoch、fencing | SUSPECT/OFFLINE/RECOVERING 故障矩阵 |
| Phase 5 | RepairCoordinator、临时 extent、原子 finalize | RF=2 节点故障后从健康源恢复到第三节点 |
| Phase 6 | EWMA placement、新节点补副本、迁移限流 | 负载变化和节点加入的可观测结果 |
| Phase 7 | 综合启动编排、测试脚本、报告 | 三 Gateway/三 Metadata/三 DataNode 全矩阵 |

最终发布前必须保存：构建命令、配置文件、进程日志、Raft term/index、Metadata version、故障时间线、
Repair 任务状态、CPU/RSS、V2/V3 性能对比和未覆盖边界。没有这些产物时，只能称为实验分支，不能创建
`v3.0.0`。

这份计划与 [V4.4 控制面 HA 设计](V4_4_CONTROL_PLANE_HA_CONSISTENCY_DESIGN_2026-08-17.md) 对齐，
但将其改写为面向 Git V3 发布的实际实施顺序和验收框架。
