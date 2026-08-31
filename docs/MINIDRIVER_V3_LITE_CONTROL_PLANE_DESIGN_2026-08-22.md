# miniDriver V3-Lite：三节点元数据与副本自愈设计（2026-08-22）

> 当前工作版本：V3-Lite；状态：Phase 1 实施中。  
> 起始代码基线：`v2.0.1` 数据面（仅作为迁移起点，不是 V3-Lite 版本号）。  
> 范围：`1 Gateway + 3 Metadata/Raft + 3 DataNode`。  
> 不在本版本：三 Gateway、MySQL 用户系统、Gateway LB、CDN、全量迁移和自动扩缩容。

## 1. V3-Lite 要解决的问题

V2 已能让客户端直传到 DataNode，并通过链式副本保存文件：

```text
Client → Gateway（预检、路由、Commit）
Client → DN-primary（上传 Chunk） → DN-replica（链式复制）
```

但关键控制状态主要驻留在单 Gateway 的 `GatewayState + LevelDB`：

```text
Gateway 进程故障后，谁拥有 Session / Lease / Object / Replica 真相？
DataNode 失联后，谁决定它不再参与新写入？
副本少于 RF=2 后，谁创建修复任务且避免重复修复？
旧节点或旧 Repair 的回调如何不覆盖新结果？
```

V3-Lite 的目标是把这些小型控制状态变成可复制、可重放、可修复的 Metadata：

```text
Gateway
  → Metadata Leader
  → Raft 2/3 majority commit
  → 三个 MetadataStateMachine apply 同一命令
```

它不重新设计 V2 的高性能数据面：4 MiB Chunk、64 KiB SharedBodyBlock、磁盘执行器、BlockPool、
socket pause/resume、客户端直传和 DN→DN 链式复制都继续使用已有路径。

## 2. 范围与延期边界

### 2.1 本版本必须交付

```text
1 个 Gateway
3 个独立 Metadata Member
成熟 Raft 库（不手写 election / AppendEntries）
确定性的 MetadataStateMachine
Session / Lease / Object / ChunkReplicaSet 的 Raft 命令
commandId 幂等、nodeEpoch 和 generation fencing
DataNode ONLINE / SUSPECT / OFFLINE / RECOVERING
RF=2 的单 Chunk RepairTask
DataNode 故障 → 第三节点补副本 → SHA-256 验证的演示
```

### 2.2 明确延期到 V3.1 或后续

```text
3 Gateway、统一域名、Gateway LB 与 Cookie 切换
MySQL users / sessions / ownerUserId 多用户隔离
MySQL 或 Gateway LB 高可用
自动扩缩容、全量数据迁移、热点再平衡
跨机房 Raft、多故障域和生产级监控平台
Kafka/Spark/Flink、CDN、公开分享链接
```

V3-Lite 使用现有 V2 `ownerId = "admin"` 兼容数据；状态机保留 `ownerId` 字段，但此时它不是来自 MySQL 的
认证主键。这样可以先学习一致性和自愈，而不把认证、浏览器 Cookie、负载均衡故障混入第一轮调试。

## 3. 目标拓扑

```text
                              Client
                                │
                                ▼
                       Gateway（单实例）
                                │ MetadataClient
                  ┌─────────────┼─────────────┐
                  │             │             │
               Meta-1        Meta-2        Meta-3
            StateMachine   StateMachine   StateMachine
                  └──────── Mature Raft ───────┘
                                │
                   health / placement / repair
                  ┌─────────────┼─────────────┐
                  │             │             │
                DN-1          DN-2          DN-3

上传 Body：Client → DN-primary → DN-replica
修复 Body：DN-source → DN-target
Raft：只复制小型 MetadataCommand，绝不复制文件 Body
```

### 3.1 本机集成端口

先在同一台开发机上验证三节点语义，使用当前 `configs/v3-lite.local.yaml` 约定：

| 服务 | 地址 |
|---|---|
| Gateway HTTP | `127.0.0.1:18280` |
| Meta-1 Raft / RPC | `127.0.0.1:18201` / `127.0.0.1:18301` |
| Meta-2 Raft / RPC | `127.0.0.1:18202` / `127.0.0.1:18302` |
| Meta-3 Raft / RPC | `127.0.0.1:18203` / `127.0.0.1:18303` |
| DN-1 / DN-2 / DN-3 | `127.0.0.1:19201` / `:19202` / `:19203` |

每个 Metadata Member 使用独立目录：

```text
/tmp/minidriver-v3-lite/metadata/meta-1
/tmp/minidriver-v3-lite/metadata/meta-2
/tmp/minidriver-v3-lite/metadata/meta-3
```

禁止共享 LevelDB、Raft WAL、snapshot 文件或 DataNode 数据目录。Raft 复制命令日志；复制 LevelDB 文件既不能
选主，也无法处理并发写入。

### 3.2 三主机演示拓扑

本机通过后，可以把相同组件拆到三台已有主机：

| 主机 | 演示组件 |
|---|---|
| `100.75.93.124` | Gateway、Meta-1、DN-1 |
| `100.75.72.15` | Meta-2、DN-2 |
| `100.89.50.125` | Meta-3、DN-3；已有 AI 服务保持独立 |

此时 Raft peer 和 DataNode address 填 Tailscale IP，而不是本机 `127.0.0.1` 或 Docker bridge 地址。V3-Lite 只有
一个 Gateway，因此它应固定运行在第一台机器；Gateway 故障切换属于 V3.1。

## 4. 组件边界

| 组件 | 负责 | 不负责 |
|---|---|---|
| Gateway | 对外 HTTP、V2 API 兼容、调用 Metadata Leader、发放 UploadCapability、响应客户端 | Raft election、存储唯一 Metadata 真相、转发文件 Body |
| Metadata Member | Raft 适配、状态机 apply/query、快照、Leader 信息 | 处理浏览器 HTTP、接收 Chunk Body、访问 DataNode 磁盘 |
| Metadata Leader | 接收 proposal、推进 commit、驱动 Health/Repair scan、发布决策 | 绕过 Raft 直接修改本地状态 |
| DataNode | Chunk、链式副本、heartbeat、Repair source/target、资源治理 | MySQL、浏览器登录、Raft 选主 |
| RepairCoordinator | 在 Leader 上扫描不足 RF、创建确定性 RepairTask、限流重试 | 自己保存未复制的任务真相、传输 Chunk Body |

原则：任何“对象是否已 Commit、某个副本是否健康、Repair 是否完成”的最终结论只能来自 Raft 已 apply 的
`MetadataStateMachine`，不能只由 Gateway 内存、Redis、日志或 DataNode 本地回调决定。

## 5. Metadata 状态机

### 5.1 状态机为什么必须先于 Raft

Raft 只保证三个节点以相同顺序得到命令；它不理解文件、副本、Lease 或幂等。

```text
Raft：把 Command A、B、C 以相同顺序交给每个节点
StateMachine：定义 A、B、C 对 Object/Lease/Replica 的确定结果
```

因此必须先做到：同一初始状态 + 同一命令序列 = 同一最终状态。状态机的 `apply()` 不得读取本机当前时间、
MySQL、网络、DataNode 或随机数；这些值都必须在命令创建时确定并放入 payload。

### 5.2 核心记录字段

```cpp
enum class ObjectState { kUploading, kProtecting, kCommitted, kDegraded, kFailed };
enum class ChunkState { kAllocated, kWriting, kCommitted, kRepairing, kFailed };
enum class ReplicaState { kWriting, kHealthy, kSuspect, kOffline, kRepairing };
enum class NodeHealth { kJoining, kOnline, kSuspect, kOffline, kRecovering };
enum class RepairState { kPending, kRunning, kSucceeded, kRetryWait, kBlocked, kFailed };

struct ObjectRecord {
    std::string objectId;        // 逻辑文件对象，而非 fileHash
    std::string ownerId;         // V3-Lite 固定兼容 "admin"
    std::string parentPath;
    std::string name;
    std::string fileHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t desiredRf = 2;
    ObjectState state = ObjectState::kUploading;
    uint64_t metadataVersion = 0;
};

struct UploadSessionRecord {
    std::string sessionId;
    std::string objectId;
    std::string ownerId;
    std::string manifestHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t totalChunks = 0;
    int64_t expiresAt = 0;
};

struct ReplicaRecord {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    ReplicaState state = ReplicaState::kWriting;
    std::string verifiedHash;
    int64_t verifiedAt = 0;
};

struct ChunkRecord {
    std::string objectId;
    uint32_t index = 0;
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredRf = 2;
    ChunkState state = ChunkState::kAllocated;
    uint64_t generation = 0;
    std::vector<ReplicaRecord> replicas;
};

struct LeaseRecord {
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    uint64_t placementEpoch = 0;
    std::vector<std::string> targetNodeIds;
    int64_t expiresAt = 0;
};

struct NodeRecord {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    std::string address;
    uint16_t dataPort = 0;
    NodeHealth health = NodeHealth::kJoining;
    uint64_t freeBytes = 0;
    uint32_t activeUploads = 0;
    uint32_t activeDownloads = 0;
    uint32_t diskQueueDepth = 0;
    uint64_t diskPauseMs = 0;
    uint64_t eventLoopLagUs = 0;
    uint64_t placementEpoch = 0;
    int64_t lastHeartbeatAt = 0;
};

struct RepairTask {
    std::string taskKey;
    std::string objectId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    std::string sourceNodeId;
    std::string targetNodeId;
    uint64_t expectedGeneration = 0;
    RepairState state = RepairState::kPending;
    uint32_t attempts = 0;
    int64_t nextRetryAt = 0;
    std::string lastError;
};
```

### 5.3 三种版本/围栏字段

```text
metadataVersion：状态机已提交的逻辑版本，用于响应、缓存和条件更新。
placementEpoch ：节点成员/可选放置集合的版本，旧路由不能再选已摘除节点。
generation     ：单个 Chunk 的副本/Repair 版本，旧的 FinishRepair 不能覆盖新的 Repair。
```

`nodeEpoch` 与上述三者不同：它表示 DataNode 的一次运行 incarnation。节点重启或重新注册时增加；持有旧 epoch 的
heartbeat、Commit 或 Repair 完成通知必须返回 `FENCED`。

## 6. Metadata 命令与幂等

```cpp
enum class MetadataCommandType {
    kCreateSession,
    kReserveLease,
    kReleaseLease,
    kCommitChunk,
    kCommitFile,
    kRegisterNode,
    kHeartbeatNode,
    kMarkNodeHealth,
    kCreateRepairTask,
    kStartRepair,
    kFinishRepair,
    kFailRepair
};

struct MetadataCommand {
    uint32_t schemaVersion = 1;
    std::string commandId;
    MetadataCommandType type;
    std::string actorType;      // gateway / datanode / repair-coordinator
    std::string actorId;
    uint64_t expectedMetadataVersion = 0;
    uint64_t placementEpoch = 0;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    int64_t issuedAt = 0;       // 审计；不得让 apply 依赖“现在时间”
    std::string payload;
};

struct ApplyResult {
    std::string commandId;
    std::string status;         // OK / ALREADY_APPLIED / FENCED / CONFLICT / INVALID
    uint64_t term = 0;
    uint64_t logIndex = 0;
    uint64_t metadataVersion = 0;
    std::string objectId;
    std::string sessionId;
    std::string message;
};
```

`commandId` 是“一次业务动作”的幂等键：Gateway 在网络超时、Leader 切换或收到重复响应后必须使用原 commandId
重试。状态机持久化 `commandId → ApplyResult`，重复命令返回原结果，不再创建第二份 Lease、Session 或 RepairTask。

### 6.1 命令的最小状态转换

```text
CreateSession
  → Object=UPLOADING，Session 创建，ChunkRecord=ALLOCATED

ReserveLease
  → 仅 ONLINE 节点可选，创建 Lease，写入 placementEpoch 和目标节点

CommitChunk
  → 校验 Session / Lease / size / hash / epoch
  → 满足目标副本数：Chunk=COMMITTED，Replica=HEALTHY
  → 副本不足：Chunk=WRITING 或 FAILED，不能伪装成功

CommitFile
  → 全部 Chunk 达 desiredRf
  → Object=COMMITTED

MarkNodeHealth(OFFLINE)
  → 新 Placement 排除该节点
  → 对受影响 Chunk 创建候选 RepairTask

FinishRepair
  → 校验 taskKey / generation / verifiedHash / size
  → 原子替换 ReplicaSet 中失效副本
```

V3-Lite 默认严格副本语义：文件仅在全部 Chunk 达到 `desiredRf=2` 后才为 `COMMITTED`。若演示降级读取，必须显式
标记 `DEGRADED`，不能以 200/COMMITTED 伪装成双副本完成。

## 7. Raft 与 Metadata RPC

### 7.1 成熟 Raft 库的职责

接入成熟库，而不手写以下内容：

```text
选主、term、RequestVote、AppendEntries、日志匹配、majority commit、持久化恢复、snapshot 传输
```

项目实现一个很薄的 `RaftAdapter`：

```cpp
class RaftAdapter {
public:
    virtual ProposalResult propose(std::string commandId,
                                   std::string encodedCommand) = 0;
    virtual ReadIndexResult readIndex() = 0;
    virtual bool isLeader() const = 0;
    virtual std::optional<LeaderHint> leaderHint() const = 0;
};
```

适配器不包含对象业务规则；commit callback 只把已提交 command 投递给 `MetadataStateMachine::apply()`。

### 7.2 Gateway 访问 Metadata

Gateway 通过 `MetadataClient` 访问三个 endpoint：

```text
POST /internal/v3/metadata/propose
GET  /internal/v3/metadata/leader
GET  /internal/v3/metadata/read-index
```

请求先到 Follower 时：

```text
Gateway → Meta-2(Follower)
       ← NOT_LEADER + leaderId + leaderAddress + term
Gateway → Meta-1(Leader) with same commandId
       ← majority committed ApplyResult
```

这些接口仅监听内部网络；不接受浏览器 Cookie，不暴露到前端。Raft peer 之间的选主/日志复制使用成熟库的独立
Transport 端口，与 Metadata RPC 端口分离。

### 7.3 线程规则

Raft 库通常有自己的网络和磁盘线程。它们不能直接调用现有 HTTP `TcpConnection` 写响应：

```text
Raft callback
  → Metadata EventLoop
  → MetadataStateMachine apply / proposal completion
  → Gateway 原请求 EventLoop
  → HTTP response
```

客户端 HTTP 超时只代表“浏览器没有等到响应”，不代表 Raft 命令没有提交；后续重试必须使用相同 `commandId`。

## 8. DataNode 心跳、Placement 与 fencing

### 8.1 Heartbeat 流程

V3-Lite 沿用 DataNode 向单 Gateway 汇报的入口，Gateway 将其转换为 Raft command：

```text
DataNode（每 2 秒）
  → Gateway internal heartbeat
  → MetadataClient → Leader
  → HeartbeatNode command → Raft majority
```

Heartbeat payload：

```text
nodeId / nodeEpoch / freeBytes / activeUploads / activeDownloads
diskQueueDepth / diskPauseMs / eventLoopLagUs / recentErrorCount / observedAt
```

Leader 定时扫描心跳时间：

```text
0～6 秒未到达    ONLINE
6～12 秒未到达   SUSPECT
超过 12 秒       OFFLINE
恢复注册         RECOVERING → ONLINE
```

状态变化也必须由 `MarkNodeHealth` command 经 Raft 提交。SUSPECT 不立即触发 Repair，避免一次网络抖动造成无意义的
大文件复制；OFFLINE 后才排除新 Placement 并启动副本不足扫描。

### 8.2 第一版 Placement

只为新写入选择节点，不承诺历史对象自动均衡：

```text
eligible = health == ONLINE
        && freeBytes 足够
        && activeUploads < maxFrontendUploads
        && diskQueueDepth 未饱和
        && nodeEpoch / placementEpoch 有效

从 eligible 中选择两个不同 nodeId
```

可在现有空闲空间、CPU、磁盘 I/O、网络、活跃上传评分的基础上增加队列/pause 指标；第一版必须加入 EWMA 或短窗口，
避免瞬时 `activeUploads` 变化导致每个 Chunk 在节点间剧烈抖动。

## 9. 副本 Repair 闭环

```text
DN-1 OFFLINE
  → Leader 的 RepairCoordinator 扫描 ChunkRecord
  → Chunk replicas=[DN-1, DN-2]，desiredRf=2
  → 选 source=DN-2，target=DN-3
  → CreateRepairTask(taskKey, generation) 经 Raft 提交
  → StartRepair 经 Raft 提交
  → DN-2 流式 GET 本地 Chunk → DN-3 临时 extent
  → DN-3 校验 size + SHA-256 == chunkHash
  → FinishRepair(taskKey, generation, verifiedHash) 经 Raft 提交
  → Chunk replicas=[DN-2, DN-3]，状态恢复 HEALTHY
```

`taskKey` 必须确定性生成，例如：

```text
sha256(objectId | chunkIndex | sourceNodeId | targetNodeId | generation)
```

同一轮扫描产生相同 taskKey，因此不会因 Leader 定时扫描或重试无限创建 RepairTask。

Repair 约束：

- 目标先写临时 extent，校验成功后再原子 finalize；
- Source 和 Target 必须都是 ONLINE 且不相同；
- Repair 使用 `maxRepairTasks=1` 等独立有界槽，不占满前台上传槽；
- 失败使用有限次数 + 指数退避，超过上限为 `REPAIR_BLOCKED`；
- 旧 generation 的 FinishRepair 返回 `FENCED`；
- Repair Body 不经过 Gateway，不写 Raft，不进入 Redis。

## 10. V2 API 的迁移方式

浏览器 URL 尽量保持不变：

```text
POST /api/v2/upload/preflight
POST /api/v2/upload/sessions/{id}/routes
POST /api/v2/upload/sessions/{id}/commit
GET  /api/v2/catalog
GET  /api/v2/objects/{id}/manifest
```

内部变化是：原来 HTTP Handler 直接调用 `GatewayState`，V3-Lite 改为：

```text
HTTP Handler
  → 把请求转换 MetadataCommand
  → MetadataClient / Raft Leader
  → apply 成功后返回原有 JSON 兼容字段
```

Chunk PUT、HTTP 流式解析、`X-Upload-Token`、DataNode 链式复制与 `pwrite` 路径保持不变；只将“路由是否有效、
Chunk/File 是否 Commit、节点是否可选”背后的真相迁入 Metadata 状态机。

## 11. Docker 运维与运行方式

Docker Compose 是 V3-Lite 的正式交付物：它负责构建镜像、拉起进程、挂载独立状态目录、收集日志和重复故障注入；
它不替代 Raft quorum、fencing、Repair 或数据完整性验收。

为了不让 Docker 网络问题掩盖一致性错误，P1/P2 仍先用原生可执行程序与本机端口验证 StateMachine/Raft；三个
Member 能稳定选主、多数派提交并重启恢复后，再将同一套可执行程序容器化。

### 11.1 本机 Compose 契约

```text
Dockerfile.minidriver                 # 统一构建 C++ Gateway / Meta / DataNode 运行镜像
deploy/v3/compose.local.yaml
  gateway
  meta-1 / meta-2 / meta-3
  dn-1 / dn-2 / dn-3
configs/v3-lite.local.yaml            # 挂载为只读运行配置
```

本机 Compose 使用容器 DNS 或明确端口映射；Gateway 是唯一默认映射给浏览器的服务。每个状态服务必须独占 volume：

```text
gateway_state
meta_1_state / meta_2_state / meta_3_state
dn_1_data / dn_2_data / dn_3_data
```

禁止两个 Meta 容器共享 Raft WAL、snapshot 或 LevelDB；禁止两个 DataNode 共享 Chunk 数据目录。启动依赖只保证容器
进程启动顺序，不能证明 Raft 已有 Leader 或多数派。

每个服务提供：

```text
/healthz  进程/HTTP 事件循环存活
/readyz   业务就绪；Meta 必须报告 term、leaderId、quorum、lastAppliedIndex
```

Gateway 的 `/readyz` 必须能说明 Metadata Leader/ReadIndex 是否可用；DataNode 的 `/readyz` 至少报告注册状态、
nodeEpoch、磁盘目录和资源治理器是否可用。容器 `healthy` 不是“Raft 正确”的替代证据。

建议用以下包装命令统一开发者入口：

```text
make v3-local-up          # build + compose up -d
make v3-status            # compose ps + /readyz 摘要
make v3-logs              # 聚合 gateway/meta/dn 日志
make v3-fault-meta-1      # docker compose stop meta-1
make v3-fault-dn-1        # docker compose stop dn-1
make v3-local-down        # 正常停止，保留 volume
```

故障/恢复测试中严禁使用 `docker compose down -v`；`-v` 会删除 Metadata WAL/snapshot 或 DataNode Chunk，等价于
人为销毁测试状态，不能称为节点故障。

### 11.2 三主机 Compose 契约

Docker Compose 不能跨 Docker daemon 编排。三主机部署使用一份共同 image tag 和三份宿主机覆盖配置：

```text
deploy/v3/compose.host-a.yaml  Gateway + Meta-1 + DN-1
deploy/v3/compose.host-b.yaml  Meta-2 + DN-2
deploy/v3/compose.host-c.yaml  Meta-3 + DN-3
```

所有 Raft peer 与 DataNode route 使用 Tailscale `advertise_address`。跨主机第一版优先采用 `network_mode: host`，
使容器监听宿主机的稳定端口；如果选择 bridge 网络，必须显式发布端口且禁止把容器 `172.x` 地址写进 Raft 配置。

每台主机都要记录：image tag、配置版本、私有 secret 文件、volume 路径、开放端口、启动/停止命令和日志位置。
三主机故障演练以停止单个容器为准，而不是删除容器 volume。

### 11.3 Kubernetes 的边界

Kubernetes/k3s 有学习价值，但不属于 V3-Lite 的实现范围。Compose 版本通过后，才作为 V3.2 部署实验：

```text
Gateway          → Deployment + Service/Ingress
Metadata Members → StatefulSet(3) + Headless Service + 独立 PVC
DataNodes        → StatefulSet(3) + 固定 nodeId + 独立 Local PV/PVC
```

Metadata 和 DataNode 不能使用随机 Deployment：Raft memberId、peer DNS、WAL/PVC、DataNode 磁盘目录和 Chunk 路由
都要求稳定身份。DataNode 也不能只放在一个随机负载均衡 Service 后面；Metadata Route 必须定位到具体的 DN member。

## 12. 实施顺序与停止条件

### P0：基线与配置（已完成骨架）

- `v3` 分支、独立 V3 运行目录、故障注入接口和本地 YAML；
- V2 build/回归基线；
- 不修改 V2 数据目录。

停止条件：基线无法复现时，不进入状态机改造。

### P1：MetadataStateMachine

- 定义本文件的 record、command、ApplyResult；
- 单节点 `apply/query/snapshot/restore`；
- LevelDB 本地 log/snapshot 或可替换 store；
- commandId 幂等、固定日志重放、epoch/generation 拒绝测试。

停止条件：不同前缀重放、快照恢复、重复 Commit 任一失败时，不接 Raft。

### P2：成熟 Raft Adapter

- 选择并 PoC C++17/CMake 兼容的成熟库；
- 三 Member election、majority commit、restart recovery；
- `NOT_LEADER + leaderHint`、term/index/leader 指标；
- apply 回调与 EventLoop 安全桥接。

停止条件：单 Member 停止后无法 commit、Leader 停止后无法重选、或恢复状态不一致时，不迁移 Gateway。

### P3：Gateway MetadataClient

- Session、Lease、Route、Chunk/File Commit 全部改为 MetadataCommand；
- 保持 V2 HTTP 响应兼容；
- commandId 重试和 ReadIndex 查询。

停止条件：Gateway 重启后 Session/Commit 需要读取本地旧 `GatewayState` 才能工作时，说明迁移未完成。

### P4：Node Health 与 Repair

- Heartbeat / OFFLINE / nodeEpoch fencing；
- RepairCoordinator、临时 extent、finish/fail 命令；
- DN-1 故障后从 DN-2 修复到 DN-3。

停止条件：Repair 能覆盖前台上传槽、产生重复任务、或未校验 SHA-256 即更新 Metadata 时，不能发布。

### P5：Docker Compose 运维与三主机验收

#### P5a：本机容器化

- 统一 C++ 运行镜像、`compose.local.yaml`、七服务独立 volume；
- `healthz/readyz`、统一 Make 命令、日志收集；
- 通过 `docker compose stop meta-1` 和 `stop dn-1` 注入故障；
- 重启后验证 WAL/snapshot/DataNode 数据仍在。

#### P5b：三主机 Compose

- 使用相同 image tag 在三台主机按 host-a/b/c 配置启动；
- 使用 Tailscale 地址、host network 或明确端口映射；
- 保存每台主机配置、volume 和端口 ACL；
- 保存日志、term/index、metadataVersion、故障时间线和 SHA-256；
- 再执行 V2 upload/download/mixed 回归。

## 13. 最小验收矩阵

| 场景 | 必须观察到的结果 |
|---|---|
| 三 Meta 启动 | 唯一 Leader、所有 Member state digest 一致 |
| Meta Follower 停止 | Leader + 另一 Follower 仍可 commit |
| Meta Leader 停止 | 新 Leader 当选；相同 commandId 重试不重复修改状态 |
| Meta 重启 | 从 WAL/snapshot 恢复并追赶，不丢 committed Object/Lease |
| DN-1 停止 | SUSPECT → OFFLINE；新 Route 不选 DN-1 |
| DN-1 副本受影响 | 只创建一个 taskKey；DN-2 → DN-3 修复后 RF 恢复 |
| 旧 Repair finish | generation 不匹配，返回 FENCED |
| 下载校验 | Repair 前后成功下载 SHA-256 都等于原始文件 |
| 数据面回归 | V2 的背压、BlockPool、磁盘队列不出现无界增长 |

## 14. 完成后的下一步

只有 V3-Lite 验收通过后，再进入本文刻意延期的 V3.1：

```text
3 Gateway
  + shared MySQL user/session
  + stable domain / Gateway LB
  + shared MetadataClient to the same 3 Meta Raft Members
```

届时多 Gateway 只增加无状态入口和认证一致性；不会重新设计已经验证过的 MetadataStateMachine、Raft、
DataNode health 或 Repair 协议。
