# miniDriver V3 目标 HA 架构与字段设计（2026-08-21）

> 状态：目标设计，尚未实现。  
> 版本策略：先完成 `V3-Lite`（1 Gateway、3 Metadata、3 DataNode），再扩展到本文定义的
> `V3.1 HA`（3 Gateway、共享 MySQL、Gateway 流量切换）。  
> 基线：`v2.0.1` 数据面；不改变客户端直传、4 MiB Chunk、链式副本、BlockPool、磁盘队列和 TCP 背压的主路径。

## 1. 最终目的

目标不是把所有进程简单复制三份，而是在三个彼此独立的状态域上建立明确边界：

```text
身份状态：用户是谁、会话是否有效                 → 共享 MySQL
存储控制状态：对象在哪里、谁持有 Lease、副本是否健康 → 三节点 Raft Metadata
文件字节：Chunk、extent、磁盘队列和网络传输        → 三个 DataNode
```

最终可证明的用户闭环是：

```text
用户登录 Gateway-1
  → 上传途中 Gateway-1 停止
  → 浏览器切换到 Gateway-2
  → Gateway-2 能从共享 MySQL 确认用户身份
  → Gateway-2 能从 Raft Metadata 继续读取同一个上传 Session
  → 上传完成后对象由 RF=2 副本承诺
  → 一个 DataNode 故障后，Metadata 标记 OFFLINE 并修复到第三节点
```

## 2. 最终逻辑拓扑

```text
                                Browser / Client
                                       │
                         Gateway LB / client failover
                  ┌────────────────────┼────────────────────┐
                  │                    │                    │
             Gateway-1            Gateway-2            Gateway-3
                  │                    │                    │
                  └────────── Auth / Session ───────────────┘
                                       │
                             Shared MySQL (single logical DB)
                                       │
                  ┌────────────────────┼────────────────────┐
                  │                    │                    │
                 Meta-1               Meta-2               Meta-3
                  └──────────────────── Raft ───────────────┘
                                       │
                     placement / health / repair commands
                  ┌────────────────────┼────────────────────┐
                  │                    │                    │
                 DN-1                 DN-2                 DN-3

上传 Body：Client → DN-primary → DN-replica
下载 Body：Client → DN（持短期 DownloadCapability）
控制写入：Gateway → Metadata Leader → Raft majority
```

### 2.1 组件数量不是物理服务器数量

上图有十个逻辑组件，但当前只有三台物理服务器时可以共置：

| 物理主机 | Tailscale IP | 学习环境中的容器/进程 |
|---|---|---|
| `gateway` | `100.75.93.124` | Gateway-1、Meta-1、DN-1 |
| `node-d` | `100.75.72.15` | Gateway-2、Meta-2、DN-2 |
| `ubuntu22data1` | `100.89.50.125` | Gateway-3、Meta-3、DN-3、AI Compose、单实例 MySQL |

这不是理想生产故障域：`ubuntu22data1` 故障会同时影响 Gateway-3、Meta-3、DN-3、AI 和 MySQL。它仍足以做
V3 学习、容器拉起、Gateway 切换和单节点 DataNode Repair 演示。真正生产化需要把 MySQL、Gateway LB 和至少一部分
Metadata/DN 拆到额外主机或可用区。

### 2.2 各组件的唯一职责

| 组件 | 必须负责 | 绝不负责 |
|---|---|---|
| Gateway | HTTP API、用户认证、对象 owner 授权、向 Leader 提交命令、签发短期 capability | 保存唯一 Metadata 真相、转发大文件 Body |
| MySQL | `users`、密码 hash、会话 token hash、禁用状态 | 对象、Chunk、Raft Log、上传进度 |
| Metadata/Raft | Session、Lease、Object、Chunk 副本集、NodeHealth、RepairTask 的一致性状态 | 密码、Cookie、4 MiB Chunk Body |
| DataNode | Chunk 本地存储、链式副本、下载、Repair 流式传输、资源治理 | MySQL、浏览器 Cookie、Raft 选主 |
| Gateway LB | 流量分发、Gateway 健康摘除 | 用户认证、元数据一致性、DataNode placement |
| AI 服务 | 消费 Commit Event、派生索引、搜索 | 修改 MiniDrive 原始对象或用户身份真源 |

## 3. 三类通信链路

### 3.1 Browser、Gateway 与 MySQL

```text
Browser Cookie(md_session)
  → 任意 Gateway
  → AuthExecutor（非 EventLoop）
  → Shared MySQL 校验 token_hash
  → RequestPrincipal{userId, username, authVersion}
```

所有 Gateway 必须使用同一个 MySQL logical database，因此用户在 Gateway-1 登录后，请求落到 Gateway-2 时仍能
识别同一个 Cookie。V3.1 初期 MySQL 是单实例共享依赖；它挂掉时 Gateway 必须 fail closed 返回 `503`，不能退化为
`admin` 或匿名用户。

### 3.2 Gateway 与 Metadata Raft

```text
Gateway
  → MetadataClient::propose(MetadataCommand)
  → Meta Follower：NOT_LEADER + leaderHint
  → Gateway 重试/转发到 Meta Leader
  → Raft 多数派 commit
  → 三个状态机按相同顺序 apply
  → ProposalResult{term, logIndex, metadataVersion, status}
```

Gateway 可以有短期只读缓存，但 Session、Lease、Route、Commit、Repair 结论不能只依赖本地内存。需要线性一致性的
读必须走 Leader 或 Raft ReadIndex；允许短暂陈旧的节点监控列表可以读取 Follower，并携带 `metadataVersion`。

### 3.3 Gateway/DataNode、DataNode/DataNode

```text
Gateway → DataNode：由 Raft 已提交的 Route 生成 UploadCapability
Client  → DataNode：PUT Chunk + UploadCapability
DN-primary → DN-replica：链式流式复制
DataNode → Metadata Leader：heartbeat / repair complete / repair fail
Gateway → DataNode：对象 owner 校验后返回 DownloadCapability
Client  → DataNode：GET Chunk + DownloadCapability
```

DataNode 不需要调用 MySQL；它只验证 HMAC capability、lease/epoch 约束和本地资源准入。Cookie、密码和用户 token
不得发送到 DataNode。

## 4. 配置字段设计

所有真实密码、集群密钥和数据库 DSN 应放入私有 `.env` 或 `passwordFile`，不写入 Git。以下 YAML 是字段契约，
不是可直接覆盖现有 Node Agent 配置的完整文件。

```yaml
cluster:
  cluster_id: minidriver-v3-lab                 # 固定集群身份；所有成员一致
  environment: lab                              # lab / staging；不得由客户端指定
  cluster_secret_file: /etc/minidriver/cluster.secret

gateway:
  gateway_id: gw-1                              # 每实例唯一
  public_listen: 0.0.0.0:18081                  # 浏览器/API 入口
  health_listen: 127.0.0.1:18082                # LB/Docker healthcheck 使用
  advertise_address: 100.75.93.124              # 其他主机可达的 Tailscale 地址
  request_timeout_ms: 5000
  metadata_timeout_ms: 1000
  metadata_retry_limit: 2
  auth_executor_threads: 2
  auth_executor_max_pending: 64

auth:
  enabled: true
  mysql:
    host: 100.89.50.125                         # V3.1 共享 MySQL 地址
    port: 3306
    database: minidrive_auth
    user: minidrive_gateway
    password_file: /etc/minidriver/mysql.password
    connect_timeout_ms: 1000
  session:
    cookie_name: md_session
    ttl_seconds: 604800
    secure_cookie: true                          # HTTPS 下必须 true
    same_site: Lax
  legacy_admin_user_id: admin                    # V2 历史 owner 的显式兼容主体

metadata:
  client_endpoints:                              # Gateway 按顺序探测；leaderHint 优先
    - id: meta-1
      address: 100.75.93.124:18101
    - id: meta-2
      address: 100.75.72.15:18101
    - id: meta-3
      address: 100.89.50.125:18101
  read_consistency: leader                       # leader / read_index / stale

raft:
  member_id: meta-1                              # 本 Meta 实例唯一，不能与 gateway_id 混用
  listen: 100.75.93.124:18001                   # Raft 内部 Transport 端口
  advertise_address: 100.75.93.124:18001
  data_dir: /var/lib/minidriver/meta-1           # 每成员独立 volume/目录
  peers:
    - id: meta-2
      address: 100.75.72.15:18001
    - id: meta-3
      address: 100.89.50.125:18001
  election_timeout_min_ms: 600
  election_timeout_max_ms: 1200
  heartbeat_interval_ms: 150
  snapshot_every_entries: 10000

datanode:
  node_id: dn-1                                  # 稳定节点 ID，磁盘重启后不变
  node_epoch_file: /var/lib/minidriver/dn-1.epoch
  public_listen: 0.0.0.0:19001                   # PUT/GET Chunk；仅 Tailnet/可信网段
  advertise_address: 100.75.93.124:19001
  data_dir: /data/minidriver/dn-1
  heartbeat_interval_ms: 2000
  suspect_after_ms: 6000
  offline_after_ms: 12000
  max_frontend_uploads: 2
  max_frontend_downloads: 4
  max_repair_tasks: 1

repair:
  enabled: true
  desired_replica_count: 2
  scan_interval_ms: 5000
  max_attempts: 5
  retry_base_ms: 1000
  retry_max_ms: 60000
  max_concurrent_tasks_cluster: 2

observability:
  log_level: info
  metrics_listen: 127.0.0.1:19100
  request_id_header: X-Request-Id
```

### 4.1 字段约束

| 字段 | 约束与含义 |
|---|---|
| `cluster_id` | 固定的集群身份，不匹配的节点不得加入；不能用作密钥。 |
| `gateway_id` / `member_id` / `node_id` | 三类 ID 独立且全局唯一；重启不改变。 |
| `advertise_address` | 必须是其他主机可达的 Tailscale/内网地址，不能填容器 `172.x` 地址。 |
| `data_dir` | 必须为本节点独占的持久化目录或 Docker volume，禁止两个 Raft 成员共享。 |
| `node_epoch_file` | 节点重新注册时增加 epoch；旧进程/旧请求被 fencing。 |
| `suspect_after_ms < offline_after_ms` | 临时网络抖动先进入 SUSPECT，避免一次心跳丢失触发 Repair。 |
| `max_repair_tasks` | Repair 独立限流，不能耗尽前台上传槽。 |
| `metadata_timeout_ms` | 小于浏览器整体超时；超时后带相同 `commandId` 重试。 |

## 5. MySQL 用户与会话字段

MySQL 是身份真源，不和 Raft 复制。用户 ID 一旦创建不修改，且作为 Raft Metadata 的 owner 引用。

```text
users
  user_id          CHAR(36) PK       # 稳定 ownerUserId，不能由 username 替代
  username         VARCHAR(64) UNIQUE
  password_hash    VARCHAR(255)      # Argon2id 编码结果
  status           active / disabled
  created_at / updated_at

user_sessions
  token_hash       BINARY(32) PK     # SHA-256(随机 opaque token)，不存原 token
  user_id          CHAR(36) FK
  created_at
  expires_at
  revoked_at
  last_seen_at
```

认证结果在 Gateway 内只表达为：

```cpp
struct RequestPrincipal {
    std::string userId;       // users.user_id，同时是 ObjectRecord.ownerUserId
    std::string username;     // 仅展示/审计；不能用作授权主键
    uint64_t authVersion = 1; // 未来禁用/角色变动后可使缓存失效
    bool isAdmin = false;     // U0 可由静态 allow-list 提供，非普通用户字段
};
```

Raft 命令不携带 Cookie、password、原始 session token 或 MySQL DSN；Gateway 认证后才创建含 `actorUserId` 的命令。

## 6. Raft Metadata 状态机字段

以下结构是目标逻辑模型，序列化时必须带 `schemaVersion`。任何容器、HTTP 或第三方 Raft 库特定类型均不得泄漏到
状态机记录中。

```cpp
enum class ObjectState { kUploading, kProtecting, kCommitted, kDegraded, kDeleting, kDeleted, kFailed };
enum class ChunkState { kAllocated, kWriting, kCommitted, kRepairing, kFailed };
enum class ReplicaState { kWriting, kHealthy, kSuspect, kOffline, kRepairing };
enum class NodeHealth { kJoining, kOnline, kSuspect, kOffline, kRecovering, kDraining };
enum class RepairState { kPending, kRunning, kSucceeded, kRetryWait, kBlocked, kFailed };

struct ObjectRecord {
    std::string objectId;              // 逻辑对象 ID；不是 fileHash
    std::string ownerUserId;           // MySQL users.user_id 的稳定引用
    std::string parentPath;
    std::string name;
    std::string contentType;
    std::string fileHash;              // 整体内容 hash；可为空直到 CommitFile
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t desiredRf = 2;
    ObjectState state;
    uint64_t objectVersion = 1;        // 内容替换时递增
    uint64_t metadataVersion = 0;      // 每次 Raft apply 后单调递增
    int64_t createdAt = 0;
    int64_t updatedAt = 0;
};

struct UploadSessionRecord {
    std::string sessionId;
    std::string ownerUserId;           // 必须与 ObjectRecord owner 一致
    std::string objectId;
    std::string manifestHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t totalChunks = 0;
    uint64_t metadataVersion = 0;
    int64_t expiresAt = 0;
    int64_t createdAt = 0;
};

struct ReplicaRecord {
    std::string nodeId;
    uint64_t nodeEpoch = 0;            // 此副本由哪个节点 incarnation 确认
    uint64_t generation = 0;           // Repair/替换副本时递增
    ReplicaState state;
    std::string verifiedHash;
    int64_t verifiedAt = 0;
};

struct ChunkRecord {
    std::string objectId;
    uint32_t index = 0;
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredRf = 2;
    ChunkState state;
    uint64_t generation = 0;           // 副本集合变更围栏
    std::vector<ReplicaRecord> replicas;
    uint64_t metadataVersion = 0;
};

struct LeaseRecord {
    std::string leaseId;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string requestKey;            // 同一请求重复的幂等键
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
    NodeHealth health;
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
    std::string taskKey;               // object/chunk/source/target/generation 的确定性 hash
    std::string objectId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    std::string sourceNodeId;
    std::string targetNodeId;
    uint64_t expectedGeneration = 0;
    RepairState state;
    uint32_t attempts = 0;
    int64_t nextRetryAt = 0;
    std::string lastError;
    uint64_t metadataVersion = 0;
};
```

### 6.1 三个关键版本字段

```text
metadataVersion：Raft 已 apply 的逻辑版本；用于缓存、读响应与乐观条件更新
placementEpoch ：节点集合/放置策略版本；旧 Route 不能继续分配到已摘除节点
generation     ：某一个 Chunk 副本集合/Repair 的版本；旧 FinishRepair 不能覆盖新 Repair
```

它们不能混用：`metadataVersion` 是全局状态演进，`placementEpoch` 是放置语义，`generation` 是单 Chunk 的副本围栏。

## 7. MetadataCommand 与结果字段

所有修改 Metadata 的操作都必须经 Raft 提交。状态机只能依赖 command payload 和当前状态，不能在 `apply()` 时查询
MySQL、DNS、系统时间或 DataNode。

```cpp
enum class MetadataCommandType {
    kCreateSession, kReserveLease, kReleaseLease,
    kCommitChunk, kCommitFile,
    kRegisterNode, kHeartbeatNode, kMarkNodeHealth,
    kCreateRepairTask, kStartRepair, kFinishRepair, kFailRepair
};

struct MetadataCommand {
    uint32_t schemaVersion = 1;
    std::string commandId;             // Gateway/DataNode 重试始终复用
    MetadataCommandType type;
    std::string actorType;             // gateway / datanode / repair-coordinator
    std::string actorId;               // gatewayId 或 nodeId，供审计
    std::string actorUserId;           // 浏览器用户操作；内部命令为空
    uint64_t expectedMetadataVersion = 0; // 0 表示该命令不要求固定版本
    uint64_t placementEpoch = 0;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    int64_t issuedAt = 0;              // 审计用途；不能影响 apply 决策
    std::string payload;               // 版本化二进制/JSON command body
};

struct ApplyResult {
    std::string commandId;
    std::string status;                // OK / ALREADY_APPLIED / NOT_FOUND / FENCED / CONFLICT / INVALID
    uint64_t term = 0;
    uint64_t logIndex = 0;
    uint64_t metadataVersion = 0;
    std::string objectId;
    std::string sessionId;
    std::string message;               // 脱敏、可观测错误；不能放 secret
};
```

`commandId → ApplyResult` 需要持久化幂等表。请求超时后，Gateway 不得换一个 commandId 再试，否则同一个 Commit/Repair
可能被执行两次；它必须用原 `commandId` 查询或再次 propose，得到 `ALREADY_APPLIED` 与原结果。

## 8. Capability 字段

能力凭证是 DataNode 的数据面授权，不是用户登录凭证。采用 HMAC-SHA-256 签名，短期有效，严格区分用途。

```text
UploadCapability
  version / purpose=upload / sessionId / chunkIndex / chunkHash / chunkSize
  leaseId / placementEpoch / targetNodeIds / expiresAt / nonce

DownloadCapability
  version / purpose=download / objectId / objectVersion / chunkHash
  targetNodeId / expiresAt / nonce
```

DataNode 验证：签名、`purpose`、HTTP method、路径 hash、目标 node、过期时间和允许的链路。Upload token 不可下载，
Download token 不可上传。token 不含密码、Cookie、MySQL session token 或用户明文资料。

## 9. 对外与内部 API 字段

### 9.1 浏览器 API

```text
POST /api/v2/auth/login
  request : username, password
  response: user{id, username}, expiresAt, Set-Cookie(md_session)

POST /api/v2/upload/preflight
  request : fileName, dirPath, fileSize, chunkSize, manifestHash, chunks[{index, hash, size}]
  response: sessionId, objectId, objectVersion, missingChunks, metadataVersion

POST /api/v2/upload/sessions/{sessionId}/routes
  request : chunks[{index, hash, size}], requestId
  response: routes[{chunkIndex, primary, chain, leaseId, placementEpoch, uploadToken, expiresAt}]

GET /api/v2/objects/{objectId}/manifest
  response: objectId, objectVersion, fileHash, chunks[{hash, size, replicas[{address, port, downloadToken}]}]
```

浏览器 API 的 `ownerUserId` 永远由 Gateway 的 `RequestPrincipal` 决定，不允许客户端 JSON 传入。

### 9.2 DataNode heartbeat

```text
POST /internal/v3/nodes/{nodeId}/heartbeat
  nodeId / nodeEpoch / freeBytes / activeUploads / activeDownloads
  diskQueueDepth / diskPauseMs / eventLoopLagUs / recentErrorCount / observedAt
```

Heartbeat 到达 Gateway/Metadata 后转成 `HeartbeatNode` command。DataNode 不能自己把状态直接写入某个 Follower 的
本地数据库。

### 9.3 Repair API

```text
POST /internal/v3/repair/start
  taskKey / objectId / chunkIndex / chunkHash / sourceNodeId / targetNodeId
  expectedGeneration / repairCapability

POST /internal/v3/repair/{taskKey}/finish
  taskKey / targetNodeId / targetNodeEpoch / expectedGeneration
  verifiedHash / size / repairCapability

POST /internal/v3/repair/{taskKey}/fail
  taskKey / expectedGeneration / retryable / errorCode
```

Repair Body 仍是 `source DataNode → target DataNode` 的流式传输；上述 API 只创建、确认或失败任务，不携带 Chunk Body。

## 10. Docker 与三主机部署字段

Docker Compose 只能管理一台 Docker daemon，不能跨三台机器直接 `compose up`。因此提供两种运行方式：

```text
本机集成：compose.v3.local.yaml
  一台机器拉起 gateway + meta-1..3 + dn-1..3，使用容器 DNS。

三主机演示：每台机器一个 compose 文件
  compose.gateway.yaml / compose.node-d.yaml / compose.ai-host.yaml
  通过 Tailscale advertise_address 通信。
```

跨主机第一版建议 `network_mode: host`：容器直接监听宿主机端口，Raft peer 使用稳定的 Tailscale 地址，避免把 Docker
bridge 地址错误写入 `advertise_address`。每个服务必须使用独立 bind mount/volume：

```text
meta-1 → /var/lib/minidriver/meta-1
meta-2 → /var/lib/minidriver/meta-2
meta-3 → /var/lib/minidriver/meta-3
dn-1   → /data/minidriver/dn-1
dn-2   → /data/minidriver/dn-2
dn-3   → /data/minidriver/dn-3
mysql  → mysql_auth_data volume
```

Raft `18001`、Metadata RPC `18101`、DataNode `19001` 和 MySQL `3306` 只允许 Tailnet 内必要来源访问；浏览器只需要
Gateway 公共端口。Compose healthcheck 只代表进程健康，不等价于“Raft 已选主”或“节点有多数派”，这两项必须由
集成测试和 `/healthz` 的 `leaderId/term/quorum` 字段额外判断。

## 11. 一致性与故障语义

| 故障 | 预期行为 |
|---|---|
| Gateway-1 停止 | LB/客户端访问 Gateway-2；共享 MySQL 校验 Cookie，Raft 读取同一 Metadata。 |
| Meta Leader 停止 | 剩余 2/3 选新 Leader；短暂 `503/NOT_LEADER` 后同 commandId 重试。 |
| 一个 Meta Follower 停止 | 2/3 多数派继续提交；恢复后日志/snapshot 追赶。 |
| 一个 DataNode 停止 | 进入 SUSPECT 再 OFFLINE；新 Placement 绕开；RF 不足创建 Repair。 |
| Repair 旧回调晚到 | generation 不匹配，`FinishRepair` 返回 FENCED，不覆盖新副本集。 |
| MySQL 停止 | 新认证/会话校验返回 `503`；不得作为 admin 继续写入；Raft Metadata 不被破坏。 |
| 网络分区少数派 Meta | 少数派无多数派，拒绝 Metadata 写，不能产生双 Leader。 |

严格副本模式下，未达到 `desiredRf` 的对象为 `DEGRADED/PROTECTING`，不能伪装成完整 `COMMITTED`。是否允许从仍健康
副本读取是独立的降级读取开关，响应必须带副本健康状态。

## 12. 分阶段实施与完成标准

### Phase A：认证与 owner（可与 V3-Lite 并行，但独立验收）

```text
MySQL users/sessions → Gateway RequestPrincipal → ownerId 隔离 → DownloadCapability
```

完成标准：双用户目录/对象隔离；Cookie 不进 DataNode；无 capability 的 Chunk 下载被拒绝。

### Phase B：V3-Lite Metadata

```text
MetadataStateMachine → 命令幂等/快照 → 成熟 Raft Adapter → 3 Meta
```

完成标准：Meta 单故障后仍可多数派 commit；重启可恢复；状态机不依赖 MySQL。

### Phase C：节点健康与 Repair

```text
Heartbeat → OFFLINE → RepairTask → DN source→target → FinishRepair fencing
```

完成标准：RF=2 的对象在一个 DN 故障后补到第三 DN，下载 SHA-256 正确。

### Phase D：三 Gateway HA

```text
3 Gateway + shared MySQL + Gateway LB/client failover + MetadataClient
```

完成标准：Gateway-1 停止后，已登录用户经 Gateway-2 可继续查询、提交幂等 Commit；不会重建不同 Session。

### Phase E：Docker 三主机与故障矩阵

```text
per-host Compose + persistent volumes + Tailscale ACL + stop/restart 演练
```

完成标准：每项故障都保存 term/index、metadataVersion、RepairTask、日志时间线、SHA-256 与资源指标。

## 13. 当前不做的事情

- MySQL 三节点 Group Replication、MySQL Router 自动切换；
- Gateway LB 自身高可用；
- 多地域/跨机房 Raft；
- 自动缩容、全量数据迁移、热点再平衡；
- OAuth/RBAC/分享链接/用户配额；
- Kafka/Spark 对对象控制面的一次性替换。

这些不是“少写几个字段”就能正确完成的功能。当前最重要的是先使每个 `ownerUserId`、`commandId`、
`metadataVersion`、`placementEpoch`、`generation` 和 capability 用途都有单一、可测试的语义。

