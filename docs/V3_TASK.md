# miniDriver 3.x 实施任务与学习路线

> 当前工作版本：MiniDriver 3.0 高性能与持久化数据面。
> 状态：性能 P0/P1 已形成基线和观测；P2 `buffered/chunk_sync`、P3 `256 KiB pwritev` 和 P4 Group Commit
> 已完成并通过新 SSD/RF=2 验收；P5-A 协议契约、P5-B 写入协调边界以及 P5-C 双身份/CRC32C 正确性闭环已完成。
> 当前下一项是新 SSD 上的 SHA-256/CRC32C 单变量性能 A/B 与对象多版本语义收口。
> 起始代码基线：V2.0.1 数据面（仅表示迁移起点，不是正在发布的版本）
> 版本决策（2026-08-27）：原 V3-Lite MetadataStateMachine Phase 1 代码与测试保留；下文 Raft、Health、Repair 任务整体
> 调整为 3.1 高可用主线，不作为 3.0 发布门槛。

> 3.0 先完成吞吐、P95/P99、背压、admission、聚合写、明确落盘、协议解耦和 Docker 可复跑部署。
> 结果见 [性能基线与瓶颈定位报告](MINIDRIVER_PERFORMANCE_BASELINE_AND_BOTTLENECK_REPORT_2026-08-25.md) 和
> [MiniDriver 3.0 高性能数据面主实施设计](MINIDRIVER_HIGH_CONCURRENCY_AND_PERFORMANCE_IMPLEMENTATION_PLAN_2026-08-25.md)。

## 1. 3.1 高可用目的（3.0 完成后恢复）

V2 已经能够完成客户端直传、分块上传、链式双副本、断点续传和有界背压。V3-Lite 不再继续堆叠上传
吞吐优化，而是解决 V2 的控制面可靠性问题：

```text
元数据只有一个副本       → 三节点 Raft 元数据集群
节点故障后状态不明确      → ONLINE/SUSPECT/OFFLINE 状态机
副本失败后没有恢复闭环    → 基础 RepairTask 自动补副本
重复请求可能重复修改状态  → commandId 幂等
旧节点可能覆盖新状态      → epoch/generation fencing
```

最终希望能够演示完整故障链：

```text
DataNode 故障
  → Metadata 标记 OFFLINE
  → 发现副本数不足
  → 创建 RepairTask
  → 健康 DataNode 流式复制到第三节点
  → SHA-256/长度校验
  → 原子 finalize
  → Metadata 确认副本恢复
```

这是一份学习型分布式对象存储控制面，不以生产级云存储为目标。

## 2. V3-Lite 范围

### 保留并实现

- 一个 Gateway；
- 三个 Metadata/Raft 节点；
- 三个 DataNode；
- 成熟 Raft 库，不手写 Raft 协议；
- MetadataStateMachine 和命令编解码；
- Metadata Leader/Follower 转发；
- DataNode heartbeat 和节点健康状态；
- RF=2 的单任务副本 Repair；
- commandId 幂等和 epoch fencing；
- V2 数据面全部保持：Chunk、SHA-256、链式副本、SharedBodyBlock、磁盘队列和背压。
- Docker Compose 本机集成、三主机 Compose 运维与可重复故障注入；

### 明确延期

- 三 Gateway 高可用和外部 L4/L7 LB；
- 全量数据迁移和节点缩容；
- 复杂热点均衡和长期再平衡；
- 多故障域、跨机房一致性；
- 自研 Raft 协议和自研 Raft Transport；
- gRPC 替换现有 HTTP/TCP；
- 生产级权限、密钥轮换和审计系统。
- Kubernetes/k3s 编排；它作为 V3.2 的有状态服务部署实验，不与 Raft/Repair 首轮调试混合。

延期功能统一放入 V3.1，不为赶进度伪装成 V3-Lite 已完成。

## 3. 目标架构

```text
                         Client
                           │
                           ▼
                    Gateway（单实例）
                     │ Metadata RPC
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Meta-1      Meta-2      Meta-3
          └────────── Raft ────────┘

正常数据路径：Client → DataNode-1 → DataNode-2
修复路径：    DataNode-2 → DataNode-3
```

Metadata 节点可以先作为独立 `metadata_node` 进程运行。这样比“三 Gateway 内嵌三 Raft”简单，也能真实
验证 Raft 节点故障。未来要做 Gateway HA 时，再把 `MetadataClient` 接入多个 Gateway 即可。

### 3.1 端口与目录

```text
Gateway HTTP       :8080
Metadata-1 Raft    :18001
Metadata-2 Raft    :18002
Metadata-3 Raft    :18003
Metadata RPC       :18101/18102/18103（可合并到 Raft 库 Transport）
```

每个 Metadata 节点拥有独立目录：

```text
/data/minidriver/v3/meta-1/
/data/minidriver/v3/meta-2/
/data/minidriver/v3/meta-3/
```

禁止共享 LevelDB 文件。Raft 复制的是命令日志，不是数据库文件。

## 4. 核心设计思路

### 4.1 Raft 只复制小型元数据

Raft 命令包括：

```text
CreateSession
ReserveLease
CommitChunk
CommitFile
MarkNodeHealth
CreateRepairTask
FinishRepair
FailRepair
```

Raft 不传输：

```text
4 MiB Chunk
64 KiB SharedBlock
HTTP Body
pwrite 数据
```

### 4.2 写入流程

```text
Gateway
  → MetadataClient
  → 当前 Leader
  → Raft 多数派提交
  → MetadataStateMachine::apply()
  → 返回 metadataVersion/result
```

如果 Gateway 访问到 Follower：

```text
Follower → NOT_LEADER + leaderHint
Gateway  → Leader 重试
```

每个命令必须带稳定的 `commandId`。请求超时不能直接认为命令未执行，重试时必须返回原来的 ApplyResult。

### 4.3 状态与 fencing

对象状态：

```text
UPLOADING → PROTECTING → COMMITTED
                     └──→ DEGRADED → REPAIRING → COMMITTED
```

节点状态：

```text
JOINING → ONLINE → SUSPECT → OFFLINE
                         └──→ RECOVERING → ONLINE
```

命令携带：

```text
commandId / metadataVersion / placementEpoch / nodeEpoch / generation
```

旧 epoch 或旧 generation 的 Commit、Repair 完成请求必须返回 `FENCED`，不能覆盖新状态。

## 5. 实施任务

## Phase 0：冻结 V2、建立 V3 分支（进行中）

- [x] 确认 V2.0.0 标签和当前工作区状态；
- [x] 从 V2.0.0 创建 `v3` 分支；
- [x] 保存 V2 的 CTest、upload/download/mixed 基线；
- [x] 增加 `MINIKV_V3_MODE=single|ha` 配置契约；
- [x] 准备 V3 独立配置、数据目录和日志目录；
- [x] 增加可注入时钟、网络断开、延迟和进程故障接口骨架；
- [x] 编写三 Metadata 节点的本地启动脚本契约（Phase 2 构建可执行目标后启用）。

验收：V2 行为不变，V3 进程可以独立启动和清理，不污染 V2 数据目录。

当前记录见 [`V3_PHASE0_BASELINE_2026-08-18.md`](V3_PHASE0_BASELINE_2026-08-18.md)。构建已通过，完整
本机 CTest 为 47/48；唯一失败是缺少 `playwright` 的 UI 测试。Metadata/Raft 启动脚本留在 Phase 2，
因为当前成熟 Raft 库和 `minidriver_v3_metadata` 可执行目标尚未接入；当前脚本只校验该启动契约。

## Phase 1：MetadataStateMachine

### 目标

把原来 GatewayState 中的控制状态抽成确定性的、可重放的状态机。

### 任务

- [x] 定义 `ObjectRecord`、`ChunkRecord`、`ReplicaRecord`、`RepairTask`；
- [x] 定义 `MetadataCommand` 和 `ApplyResult`；
- [x] 实现 `apply()`、只读查询、`snapshot()`、`restore()`；
- [x] 实现 P1 已支持命令的二进制编解码和 `schemaVersion`；
- [x] 为 P1 已支持写命令增加 commandId 幂等结果账本；
- [x] 加入 expectedVersion、placementEpoch、nodeEpoch、generation 校验；
- [ ] Phase 2 接入 Raft 后，为 Snapshot 增加持久化 Store；Raft WAL 是唯一命令日志，不新增竞争的 LevelDB 业务日志。

### 必须测试

- [x] 相同 commandId 重放不会创建两份 Lease；
- [x] 重复 CommitChunk/CommitFile 返回相同结果；
- [x] 快照恢复后查询结果一致；
- [x] 旧版本、placement epoch、node epoch 和 generation 请求被拒绝；
- [x] 固定命令序列重放结果确定。

验收：不接 Raft 时，单节点状态机测试全部通过。

## Phase 1.5：控制面边界治理（下一步）

### 目标

在接入 Raft 前，先将 Gateway 的 HTTP 协议适配与控制面业务状态分开；保持现有 V2 HTTP URL、上传 Capability、
客户端直传和 DataNode 背压路径不变。

当前上传路径中，`TcpConnection`/`HttpContext` 的 streaming pause/resume、`DiskWriteExecutor`、
`FastDataStore`、`SharedBodyBlock` 与 `ReplicaUploadPipe` 的协作是高性能数据面的一部分，不因抽象协议而重写。
真正优先治理的是 `gateway_main.cpp` 直接解析 JSON、调用 `GatewayState` 并决定业务响应的耦合，以及
`GatewayState` 同时承担 Session、Lease、Placement、目录、媒体任务和 Outbox 的全能职责。

### 任务

- [ ] 定义协议无关的 `MetadataFacade` request/result：preflight、Session、Route/Lease、Chunk/File Commit、
  Node register/heartbeat、object/manifest query；
- [ ] 实现临时 `LegacyGatewayStateFacade`，将 Facade 调用翻译到现有 `GatewayState`；
- [ ] 将上述 Gateway HTTP Handler 改为 DTO/JSON → Facade → HTTP response 映射；
- [ ] 保留 `GatewayControlClient` 作为 DataNode 控制面语义接口，继续由 `ControlRequestCodec` 承载当前 HTTP 编码；
- [ ] 将 `ChunkUploadStream` 从 `datanode_main.cpp` 移出为 `ChunkUploadCoordinator`，但不改变共享 Block、
  本地写盘、副本 ACK、队列水位线和 socket pause/resume 算法；
- [ ] 对照 V2 现有上传/下载/压力测试，验证 HTTP 返回、Capability、背压指标和 SHA-256 不回归。

### 验收

```text
HTTP Handler 不再直接修改 GatewayState；
未来 Raft MetadataClient 可替换 Facade 后端而不改浏览器 API；
Chunk Body 仍只走 Client → DataNode → Replica，不进入 Facade/Raft；
Disk/Replica 任一队列饱和时，仍能 pauseRead() 并在 drain 后 resumeRead()。
```

详细耦合审计与目标分层见
[MiniDriver 后续对象存储路线图](MINIDRIVER_FUTURE_STORAGE_PLATFORM_ROADMAP_2026-08-25.md)。

## Phase 2：接入成熟 Raft 库

### 目标

使用成熟 Raft 库完成选主、日志复制、持久化和快照；我们只实现业务适配层。

### 选型检查

- [ ] 兼容 C++17/CMake；
- [ ] 许可证允许本项目使用；
- [ ] 支持三节点选主和多数派提交；
- [ ] 支持节点重启恢复；
- [ ] 支持 snapshot/log compaction，或能安全延期；
- [ ] 提供 apply 回调、term、index、leader 信息；
- [ ] 可以注入网络故障和替换存储层。

### 任务

- [ ] 编写 `RaftAdapter`；
- [ ] 编写 `RaftConfig` 和 peer 配置；
- [ ] 接入三节点本地 Transport；
- [ ] 将序列化后的 MetadataCommand 提交给 Raft；
- [ ] 在 commit 后调用 MetadataStateMachine::apply；
- [ ] 将 apply 结果安全投递回 Metadata EventLoop；
- [ ] 处理 `NOT_LEADER`、超时、重复响应和节点重启；
- [ ] 记录 term、commitIndex、leaderId 和 apply latency。

验收：三节点中停止一个节点，剩余两个节点仍可提交元数据；停止 Leader 后能重新选主。

## Phase 3：Gateway MetadataClient

- [ ] Gateway 不再直接修改控制状态；
- [ ] 所有 Session/Lease/Route/Commit 转换为 MetadataCommand；
- [ ] 实现 Leader 查询和 Follower 转发；
- [ ] 实现 commandId 重试；
- [ ] 返回 metadataVersion、状态和降级原因；
- [ ] 线性一致性查询走 Leader，普通列表查询可读本地缓存。

验收：Gateway 重启后，已有 Session 和 Commit 状态仍由 Metadata 集群保留。

## Phase 4：DataNode 心跳与健康状态

- [ ] DataNode heartbeat 上报 nodeId、nodeEpoch、空间、活动上传、磁盘队列和错误计数；
- [ ] 实现 ONLINE/SUSPECT/OFFLINE/RECOVERING；
- [ ] 心跳超时状态通过 MetadataCommand 写入 Raft；
- [ ] OFFLINE 节点不再参与新 Placement；
- [ ] 旧 nodeEpoch 的请求返回 FENCED；
- [ ] 节点恢复后先校验本地数据，再回到 ONLINE。

验收：停止一个 DataNode 后，Metadata 在限定时间内标记 OFFLINE，新的写入不会选择它。

## Phase 5：基础副本 Repair

### 任务

- [ ] 扫描 `ChunkRecord`，发现健康副本数低于 desiredRf；
- [ ] 使用确定性的 `taskKey` 创建 RepairTask；
- [ ] 选择一个健康源和一个健康目标；
- [ ] 目标写入临时 extent；
- [ ] 源节点流式读取，目标节点流式写入；
- [ ] 校验 SHA-256、长度和 chunkHash；
- [ ] 成功后原子 finalize；
- [ ] 通过 `FinishRepair` 更新 Metadata；
- [ ] 失败时有限重试和指数退避；
- [ ] Repair 使用独立有界并发，不挤占前台上传槽。

验收：DataNode-1 故障后，DataNode-2 的副本能够恢复到 DataNode-3，且下载 SHA-256 正确。

## Phase 6：Docker Compose 运维与故障注入

### 本机 Compose

- [ ] 编写统一 C++ 运行镜像的 `Dockerfile.minidriver`；
- [ ] 编写 `deploy/v3/compose.local.yaml`，拉起 gateway、meta-1..3、dn-1..3；
- [ ] 为每个 Meta/DN 声明独立持久化 volume，禁止共享 Metadata WAL、snapshot 或 DataNode 数据；
- [ ] 只将 Gateway HTTP 端口映射给浏览器；Raft/Metadata RPC/DataNode 端口按本机实验需求最小暴露；
- [ ] 实现 `/healthz`（进程存活）和 `/readyz`（Raft leader/quorum、lastApplied 等就绪状态）；
- [ ] 提供 `make v3-local-up`、`v3-status`、`v3-logs`、`v3-local-down`、`v3-fault-meta-1`、`v3-fault-dn-1`；
- [ ] 文档明确正常停止使用 `docker compose stop/down`，故障/恢复测试禁止使用 `down -v` 删除状态 volume。

### 三主机 Compose

- [ ] 按 `gateway`、`node-d`、`ubuntu22data1` 分别提供 host-a/host-b/host-c Compose 覆盖配置；
- [ ] 跨主机 Raft peer、DataNode address 使用 Tailscale `advertise_address`，不使用容器 `172.x` 地址；
- [ ] 第一版采用 `network_mode: host` 或等价的明确端口映射，避免 NAT 破坏 Raft peer 身份；
- [ ] 保存每台主机的 image tag、私有配置、volume 路径、启动/停止/日志命令和端口 ACL；
- [ ] 使用 `docker compose stop meta-1`、`stop dn-1` 完成 Meta/DataNode 故障演练。

验收：从空运行目录可用一组命令拉起本机七组件；停止单个 Meta 或 DataNode 后，日志和状态接口能够证明预期的
选主、OFFLINE 或 Repair 行为；重启容器后 WAL/snapshot/DataNode 数据仍在。

## Phase 7：综合测试与 V3-Lite 报告

- [ ] 三节点选主和 Leader 故障测试；
- [ ] Follower 掉线和恢复测试；
- [ ] DataNode 故障、Repair 和节点恢复测试；
- [ ] 重复 Commit、重复 Repair、旧 epoch 请求测试；
- [ ] V2 upload/download/mixed 回归；
- [ ] 记录 Metadata command latency、Raft apply latency、Repair latency；
- [ ] 记录文件/Chunk P50/P95/P99、503、SHA-256、CPU 和 RSS；
- [ ] 编写 V3-Lite acceptance report；
- [ ] 只有所有验收通过后才创建 `v3.0.0`。

## 6. 推荐代码结构

```text
include/metadata/
  MetadataTypes.hpp
  MetadataCommand.hpp
  MetadataStateMachine.hpp
  MetadataSnapshot.hpp

include/raft/
  RaftAdapter.hpp
  RaftConfig.hpp
  RaftCommandCodec.hpp

include/control/
  MetadataClient.hpp
  NodeHealthCoordinator.hpp
  RepairCoordinator.hpp

src/metadata/
src/raft/
src/control/

test/
  test_metadata_state_machine.cpp
  test_metadata_snapshot.cpp
  test_raft_three_nodes.cpp
  test_gateway_metadata_client.cpp
  test_node_health.cpp
  test_replica_repair.cpp
```

## 7. 推荐时间安排

按每天约 5 小时：

| 周期 | 任务 |
|---|---|
| 第 1 周 | Phase 0、配置、故障注入、V2 基线 |
| 第 2 周 | MetadataStateMachine、LevelDB、重放和快照 |
| 第 3 周 | Mature RaftAdapter、三节点选主和日志复制 |
| 第 4 周 | Gateway MetadataClient、Leader 转发、幂等 |
| 第 5 周 | DataNode 健康检测、epoch fencing |
| 第 6 周 | 基础 Repair 与数据完整性测试 |
| 第 7 周 | Docker Compose 本机集成、容器故障注入 |
| 第 8 周 | 三主机 Compose 演练、综合测试和报告 |

如果某一阶段的测试没有通过，不进入下一阶段。特别是 StateMachine 未通过重放测试时，不接入 Raft；
Raft 多数派未通过时，不开始 Repair。

## 8. V3-Lite 完成定义

满足以下条件即可认为 V3-Lite 完成：

```text
元数据：三节点可选主，多数派提交，重启可恢复
网关：能访问 Leader，Follower 能转发，重复命令幂等
节点：心跳超时可进入 OFFLINE，旧 epoch 被拒绝
副本：故障后可从健康源修复到第三节点，SHA-256 正确
兼容：V2 数据面测试不退化，未引入无界队列或数据损坏
运维：Docker Compose 可重复拉起/停止；每个状态组件拥有独立持久化 volume
证据：有完整日志、测试命令、故障时间线和性能报告
```

## 9. 后续 V3.1

V3-Lite 稳定后再考虑：

- 三 Gateway 和外部 LB；
- Metadata 独立扩缩容；
- 新节点加入后的数据迁移；
- 复杂负载均衡和热点再平衡；
- 多副本故障域和跨地域部署。
- Kubernetes/k3s：Gateway 使用 Deployment/Ingress，Metadata/DataNode 使用 StatefulSet、稳定 memberId 与独立 PVC；
  在 Compose 版本稳定前不实施。

V3-Lite 的价值不是功能数量，而是完整掌握下面这条链路：

```text
Raft 保证元数据一致
  → 健康状态识别节点故障
  → Repair 恢复副本
  → fencing 防止旧节点覆盖新状态
```
