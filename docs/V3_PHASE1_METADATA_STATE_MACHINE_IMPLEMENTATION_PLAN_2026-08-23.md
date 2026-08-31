# miniDriver V3-Lite Phase 1：MetadataStateMachine 实现原理与修改计划（2026-08-23）

> 工作版本：V3-Lite。  
> 起始代码基线：V2.0.1 数据面。  
> 本阶段目标：先完成可验证的单节点、确定性元数据状态机；**本阶段不接入 Raft，不迁移 Gateway HTTP 路径，也不传输文件 Body**。

## 1. 本阶段最终目的

V2 的 `GatewayState` 同时承担 HTTP 业务状态、LevelDB 持久化和节点放置决策。它可以让单 Gateway 工作，但无法证明三台 Metadata 节点在收到同一组操作时会得到同一结论。

Phase 1 先把“结论如何产生”从 Gateway 中抽离：

```text
MetadataCommand（已包含所有外部输入）
  → MetadataStateMachine::apply(command, appliedIndex)
  → ApplyResult + 新状态
```

之后 Phase 2 的 Raft 只负责让三台 Metadata Member 按相同顺序交付同一命令：

```text
Raft：保证相同顺序
StateMachine：保证相同结果
```

因此，Phase 1 成功的定义不是“已经高可用”，而是：相同初始状态、相同命令序列和相同已应用索引，必定产生相同状态摘要与相同 `ApplyResult`。这是后续选主、重试、故障恢复和 Repair 正确性的基础。

## 2. 不变量与边界

状态机只能依赖命令中显式给出的数据；以下行为禁止出现在 `apply()` 中：

- 读取本机时间、随机数、环境变量或配置文件；
- 访问网络、MySQL、Redis、DataNode 或 Gateway；
- 直接写 LevelDB/WAL；
- 启动线程、定时器、Repair 传输或 HTTP 回调。

`issuedAt`、`expiresAt`、`observedAt` 等时间由 Gateway/Coordinator 在创建命令时填入。Phase 2 中由 Raft 提交索引传给 `apply()`；本阶段测试显式提供单调递增的 `appliedIndex`。

另外，Raft 库将在 Phase 2 成为日志/WAL 的唯一权威来源。因此 Phase 1 只实现内存状态与可编码 Snapshot，**不再新增一套会与 Raft Log 竞争的 LevelDB 命令日志**。若需要本地持久化，后续由 `IMetadataStore` 承载 Raft Snapshot，而非复制一份业务操作日志。

## 3. 第一批实现的模型与命令

先定义完整的记录骨架，先实现上传与节点围栏所需的最小命令：

```text
记录：Object / UploadSession / Chunk / Replica / Lease / Node / RepairTask

已实现命令：
  CreateSession, RegisterNode, HeartbeatNode, MarkNodeHealth,
  ReserveLease, CommitChunk, CommitFile

仅定义协议类型、留待 Phase 4 实现：
  ReleaseLease, CreateRepairTask, StartRepair, FinishRepair, FailRepair
```

这样 P1 可以独立验证上传控制面最关键的 Session → Lease → Chunk Commit → File Commit，且不会提前把尚未存在的 Repair HTTP 接口伪装为已实现。

## 4. 数据与围栏语义

| 字段 | 含义 | P1 规则 |
|---|---|---|
| `commandId` | 一次业务动作的稳定幂等键 | 已执行过时直接返回首次 `ApplyResult`，绝不再次修改状态。 |
| `metadataVersion` | 全局已应用元数据版本 | 等于成功/失败命令处理时对应的 `appliedIndex`；Phase 2 对应 Raft `lastApplied`。 |
| `nodeEpoch` | 某 DataNode 的运行 incarnation | `RegisterNode` 由 Metadata 分配；新 `bootId` 递增 epoch，旧 epoch heartbeat/commit 会被 `FENCED`。 |
| `placementEpoch` | 可放置节点集合版本 | 注册新节点或状态变更后递增；Lease 与 Commit 必须匹配。 |
| `generation` | 某 Chunk 副本集合/Repair 代次 | P1 在 Commit 记录；Phase 4 用于拒绝旧 Repair 完成回调。 |

P1 采用 V3-Lite 的严格副本语义：`CommitChunk` 只有在回报的健康副本数达到 `desiredRf` 后才能把 Chunk 标为 `COMMITTED`；`CommitFile` 只有所有 Chunk 都已完成时才能把 Object 标为 `COMMITTED`。

## 5. 文件改造计划

```text
新增 include/metadata/MetadataTypes.hpp
  枚举、Object/Session/Chunk/Lease/Node/RepairTask 记录与 ApplyResult。

新增 include/metadata/MetadataCommand.hpp
  类型安全的命令 payload、MetadataCommand、二进制编解码接口。

新增 include/metadata/MetadataStateMachine.hpp
  apply/query/snapshot/restore 与只读查询接口。

新增 src/metadata/MetadataCommand.cpp
  长度前缀二进制编码，保证 command/snapshot 可跨 Member 传递；不依赖 JSON 或本机格式。

新增 src/metadata/MetadataStateMachine.cpp
  确定性状态转移、commandId 结果账本、Snapshot 生成与恢复。

修改 CMakeLists.txt
  新增 minikv_metadata 静态库，以及状态机与 Snapshot 两个 CTest 目标。

新增 test/test_metadata_state_machine.cpp
  覆盖幂等、严格 RF、版本/epoch fencing 与固定序列确定性。

新增 test/test_metadata_snapshot.cpp
  覆盖 Snapshot restore、已执行 commandId 的恢复与状态摘要一致性。
```

本阶段**不修改** `GatewayState`、`gateway_main.cpp`、DataNode 上传 Body、Redis、MySQL 或 Docker 编排；这些改动分别属于 P3、P4、P5。这样失败可以局限在新库与测试中，V2 数据面保持可回归。

## 6. 核心状态转移

```text
RegisterNode(new bootId)
  → 分配 nodeEpoch，Node=RECOVERING，placementEpoch++

HeartbeatNode(correct epoch)
  → 更新资源快照与 observedAt

CreateSession
  → Object=UPLOADING，Chunk=ALLOCATED

ReserveLease
  → 只选 ONLINE、epoch 有效的节点；写 placementEpoch

CommitChunk
  → 校验 session / lease / hash / size / placementEpoch / nodeEpoch
  → RF 达标：Chunk=COMMITTED、replica=HEALTHY

CommitFile
  → 所有 Chunk=COMMITTED
  → Object=COMMITTED
```

`MarkNodeHealth` 不从本机时钟推导状态；Leader 后续扫描发现超时后，显式提交带 `observedAt` 的命令。Repair 命令类型虽会先占位，但真正的 `taskKey`、临时 extent、SHA-256 finalize 与 `generation` 校验留到 P4 一次完成。

## 7. 测试与停止条件

必须先通过以下测试才进入 Phase 2：

1. 同一 `commandId` 的 Lease、Chunk Commit、File Commit 重放只产生一个状态变化；
2. 副本数不足时，Chunk/File 不能变成 `COMMITTED`；
3. `placementEpoch`、`nodeEpoch` 或 `generation` 过期时得到 `FENCED`/`CONFLICT`；
4. 任何固定命令前缀在两个全新状态机上执行，状态摘要相同；
5. Snapshot restore 后，查询结果与 commandId 幂等结果相同。

未通过以上任一项，不选 Raft 库、不改 Gateway Handler。通过后才进入“成熟 Raft Adapter PoC”，让 Raft 负责日志复制、选主和持久化恢复。
