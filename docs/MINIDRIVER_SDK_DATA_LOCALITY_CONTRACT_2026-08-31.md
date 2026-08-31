# MiniDriver 3.0 SDK：数据亲和性契约（2026-08-31）

`MiniDriverClient::getReadPlan(ObjectRef, ...)` 是原生计算 Worker 获取数据位置的正式接口。
计算框架不读取 DataNode extent、数据目录或内部索引。

## 调度输入

持久化的对象引用只有：

```text
objectId + objectVersion
```

任务开始时，Worker 使用该引用请求最新 `ObjectReadPlan`。每个 `ChunkReadPlan` 的
`replicas[]` 都包含：

```text
nodeId       # 亲和性 / 调度标签
endpoint     # 当前 DataNode 数据面地址
readCapability
```

调度器可按 Chunk 的 `replicas[].nodeId` 统计候选 Worker 的本地数据量，并优先把计算任务
放到拥有最多本地 Chunk 副本的节点。读取仍由 SDK 按 ReadPlan 的候选顺序、校验和与有界副本
fallback 执行；调度器不直接打开 DataNode 本地文件。

## 生命周期边界

`ObjectRef` 是可持久化的版本化身份。`ObjectReadPlan`、endpoint 与 `readCapability` 是短期
授权和拓扑快照：任务实际开始前重新获取，不能写入任务数据库后长期复用。这样节点迁移、Capability
撤销或副本变化不会让计算任务持有失效位置。

## 最小 Worker 流程

```text
持久化 ObjectRef
  → 调度前 getReadPlan(ObjectRef)
  → 用 replicas[].nodeId 做本地性打分
  → 在选定 Worker 上再次/立即使用该 ReadPlan
  → SDK 直接读取 DataNode，并验证 whole-chunk checksum
```

仓库中的 `minidriver_client_worker` 是该边界的参考实现：仅接受 Gateway 身份和
`objectId + objectVersion`，通过 SDK 取得 ReadPlan 后读取对象；它不接触 extent 路径。

## 非承诺

当前 `nodeId` 表示数据副本候选，不是资源预留、负载均衡或“必然本地命中”保证。调度器应把亲和性
视为优先级；节点不可用时，SDK 可按同一 ReadPlan 的副本候选有界 fallback。
