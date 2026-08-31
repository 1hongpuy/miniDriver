# MiniDriver 3.0 SDK：数据亲和性契约（2026-08-31）

`MiniDriverClient::getObjectReadHints(ObjectRef, ...)` 和
`batchGetObjectReadHints([ObjectRef...], ...)` 是调度器获取数据亲和性的正式接口。
`MiniDriverClient::getReadPlan(ObjectRef, ...)` 只供 SDK 的实际读取路径使用；它带有短期
DataNode read capability，不应作为调度器的持久化输入。计算框架不读取 DataNode extent、
数据目录或内部索引。

## 调度输入

持久化的对象引用只有：

```text
objectId + objectVersion
```

调度前，Scheduler 可一次对多个对象请求 `ObjectReadHints`。每个对象的
`candidates[]` 包含：

```text
nodeId          # 亲和性 / 调度标签
localBytes      # 此节点拥有的对象副本字节数
coverageRatio   # localBytes / object size
health          # Gateway 心跳快照，不是资源预留
```

调度器可按 `localBytes` 与自身 Worker 空闲 slot 打分，并优先把计算任务放到拥有最多本地
数据的节点。真正读取时，Worker 仍调用 `getObject`/`getRange`；SDK 在内部获取短期
`ObjectReadPlan`，按候选副本、校验和与有界 fallback 执行。调度器不直接打开 DataNode
本地文件。

## 生命周期边界

`ObjectRef` 是唯一可持久化的版本化身份。`ObjectReadHints` 是短期调度快照；
`ObjectReadPlan`、endpoint 与 `readCapability` 则是更短期的读取授权和拓扑快照。三者都不应
写入任务数据库后长期复用。这样节点迁移、Capability 撤销或副本变化不会让计算任务持有失效位置。

## 最小 Worker 流程

```text
持久化 ObjectRef
  → 调度前 batchGetObjectReadHints([ObjectRef...])
  → 用 candidates[].localBytes / health 做本地性打分
  → 在选定 Worker 上 getObject(ObjectRef, sink, ...)
  → SDK 获取 ReadPlan、直接读取 DataNode，并验证 whole-chunk checksum
```

仓库中的 `minidriver_client_worker` 是该边界的参考实现：仅接受 Gateway 身份和
`objectId + objectVersion`，通过 SDK 取得 ReadPlan 后读取对象；它不接触 extent 路径。

## 非承诺

当前 `nodeId` 表示数据副本候选，不是资源预留、负载均衡或“必然本地命中”保证。`health` 也是
心跳快照，不能替代 Scheduler 自身的资源判断。调度器应把亲和性视为优先级；节点不可用时，SDK
可按同一 ReadPlan 的副本候选有界 fallback。
