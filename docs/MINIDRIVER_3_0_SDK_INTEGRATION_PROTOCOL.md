# MiniDriver 3.0：SDK 集成与数据访问协议

> 状态：v3.0 C++ SDK 的当前调用契约。它描述调用方应使用的稳定边界，不把 DataNode 文件路径、extent
> 或 Gateway/DataNode 内部控制接口作为第三方 API。

## 1. 调用方实际拿到什么

MiniDriver 对原生 Worker 暴露的是 C++ SDK：

```text
include/client/MiniDriverClient.hpp
target: minikv_client
```

调用方配置 Gateway 地址、集群内部凭据和服务身份；读写数据经 SDK 完成。调用方**不**自行向 DataNode 猜测
URL、访问 DataNode 数据目录或保存 capability。

```cpp
#include "client/MiniDriverClient.hpp"

miniKV::client::ClientConfig config;
config.gateway = {"gateway.example.internal", 8081};
config.clusterInternalToken = "<injected-secret>";
config.servicePrincipal = "lens-worker-a";

miniKV::client::MiniDriverClient client(std::move(config));
```

`clusterInternalToken` 必须从部署的 secret store、容器 secret 或受控环境变量注入，不能硬编码或写进任务库。
`servicePrincipal` 是 Worker 的服务身份。当前 v3 ReadPlan 端点要求二者同时存在。

## 2. 上传：本地文件 → 版本化 ObjectRef

```cpp
miniKV::client::UploadOptions options;
options.chunkWindow = 2;             // 每个 SDK 实例同时在途的 Chunk 数
options.checksumType = "crc32c";     // v3 默认；显式 legacy 才用 "sha256"

miniKV::client::UploadResult uploaded;
std::string error;
if (!client.uploadFile("/work/input.bin", "input.bin", "/datasets/raw",
                       options, uploaded, error)) {
    throw std::runtime_error(error);
}

// 持久化这两个字段，作为后续读取、计算和审计身份。
const miniKV::client::ObjectRef object = uploaded.object;
// object.objectId, object.objectVersion
```

SDK 内部协议顺序为：

```text
POST Gateway /api/v2/upload/sessions
  → GET  Gateway /api/v2/upload/sessions/{sessionId}
  → 每个 Chunk：向 Gateway 获取 route/capability，再 PUT 到指定 DataNode/replica
  → POST Gateway /api/v2/upload/sessions/{sessionId}/commit
  → UploadResult.object = { objectId, objectVersion }
```

`sessionId + chunkIndex` 在 SDK 的有界重试中保持稳定，避免一次重连产生第二个逻辑 Chunk。快速路径使用
CRC32C；它不在上传热路径计算 SHA-256。`fileHash` 只为 V2 兼容保留，不应作为新 Worker 的对象身份。

## 3. 完整对象读取：ObjectRef → 文件

```cpp
miniKV::client::ObjectReadPlan plan;
std::string error;
if (!client.getReadPlan(object, plan, error)) {
    throw std::runtime_error(error);
}

miniKV::client::ReadOptions read;
read.keepAlive = true;
read.verifyChecksum = true;
read.allowReplicaRetry = true;
read.maxReplicaAttempts = 2;

miniKV::client::TransferStats stats;
if (!client.downloadToFile(plan, "/work/output.bin", read, stats, error)) {
    throw std::runtime_error(error);
}
```

SDK 向 Gateway 请求：

```text
POST /internal/v3/objects/{objectId}/versions/{objectVersion}/read-plan
Headers:
  X-Cluster-Internal-Token: <secret>
  X-Service-Principal: <worker identity>
```

Gateway 返回短期 `ObjectReadPlan`。SDK 使用其中每个 Chunk 的 read capability 直连 DataNode，验证完整
Chunk checksum，并在候选副本间作有界 fallback。调用方不应缓存 capability，也不应自己伪造 `X-Read-Token`。

## 4. 数据亲和性：为分布式计算调度提供什么

每个 `ChunkReadPlan` 的 `replicas` 是候选副本列表：

```cpp
for (const auto& chunk : plan.chunks) {
    for (const auto& replica : chunk.replicas) {
        // replica.nodeId   ：调度亲和性标签
        // replica.endpoint ：当前数据面地址，由 SDK 实际使用
    }
}
```

计算调度器应：

```text
持久化 ObjectRef
  → 任务调度前获取新 ReadPlan
  → 统计每个候选 Worker/nodeId 持有的本地 Chunk 字节数
  → 优先把任务提交给本地数据最多的 Worker
  → Worker 仍调用 SDK 下载；不读取 extent
```

`nodeId` 是亲和性提示而不是资源预留。节点不可用、Capability 过期或副本错误时，SDK 按同一 ReadPlan 的候选副本
有限 fallback。详见 [数据亲和性契约](MINIDRIVER_SDK_DATA_LOCALITY_CONTRACT_2026-08-31.md)。

## 5. Range 读取

```cpp
miniKV::client::RangeReadResult rangeResult;
if (!client.downloadRangeToFile(plan, /* offset */ 1024, /* length */ 4096,
                                "/work/part.bin", read, stats, rangeResult, error)) {
    throw std::runtime_error(error);
}
```

完整覆盖的 Chunk 可被 whole-chunk checksum 验证。只要范围涉及部分 Chunk，结果会显式标记为
`kUnverifiedPartialRange`；当前没有 segment checksum sidecar，因此调用方不能将部分 Range 当作完整内容证明。

## 6. 并发与生命周期

- 一个 `MiniDriverClient` 正常由一个 Worker 持有，顺序 HTTP/1.1 Keep-Alive 连接在同一 Worker 内复用。
- 多对象并发使用多个 Worker/SDK 实例；当前单 DataNode 开发机的推荐起点是上传 c8、下载 c8。
- `UploadOptions::chunkWindow` 控制一个上传实例的内部 Chunk 并行度；它不是全局并发上限。
- 上传和下载协议允许同时进行，但尚未完成读写混合负载矩阵。因此 v3.0 没有发布“边上传边下载”的吞吐或 P95 承诺。
- 当前读写性能数据来自单机、loopback、warm cache；真实多主机容量必须在独立磁盘和真实 NIC 上复测。

## 7. 参考入口

`minidriver_client_worker` 是最小可执行读取样例：

```bash
build/bin/minidriver_client_worker read \
  --gateway gateway.example.internal:8081 \
  --cluster-token "$MINIKV_V2_CLUSTER_SECRET" \
  --service-principal lens-worker-a \
  --object-id '<objectId>' \
  --object-version 1 \
  --output /work/output.bin
```

它故意只接受 Gateway 身份和 `ObjectRef`，以确保未来 LensCompute/其他原生 Worker 不绕开 SDK 的授权、校验和
副本恢复语义。
