# MiniDriver 3.0：SDK 集成与数据访问协议

> 状态：v3.0 C++ SDK 的当前调用契约。它描述调用方应使用的稳定边界，不把 DataNode 文件路径、extent
> 或 Gateway/DataNode 内部控制接口作为第三方 API。

## 1. 调用方实际拿到什么

MiniDriver 对原生 C++ Worker 暴露 C++ SDK，并提供同一对象协议的原生 Go SDK：

```text
include/client/MiniDriverClient.hpp
target: minikv_client
Go module: sdk/go  (package minidriver)
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

## 2. 上传：ObjectSource → 版本化 ObjectRef

```cpp
miniKV::client::ObjectSource source{"/work/input.bin"};
miniKV::client::PutObjectOptions options;
options.transfer.chunkWindow = 2;
options.transfer.checksumType = "crc32c";
options.contentType = "image/jpeg";

miniKV::client::ObjectRef object;
std::string error;
if (!client.putObject(source, options, object, error)) {
    throw std::runtime_error(error);
}
```

`ObjectSource` 当前是可重放的本地文件源；非 seekable stream 不能安全支持上传 body 重试，暂不伪装成已支持。
`uploadFile(path, fileName, dirPath, ...)` 仍为 Web/catalog 的兼容 helper，新计算代码不传远端目录、相册或业务文件名。

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

## 3. 完整对象读取：ObjectRef → ObjectSink

```cpp
std::string error;
miniKV::client::ReadOptions read;
read.keepAlive = true;
read.verifyChecksum = true;
read.allowReplicaRetry = true;
read.maxReplicaAttempts = 2;

miniKV::client::TransferStats stats;
if (!client.getObject(object, read,
        [&](const char* data, size_t size, std::string&) {
            decoder.feed(data, size); // 计算 Worker 直接消费对象字节
            return true;
        }, stats, error)) {
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
Chunk checksum，并在候选副本间作有界 fallback。`getObject()` 当前在完整 Chunk 校验后才调用 Sink，因此它是
“对象顺序流”，但不是无校验的边收边交付。`downloadToFile()` 是 FileSink 便利 helper，不是计算 API 核心。

## 4. 数据亲和性：为分布式计算调度提供什么

Go Scheduler 使用 Go SDK 的 `GetObjectReadHints()` 或 `BatchGetObjectReadHints()`，不消费 ReadPlan：

```cpp
// hints.Candidates: nodeId, localBytes, coverageRatio, health
hints, err := client.GetObjectReadHints(ctx, objectRef)
```

计算调度器应：

```text
持久化 ObjectRef
  → 任务调度前获取 ReadHints
  → 结合每个候选 Worker/nodeId 的本地字节数、覆盖率与空闲 slot
  → 优先把任务提交给本地数据最多的 Worker
  → Worker 仍调用 SDK 下载；不读取 extent
```

`nodeId` 是亲和性提示而不是资源预留。真正读取时，SDK 自己重新取得 ReadPlan，并处理 Capability、副本候选与
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

## 6. Object 控制与调度接口

除底层兼容 API 外，v3 正在收敛以下对象级 SDK 门面：

```text
putObject(path, ...)                     → ObjectRef
getObject(ObjectRef, ObjectSink, ...)    → 按对象顺序交付字节
getRange(ObjectRef, ..., ObjectSink, ...)→ RangeReadResult
headObject(ObjectRef)                    → ObjectInfo
deleteObject(ObjectRef)                  → 直接删除控制请求
getObjectReadHints(ObjectRef)            → 单对象亲和性
batchGetObjectReadHints([ObjectRef...])  → 一次 Gateway RPC 的批量亲和性
```

`ObjectReadHints.candidates[]` 返回 `nodeId`、`localBytes`、`coverageRatio` 和 Gateway 心跳快照中的
`health`。这是 Go Scheduler 的输入：它可结合自身 Worker 空闲 slot 决定任务位置，但不能指定存储副本位置。

批量接口对应：

```text
POST /internal/v3/objects/read-hints:batch
```

它不签发 DataNode read capability，也不暴露 extent；与读取实际对象的 ReadPlan 是不同权限和用途的接口。

### 直接删除

`deleteObject(ObjectRef)` 是 Control/GC API，不应由普通媒体 Worker 任意调用。它使用：

```text
DELETE /internal/v3/objects/{objectId}/versions/{objectVersion}/delete
→ 202 { "status": "deleting", ... }
```

`202 deleting` 表示对象已立即从目录和读取路径移除；Gateway 已持久化每个可回收 Chunk 的删除任务，并异步向所有
DataNode 副本派发物理删除。磁盘空间只在全部副本确认后释放。当前 content-link/CAS 兼容路径可能复用同一物理内容，
因此仍有其他 Object 引用时不会删除该物理数据；未来若明确部署 `opaque + no-dedup`，可将此 GC 策略单独简化。
请求必须带版本；版本不匹配不会删除对象。

## 7. 并发与生命周期

- 当前 `MiniDriverClient` 不是 thread-safe connection pool；一个 Worker 持有一个实例是当前实现选择，不是永久公共契约。
- Go `minidriver.Client` 使用标准库 `http.Client`，可由一个 Go 进程的多个 Goroutine 共享；每个上传对象仍由
  `PutOptions.ChunkWindow` 限制其内部 Chunk 并发。
- 多对象并发当前使用多个 Worker/SDK 实例；后续 ConnectionManager 会提供进程级 Gateway/DataNode 连接池、per-node
  inflight budget 和共享副本选择状态。当前单 DataNode 开发机的推荐起点是上传 c8、下载 c8。
- `UploadOptions::chunkWindow` 控制一个上传实例的内部 Chunk 并行度；它不是全局并发上限。
- 上传和下载协议允许同时进行，但尚未完成读写混合负载矩阵。因此 v3.0 没有发布“边上传边下载”的吞吐或 P95 承诺。
- 当前读写性能数据来自单机、loopback、warm cache；真实多主机容量必须在独立磁盘和真实 NIC 上复测。

## 8. 参考入口

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
