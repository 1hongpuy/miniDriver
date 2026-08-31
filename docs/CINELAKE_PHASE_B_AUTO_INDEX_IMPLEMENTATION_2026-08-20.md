# CineLake Phase B Lite：文件 Commit 驱动的自动索引实现（2026-08-20）

## 结论

Phase B Lite 已把原来“上传成功后手工调用 `POST /ai/assets`”的断点替换为自动链路：

```text
Browser upload
  → Gateway File Commit
  → LevelDB Outbox
  → Redis Stream FILE_UPLOAD_COMMITTED
  → AI stream-worker（consumer group）
  → SQLite Index Job
  → AI Worker 通过 manifest 读取对象并索引
  → /ai/search 与 www-v2 展示结果
```

文件 Body 不进入 Redis，也不经过 AI API。Redis 只传对象身份和小型元数据；Worker 根据 `objectId` 读取
已提交对象的 manifest，再从 DataNode 副本拉取并验证 Chunk。这样不会改变 MiniDrive 的客户端直传、链式副本
和有界背压数据路径。

这是单 Gateway、可信内网、SQLite 演示范围内的**至少一次事件发布 + 幂等消费**实现，不应宣称为生产级
湖仓或生产级事件平台。

## 1. 为什么需要 Outbox

仅在 HTTP File Commit Handler 中直接 `XADD` 有两个不正确的窗口：

```text
先 Commit、后 XADD：进程崩溃 → 对象存在但没有索引事件
先 XADD、后 Commit：Commit 失败 → Worker 索引不存在的对象
```

因此 Gateway 在同一个 LevelDB `WriteBatch` 中写入对象元数据、目录映射和 `ai:<eventId>`：

```text
ObjectRecord(COMMITTED) + path mapping + AiIndexEvent(PUBLISHED=0)
```

写成功后才向客户端返回 File Commit 成功。异步 Publisher 在启动、定时扫描和每次 Commit 后扫描未发布事件：

```text
PENDING outbox
  → Redis XADD
  → 成功：publishedAt = now
  → 失败：保留 PENDING，稍后重试
```

若 `XADD` 已成功但 Gateway 在写 `publishedAt` 前崩溃，重启会重复发布。因此发布语义是**至少一次**，不是
“恰好一次”；后面的消费端必须用业务幂等键消除重复效果。

## 2. 事件契约

Redis Stream 默认名为 `minidrive:file-events`，消息字段如下：

```text
eventId          V2 使用 objectId；同一个逻辑目录对象稳定唯一
eventType        FILE_UPLOAD_COMMITTED
objectId         AI asset_id，与前端预览的对象身份一致
objectKey        例如 /travel/temple-sunset.jpg，仅用于展示
fileHash         MiniDrive manifest identity，不假设等于整文件 SHA-256(bytes)
fileSize
objectVersion    V2 固定 1，V3 Metadata 负责真实演进
metadataVersion  V2 固定 1，V3 Metadata 负责真实演进
occurredAt
```

事件不含 `local_path`、DataNode 磁盘路径、Chunk Body 或用户密钥。对象的字节与副本路由只从已 Commit 的
manifest 取得。

## 3. AI 消费与幂等边界

`stream-worker` 创建/加入 Redis consumer group（默认 `cinedata-indexers`）。处理顺序为：

```text
Redis entry
  → SQLite transaction
      index_events(event_id PRIMARY KEY)
      index_targets(object_id, object_version PRIMARY KEY)
      jobs(status=pending)
  → XACK
  → AssetWorker claim Job
  → materialize + EXIF/Embedding + assets(upsert ready)
  → jobs(done | failed)
```

这有两层幂等：

- 同一 `eventId` 重复投递：直接返回原 Job；
- 因 Gateway 重试而生成不同 `eventId`，但 `objectId + objectVersion` 相同：复用原 Job；失败 Job 收到重复
  事件时会安全地回到 `pending`。

ACK 的边界是“SQLite Job 已持久化”，而不是“模型推理已经结束”。这样长时间的模型处理不会占住 Redis
pending entry；Worker 失败仍可在 SQLite 中追踪为 `failed`。Consumer 自身在 SQLite 事务失败时不会 ACK，
消息会被保留并被其他 Consumer 回收。

Redis 7 使用 `XAUTOCLAIM`；为兼容本地 Redis 6，检测到命令不支持后自动改用 `XPENDING + XCLAIM`。两种方式
都只回收超过 `AI_REDIS_RECLAIM_IDLE_MS` 的 pending 消息。

## 4. 已提交对象如何安全读取

`MiniDriveObjectReader` 不读取 DataNode 磁盘文件：

```text
GET Gateway /api/v2/objects/{objectId}/manifest
  → 对每个 Chunk 按 manifest 副本候选尝试
  → GET DataNode /v2/chunks/{chunkHash}
  → 校验精确长度 + SHA-256(chunk bytes)
  → 临时文件交给 EXIF/Embedding
  → context 退出删除临时文件
```

读取器限制 `AI_MAX_OBJECT_BYTES` 和 HTTP 超时。它校验各 Chunk SHA-256；事件中的 `fileHash` 是 V2 manifest
身份，因此不会错误地把它当作整文件 byte hash。索引记录保存 `minidrive://{objectId}`，不保存会被清理的临时
文件路径。

当前 V2 读取路径默认属于可信内网。生产化前必须增加 Worker service credential、短期读取令牌、DataNode
下载配额与审计；这些属于 V3 Metadata/鉴权演进，不在本次 Lite 范围内。

## 5. 运行方式

Gateway 端 Redis 配置：

```bash
export MINIKV_V2_REDIS_ADDRESS=127.0.0.1
export MINIKV_V2_REDIS_PORT=6379
export MINIKV_V2_REDIS_AI_STREAM=minidrive:file-events
```

AI 端启动三个角色：

```bash
export PYTHONPATH="$PWD/ai-app-lite"
export AI_APP_DB="$PWD/ai-app-lite/data/ai.sqlite3"
export AI_REDIS_FILE_EVENT_STREAM=minidrive:file-events
export AI_MINIDRIVE_GATEWAY_URL=http://127.0.0.1:<gateway-port>

# API：供 www-v2 查询
python3 -m cinedata_ai.main api --port 18290

# Redis entry → SQLite Index Job
python3 -m cinedata_ai.main stream-worker

# SQLite Job → metadata/vector/ready Asset
python3 -m cinedata_ai.main worker
```

默认 `hash` provider 只验证自动化链路。要验证真实图文搜索，需另行安装模型依赖并设置
`AI_EMBEDDING_PROVIDER=openclip`，同时用固定图像/文本集评价结果。

## 6. 验证证据

本次新增的自动测试覆盖：

| 测试 | 证明 |
|---|---|
| `gateway_ai_index_outbox` | File Commit 与 outbox 同步持久化；重启后 published 事件不再待发 |
| `v2_ai_file_event_redis` | 隔离 Redis 中 C++ Publisher 真正 `XADD`，Python consumer 真正消费、SQLite 入队并 ACK |
| `test_stream_event.py` | eventId 与 object/version 两层幂等；Worker 使用 MiniDrive reader 并保存稳定逻辑地址 |
| 既有 Python pipeline tests | 本地登记、Worker、向量检索回归 |

建议执行：

```bash
cmake --build build -j2
ctest --test-dir build --output-on-failure -R 'gateway_ai_index_outbox|v2_ai_file_event_redis'
PYTHONPATH=ai-app-lite python3 -m unittest discover -s ai-app-lite/tests -v
```

`v2_ai_file_event_redis` 会在测试中启动临时 loopback Redis，需要环境允许本地 socket。

## 7. 明确未完成项

- V3 Metadata StateMachine/Raft 下的分布式 Outbox、真实 `objectVersion` 和 fencing；
- Worker/API 身份认证、短期读令牌、权限过滤；
- Job lease、指数退避、自动重试、DLQ 和可视化运维；
- OpenCLIP/GPU 批处理、OCR、RAW/视频派生任务；
- PostgreSQL/pgvector 或 Milvus/Faiss 的规模化检索与查询 SLA；
- 真正的浏览器上传 JPEG 端到端环境报告（本次先以 outbox、Redis 和 Worker bridge 自动化联调证明）。

这些不应与“自动索引事件闭环已实现”混淆：Phase B 解决的是可靠地把已提交对象交给异步索引系统，而不是
一次完成多模态数据平台的全部能力。
