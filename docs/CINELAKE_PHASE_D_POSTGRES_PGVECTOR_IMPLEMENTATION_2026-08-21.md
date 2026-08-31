# CineLake Phase D：PostgreSQL + pgvector 正式检索后端实施说明（2026-08-21）

## 状态

代码、Compose、schema migration 和集成测试已经加入；SQLite 仍保留为无外部依赖的本地 fallback。

真实 Docker Compose 服务已启动并通过 healthcheck；使用 `pgvector/pgvector:pg16`、本机
`127.0.0.1:54329` 运行 PostgreSQL integration test 已通过。完整 Python 回归为 **9 通过、1 跳过**；唯一
跳过项是未启动独立 Redis 的既有 Stream 集成测试。

## 1. 为什么 Phase D 必须迁移 PostgreSQL

Phase D Lite 已定义搜索 API、cursor、filter、观测和前端分页，但 SQLite 的单进程快照缓存不能解决：

- API/Worker 多进程之间的索引可见性；
- 多 Worker 并发领取同一 Job；
- JSONB 结构化 metadata filter 与向量查询在数据库内组合；
- 向量规模增长后的 pgvector HNSW/IVFFlat 迁移路径。

因此正式 Phase D 的权威状态是 PostgreSQL，不是 SQLite 缓存。

```text
Gateway Commit → Redis Stream → Stream Worker
                                     │
                                     ▼
                         PostgreSQL jobs/index_events/index_targets
                                     │  FOR UPDATE SKIP LOCKED
                                     ▼
                                  AI Worker
                                     │ upsert
                                     ▼
                         PostgreSQL assets + pgvector embedding
                                     ▲
www-v2 → AI API → metadata filter + SQL cosine retrieval + cursor
```

## 2. 新增组件

| 文件 | 职责 |
|---|---|
| `ai-app-lite/compose.yaml` | 本机 `127.0.0.1:54329` 的 pgvector PostgreSQL 16 服务与命名 volume |
| `requirements-postgres.txt` | `psycopg[binary]` Python 驱动 |
| `cinedata_ai/postgres_store.py` | PostgreSQL Repository、schema migration、pgvector query、Job 事务 |
| `cinedata_ai/repository.py` | `AI_STORAGE_BACKEND=sqlite|postgres` 工厂 |
| `test_postgres_store_integration.py` | 真 PostgreSQL 的分页/filter/Job 领取测试 |

配置：

```text
AI_STORAGE_BACKEND=postgres
AI_POSTGRES_DSN=postgresql://cinedata:<password>@127.0.0.1:54329/cinedata
```

## 3. 数据模型与一致性

```text
assets
  asset_id (PK) / object_key / metadata_json JSONB
  embedding vector / embedding_model / status
  updated_at / index_generation

jobs
  job_id (PK) / payload_json JSONB / status / attempts / last_error

index_events
  event_id (PK) → 防止同一 Redis Stream 事件重复入队

index_targets
  (object_id, object_version) (PK) → 防止不同 eventId 重复索引同一对象版本

datasets / dataset_members
  Dataset 定义与 (asset_id, object_version) 成员快照；成员不会随 Asset 重建静默改变

agent_runs / agent_run_results
  只读检索 Agent 的 caller/query/filter/model 及按 rank 保存的版本化证据
```

`upsert_asset` 在每次索引成功时分配新的 `index_generation`。首个搜索页读取当前 generation 上限；下一页带回
该上限，因此后续刚写入/更新的对象不会插入旧 cursor 的分页序列。cursor 同时绑定 query vector、filter 和
embedding model；不能被另一 query 复用。

Worker 领取采用：

```sql
SELECT ... FROM jobs
WHERE status = 'pending'
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT $N;
```

多个 Worker 会跳过已被其他事务锁定的 Job，而非等待或重复处理。

## 4. 查询执行

```text
POST /ai/search
  → Provider.embed_text(query)
  → SQL WHERE status/model/filter/index_generation
  → embedding <=> query_vector   -- cosine distance
  → ORDER BY distance ASC, asset_id ASC
  → keyset cursor
  → 脱敏 object route / score / metadata
```

当前 `assets.embedding` 允许不同维度模型，以支持 hash smoke (256) 和 OpenCLIP (512) 共存，并始终由
`embedding_model` 隔离。正因如此，当前只使用 pgvector exact scan；不要为不同维度混合数据仓促创建单一
HNSW/IVFFlat index。固定模型后应拆出维度固定的 `embeddings` 表，分别按模型建立 ANN index，并以
Recall@K、P95、索引构建时间和磁盘大小选择 HNSW 或 IVFFlat。

## 5. 安全与部署边界

- Compose 端口只绑定 `127.0.0.1`；默认密码仅用于本地开发，外部部署必须设置强 `POSTGRES_PASSWORD`；
- DSN、密码、Docker volume 和数据库运行数据不进入 Git；
- 数据库只保存 metadata/vector/Job，不保存 MiniDrive 原始图片或 Chunk Body；
- SQL filter 的 key/value 始终使用参数绑定，不能由请求拼接 SQL；
- API 的权限/Dataset scope 尚未实现，加入后必须同时进入 SQL filter 与 cursor fingerprint。

## 6. 启动与验收

在已经运行 `newgrp docker` 或重新登录的终端：

```bash
cd ~/miniKV_v2/miniDriver
docker compose -f ai-app-lite/compose.yaml up -d
docker compose -f ai-app-lite/compose.yaml ps

export AI_STORAGE_BACKEND=postgres
export AI_POSTGRES_DSN='postgresql://cinedata:cinedata@127.0.0.1:54329/cinedata'
export AI_TEST_POSTGRES_DSN="$AI_POSTGRES_DSN"
PYTHONPATH=ai-app-lite ai-app-lite/.venv/bin/python -m unittest \
  ai-app-lite/tests/test_postgres_store_integration.py -v
```

验收要求：

1. Compose health 为 `healthy`；
2. migration 成功创建 `vector` extension 和四张业务表；
3. PostgreSQL integration test 通过：vector Top-K、cursor、JSONB filter、`SKIP LOCKED` Job claim；
4. 以 `AI_STORAGE_BACKEND=postgres` 分别启动 `api`、`stream-worker`、`worker`，上传一张 JPEG 后搜索可见；
5. 再运行 Phase C 固定检索集，记录 PostgreSQL exact scan P50/P95 后才评估 ANN。

## 7. 当前未完成项

- PostgreSQL 生产凭据、TLS、备份与监控；
- 模型维度固定后的 HNSW/IVFFlat index 与 Recall/P95 对照；
- Dataset/权限/对象 version 全量接入 V3 Metadata（当前只有 AI 侧最小 Dataset snapshot）；
