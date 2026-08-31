# MiniDrive AI-App-Lite

这是 MiniDrive 的独立 AI 最小版本。它不改变文件 Body 的 V2 数据面，但会订阅 Gateway 在 File Commit
后发布的 Redis Stream 事件，跑通一条可解释的自动索引闭环：

```text
MiniDrive File Commit
  → LevelDB Outbox
  → Redis Stream FILE_UPLOAD_COMMITTED
  → SQLite Job Queue
  → Asset Worker
  → EXIF/基础元数据
  → Embedding
  → 向量相似度 + 元数据过滤
  → 可选 LLM 文案生成
```

## 当前能力

- `POST /ai/assets`：登记一个本地对象并异步排队；
- `stream-worker`：消费 `FILE_UPLOAD_COMMITTED`，以 `objectId + objectVersion` 幂等创建索引任务；
- `GET /ai/jobs/{id}`：查询索引任务状态；
- `POST /ai/search`：文本向量检索，支持 metadata filter、稳定 keyset cursor 与跨目录分页；
- `POST /ai/datasets`、`POST /ai/datasets/{id}/members`：创建 Dataset，并将 Ready Asset 的当前对象版本写成成员快照；
- `POST /ai/agent/search`：只读检索 Agent，返回版本化证据并持久化 `agent_run` 审计；它不拥有对象或 Dataset 的写权限；
- `GET /ai/metrics`：查询 P50/P95、结果数、错误数和索引新鲜度的本地观测快照；
- `POST /ai/caption`：根据检索结果生成模板文案；配置 LLM 地址后切换到 OpenAI-compatible API；
- SQLite 元数据和向量存储，后续可以迁移为 PostgreSQL + pgvector；
- 默认 `hash-smoke` Embedding 不需要下载模型，只用于验证完整链路；
- 可选 OpenCLIP 图文向量，需要额外安装 PyTorch/OpenCLIP。
- Phase C Lite：有界批量索引、`embedding_model` 隔离、OpenCLIP zero-shot 标签，以及 Pillow 图像质量/pHash 元数据；
  RAW、视频和 OCR 必须等待独立派生 Worker，当前不会伪装成已支持。

## 启动

### 推荐：一条 Compose 命令启动 AI 运行面

`compose.yaml` 会一起启动 `PostgreSQL + pgvector`、Redis、AI API、Redis Stream Worker 与 Asset Worker；
日常不再需要分别打开三个 Python 终端。

```bash
cd ~/miniKV_v2/miniDriver
cp ai-app-lite/.env.example ai-app-lite/.env
# 按需编辑 .env：至少修改 POSTGRES_PASSWORD；若宿主机 Redis 已占 6379，修改 AI_REDIS_HOST_PORT。
docker compose -f ai-app-lite/compose.yaml up -d --build
docker compose -f ai-app-lite/compose.yaml ps
curl http://127.0.0.1:18290/healthz
```

MiniDrive V2 数据面暂不放进此 Compose：仍按现有二进制/Node Agent 方式在宿主机启动。Compose 已把
`AI_MINIDRIVE_GATEWAY_URL` 默认指向 `http://host.docker.internal:8080`；Linux 上通过 Docker
`host-gateway` 映射生效。Gateway 需要连接 Compose 的 Redis 时，使用宿主机地址 `127.0.0.1` 与
`AI_REDIS_HOST_PORT`（默认 `6379`）。注意 Gateway 必须监听 `0.0.0.0:8080`（或其他容器可达地址），
不能只监听 `127.0.0.1`，否则 Worker 无法通过 host gateway 读取对象。

常用运维命令：

```bash
docker compose -f ai-app-lite/compose.yaml logs -f api stream-worker worker
docker compose -f ai-app-lite/compose.yaml restart worker
docker compose -f ai-app-lite/compose.yaml down       # 保留 PostgreSQL volume
docker compose -f ai-app-lite/compose.yaml down -v    # 删除数据库；仅在确认数据可丢弃时使用
```

默认容器镜像采用 `hash-smoke` embedding，保持小且可重复；OpenCLIP/CPU-GPU 依赖尚未放入默认镜像，避免一次
`docker compose up` 就下载大模型并把硬件选择写死。切换真实语义模型前应先构建独立的 semantic worker 镜像。

### 手动：分别启动各组件

在仓库根目录执行：

```bash
export PYTHONPATH="$PWD/ai-app-lite"
export AI_APP_DB="$PWD/ai-app-lite/data/ai.sqlite3"

# 终端 1：API
python3 -m cinedata_ai.main api --port 18290

# 终端 2：索引事件消费者（把 Redis Stream 持久化为 SQLite Job）
export AI_REDIS_ADDRESS=127.0.0.1
export AI_REDIS_PORT=6379
export AI_MINIDRIVE_GATEWAY_URL="http://127.0.0.1:8080"
python3 -m cinedata_ai.main stream-worker

# 终端 3：索引 Worker（读取已提交对象、生成元数据与向量）
python3 -m cinedata_ai.main worker
```

登记一个素材：

```bash
curl -X POST http://127.0.0.1:18290/ai/assets \
  -H 'Content-Type: application/json' \
  -d '{
    "asset_id": "photo-001",
    "object_key": "photos/temple-sunset.jpg",
    "local_path": "/data/photos/temple-sunset.jpg",
    "metadata": {"scene": "temple sunset", "camera_model": "Nikon D610"}
  }'
```

查询任务完成后进行检索：

```bash
curl -X POST http://127.0.0.1:18290/ai/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"sunset temple", "page_size":9, "filters":{"camera_model":"Nikon", "quality_min":0.6}}'
```

生成文案：

```bash
curl -X POST http://127.0.0.1:18290/ai/caption \
  -H 'Content-Type: application/json' \
  -d '{"asset_ids":["photo-001"], "instruction":"写一段朋友圈文案"}'
```

## 切换真实图文 Embedding

默认 provider 是 `hash`，它是确定性的 smoke provider，不代表语义模型效果。安装可选依赖后：

```bash
pip install -r ai-app-lite/requirements.txt
pip install torch open_clip_torch
export AI_EMBEDDING_PROVIDER=openclip
export AI_OPENCLIP_MODEL=ViT-B-32-quickgelu
export AI_OPENCLIP_PRETRAINED=openai
export AI_WORKER_BATCH_SIZE=4
export AI_SEMANTIC_TAG_TOP_K=3
export AI_SEMANTIC_TAG_MIN_SCORE=0.20
```

真实 OpenCLIP 会把图片和文本映射到同一向量空间；SQLite 仍只用于 MVP，数据规模增大后再切换
PostgreSQL + pgvector。

OpenCLIP 索引只接受当前可解码的 raster 图片（JPEG/PNG/WebP/TIFF）。每个 Asset 会记录模型身份，例如
`openclip:ViT-B-32-quickgelu:openai`；Query API 只检索相同模型产生的向量，避免模型升级后混算余弦相似度。Pillow 写入的
亮度、对比度、锐度、曝光和 `perceptual_hash` 是可解释启发式指标，不是审美模型或自动删除规则。

## 固定检索评测

准备已索引对象的固定 case 文件后可运行：

```bash
python3 -m cinedata_ai.main evaluate --cases ./cases.json --top-k 5
```

每项 case 需要 `query` 与 `expected_asset_ids`；命令输出每条命中、`recall_at_k`、`mrr_at_k` 和当前
`embedding_model`。只有在保存了素材集、模型/权重、batch size 和原始 JSON 后，才应把结果写入性能/质量报告。

## Phase D 查询分页与基准

`POST /ai/search` 的 `page_size` 范围为 1--50；首个响应中的 `page.next_cursor` 可原样传入同一个
`query + filters` 请求取得下一页。Cursor 与 query vector、filter 和 model 绑定，不能混用。排序固定为
`score DESC, asset_id ASC`；结果只返回对象路由、元数据和分数，不返回 Worker 本地路径或 embedding。

可查看本进程观测：

```bash
curl http://127.0.0.1:18290/ai/metrics
```

对本地索引进行并发 query-executor 基准（不重复计算文字向量，也不包含 HTTP）:

```bash
python3 -m cinedata_ai.main benchmark-query \
  --query 鸟类 --requests 100 --concurrency 4 --page-size 5
```

SQLite 只适合单机 Lite：它保存事务性元数据，AI API 进程为 ready Assets 建立只读快照以减少重复 JSON
解码。多进程部署、更大数据规模或更严格 P95/Recall 要求出现后，应迁移到 PostgreSQL + pgvector；详见
[`Phase D` 架构与测试报告](../docs/CINELAKE_PHASE_D_QUERY_SERVICE_ARCHITECTURE_AND_TEST_REPORT_2026-08-21.md)。

## 正式 Phase D：PostgreSQL + pgvector

启动项目内的数据库（只绑定本机 `127.0.0.1:54329`）：

```bash
docker compose -f ai-app-lite/compose.yaml up -d
ai-app-lite/.venv/bin/pip install -r ai-app-lite/requirements-postgres.txt

export AI_STORAGE_BACKEND=postgres
export AI_POSTGRES_DSN='postgresql://cinedata:cinedata@127.0.0.1:54329/cinedata'
```

首次创建 Repository 时会执行 schema migration：启用 `vector` 扩展，创建 `assets`（JSONB metadata + vector
embedding + model/version）、`jobs`、event 去重账本、Dataset 版本快照和 Agent 审计表。PostgreSQL 使用事务和
`FOR UPDATE SKIP LOCKED` 领取 Worker Job；向量检索在 SQL 中执行 `<=>` cosine distance，分页 cursor 以
`index_generation` 固定快照，保证新索引对象不会插入旧分页序列。

验证真实后端：

```bash
export AI_TEST_POSTGRES_DSN="$AI_POSTGRES_DSN"
PYTHONPATH=ai-app-lite ai-app-lite/.venv/bin/python -m unittest \
  ai-app-lite/tests/test_postgres_store_integration.py -v
```

Phase D 的 PostgreSQL 路径已经是正式多进程 metadata/vector 真源；当前未默认创建 HNSW/IVFFlat 索引，
因为 hash(256 维) 与 OpenCLIP(512 维) 模型不能安全混用同一 ANN 索引。固定生产模型与数据量后，应将
embedding 拆为模型维度固定的表，再以 Recall@K/P95 选择 HNSW 或 IVFFlat 参数。

## E0/F0：Dataset 快照与只读检索 Agent

Dataset 不是目录，而是“某次训练/评测选择了哪些对象版本”的不可变成员集合。选入成员时服务记录 Asset 的
`metadata.object_version`（缺失时兼容为 `1`）；对象后续重新索引为新版本不会改变已有 Dataset。

```bash
curl -X POST http://127.0.0.1:18290/ai/datasets \
  -H 'Content-Type: application/json' \
  -d '{"name":"sunset-eval","owner":"hpy","purpose":"检索评测"}'

curl -X POST http://127.0.0.1:18290/ai/datasets/<dataset_id>/members \
  -H 'Content-Type: application/json' \
  -d '{"asset_ids":["photo-001"],"role":"evaluation"}'

curl -X POST http://127.0.0.1:18290/ai/agent/search \
  -H 'Content-Type: application/json' \
  -d '{"caller":"hpy","query":"日落","page_size":5}'
```

Agent 响应的 `run_id` 可由 `GET /ai/agent/runs/<run_id>` 查询，返回当时的 `(asset_id, object_version, rank, score)`
证据。当前 `owner/caller` 只是审计字段，尚未实现登录、ACL 或 Agent 自动创建 Dataset；这些边界是故意保留的。
完整设计和测试证据见
[`Phase E0/F0 报告`](../docs/CINELAKE_PHASE_E_F_MINIMUM_GOVERNED_AGENT_IMPLEMENTATION_2026-08-21.md)。

## 摄影工作台搜索入口

`www-v2` 已提供“AI FIND / 02.1”搜索框和快捷标签。默认会访问与网页相同主机的 `18290` 端口；启动
上面的 API 与 Worker 后，输入描述或点击标签即可调用 `POST /ai/search`。搜索结果的 `asset_id` 应使用
MiniDrive 的 `objectId`，这样“在目录中预览”可以直接复用现有预览器。

开发环境默认允许跨域访问；若部署到非本机环境，请设置受信任来源，例如：

```bash
export AI_CORS_ALLOW_ORIGIN="http://127.0.0.1:8080"
```

## 与 MiniDrive 的自动连接

Phase B Lite 已实现下面的事件边界：

```text
Gateway File Commit
  → 同一 LevelDB batch: ObjectRecord + ai:<eventId> outbox
  → Gateway Publisher XADD minidrive:file-events
  → stream-worker: consumer group 持久化去重 ledger + SQLite Job
  → Worker: manifest → DataNode Chunk（逐 Chunk SHA-256 校验）→ AssetStore
```

Gateway 的 Redis 配置沿用 `MINIKV_V2_REDIS_ADDRESS` / `MINIKV_V2_REDIS_PORT`，AI Stream 可通过
`MINIKV_V2_REDIS_AI_STREAM` 覆盖；AI 端使用同名的 `AI_REDIS_FILE_EVENT_STREAM`。

消费者只会在 SQLite 的 `index_events` 去重账本和 `jobs` 事务成功后才 `XACK`。重复消息按 `eventId` 去重；
即使发布端为同一 `objectId + objectVersion` 产生了不同 `eventId`，也会复用原 Job。Redis 7 使用
`XAUTOCLAIM` 回收超时 pending 消息；Redis 5/6 自动回退为 `XPENDING + XCLAIM`。

### 当前边界

- 仍保留 `POST /ai/assets`，便于脱离 MiniDrive 的本地调试；
- `hash-smoke` 只证明链路，不提供真实图文语义；
- V2 事件的 `objectVersion/metadataVersion` 目前固定为 `1`，真实版本演进属于 V3 Metadata 工作；
- 对象读取使用当前可信内网的 manifest/DataNode HTTP 路径；生产环境应补 Worker 身份认证、短期读令牌和
  更严格的下载配额；
- SQLite 适合单机演示，失败 Job 的人工/定时重试策略、DLQ 与向量数据库仍是下一阶段。
