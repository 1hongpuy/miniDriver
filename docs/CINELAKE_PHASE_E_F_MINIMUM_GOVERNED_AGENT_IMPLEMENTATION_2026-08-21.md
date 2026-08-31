# CineLake Phase E0/F0：Dataset 治理与只读检索 Agent 最小实现（2026-08-21）

## 结论

本轮完成的是一个可运行、可验证的最小数据治理与 Agent 闭环，而不是提前宣称“企业级湖仓”或“全自动
训练 Agent”：

- **E0**：Dataset 保存素材的版本快照。成员使用 `(dataset_id, asset_id, object_version)` 标识；重建或更新
  同一 Asset 后，历史 Dataset 不会静默指向新版本。
- **F0**：只读检索 Agent 复用已有语义搜索，返回有版本的证据，并记录每次调用的 caller、query、filter、
  embedding model 与排序结果。
- 两个能力同时支持 SQLite Lite 和 PostgreSQL + pgvector 后端；PostgreSQL 集成测试已经在本机 Compose
  服务上通过。

F0 Agent 不拥有删除对象、更新 Asset、创建 Dataset、导出训练集或调用 MiniDrive 写接口的工具。它唯一产生的
写入是审计记录；这正是先建立安全边界、再逐步增加受控能力的原因。

## 1. 解决的问题

Phase C/D 已能将图片索引为 `Asset` 并返回相似结果，但“某个搜索结果”不足以回答：

```text
某次训练/标注实际使用了哪些素材？
它们当时是哪个对象版本？
一个检索或 Agent 答案依据了哪些对象？
后来重新索引同一 asset，会不会改变历史结论？
```

E0/F0 将上述最小事实固化为两类记录：Dataset 成员快照与 Agent Run 证据。

## 2. 架构与边界

```text
网页 / API Client
       │
       ├─ POST /ai/datasets ────────────────┐
       ├─ POST /ai/datasets/{id}/members ───┤
       └─ POST /ai/agent/search ────────────┤
                                               ▼
                                      AI Query Service
                                    ┌──────────┴──────────┐
                                    │ Dataset E0          │ Agent F0
                                    │ snapshot Asset vN   │ read-only search
                                    └──────────┬──────────┘
                                               ▼
                    SQLite Lite / PostgreSQL + pgvector authoritative metadata
                  datasets, dataset_members, agent_runs, agent_run_results, assets
                                               │
                                               ▼
                               MiniDrive object route (只读引用，不传 Body)
```

不变的职责边界：

- MiniDrive 仍负责对象 Body、Chunk、复制与对象版本的最终来源；
- AI 数据库保存逻辑对象标识、派生 metadata/vector、Dataset 快照和审计，不复制图片 Body；
- Agent 调用 `POST /ai/search` 同样的向量/metadata filter 路径，不绕过索引服务；
- E0 的 `owner`/F0 的 `caller` 目前只是审计字段；没有认证、租户隔离或 ACL，不能作为安全授权依据。

## 3. 数据模型与语义

```text
datasets
  dataset_id (PK) / name / owner / purpose / created_at

dataset_members
  dataset_id / asset_id / object_version / role / added_at
  PK(dataset_id, asset_id, object_version)

agent_runs
  run_id (PK) / caller / query / filters_json / embedding_model / created_at

agent_run_results
  run_id / asset_id / object_version / rank / score
  PK(run_id, rank)
```

### Dataset 版本快照

选入成员时服务读取 Ready Asset 的 `metadata.object_version`（当前无该字段时兼容默认值 `1`），并写入
`dataset_members`。同一个请求再次选择同一 `(asset, version)` 是幂等的；如果对象后来重建成 v2，旧 v1 成员
保留不变，用户必须显式加入 v2。

这让 Dataset 成为“可复现选择集”，而不是一个自动跟随当前 Asset 行变化的目录。

### Agent 证据

`POST /ai/agent/search` 先执行正常搜索，再将脱敏搜索结果中的 `(asset_id, object_version, rank, score)` 写入
`agent_run_results`。返回的 `run_id` 可通过 `GET /ai/agent/runs/{run_id}` 查询。因此即使后续索引更新，仍可
知道本次回答实际引用了哪个版本。

## 4. HTTP API

```text
POST /ai/datasets
{ "name": "sunset-train", "owner": "alice", "purpose": "CLIP 检索评估" }

POST /ai/datasets/{dataset_id}/members
{ "asset_ids": ["photo-001", "photo-002"], "role": "evaluation" }

GET /ai/datasets
GET /ai/datasets?owner=alice
GET /ai/datasets/{dataset_id}

POST /ai/agent/search
{ "caller": "alice", "query": "日落", "filters": {"quality_min": 0.6}, "page_size": 5 }

GET /ai/agent/runs/{run_id}
```

`/ai/agent/search` 的返回包含普通 `search` 内容、模板化说明、`run_id` 与持久化 evidence。它不接受任何写对象
参数，也不会代替用户创建 Dataset。

## 5. 验证结果

新增自动化测试覆盖：

1. 选入 Asset v1 后重新索引为 v2，Dataset 仍保存 v1；显式再次选入才出现 v2；
2. Agent 检索返回有版本的 evidence，且审计可按 `run_id` 读取；
3. Agent 调用不改变 Asset/index 内容；
4. PostgreSQL + pgvector 后端同样验证 Dataset snapshot 与 Agent audit。

本轮验证命令与结果：

```bash
PYTHONPATH=ai-app-lite ai-app-lite/.venv/bin/python -m unittest discover -s ai-app-lite/tests -v
# Ran 13 tests ... OK (skipped=3)
# 跳过：未设置 PostgreSQL DSN 的两个真实 DB 测试、未启动 Redis 的既有 Stream 测试

PYTHONPATH=ai-app-lite \
  AI_TEST_POSTGRES_DSN='postgresql://cinedata:cinedata@127.0.0.1:54329/cinedata' \
  ai-app-lite/.venv/bin/python -m unittest ai-app-lite/tests/test_postgres_store_integration.py -v
# Ran 2 tests ... OK
```

## 6. 手工验收步骤

先启动已配置的 PostgreSQL（或省略环境变量使用 SQLite Lite），再启动 API：

```bash
cd ~/miniKV_v2/miniDriver
docker compose -f ai-app-lite/compose.yaml up -d
export PYTHONPATH="$PWD/ai-app-lite"
export AI_STORAGE_BACKEND=postgres
export AI_POSTGRES_DSN='postgresql://cinedata:cinedata@127.0.0.1:54329/cinedata'
ai-app-lite/.venv/bin/python -m cinedata_ai.main api --port 18290
```

在另一个终端，对已经由 Worker 索引且状态为 Ready 的 `asset_id` 操作：

```bash
curl -X POST http://127.0.0.1:18290/ai/datasets \
  -H 'Content-Type: application/json' \
  -d '{"name":"manual-check","owner":"hpy","purpose":"E0/F0 验收"}'

# 将上一步的 dataset_id 替换到 URL；photo-001 替换为真实 asset_id。
curl -X POST http://127.0.0.1:18290/ai/datasets/<dataset_id>/members \
  -H 'Content-Type: application/json' \
  -d '{"asset_ids":["photo-001"],"role":"evaluation"}'

curl -X POST http://127.0.0.1:18290/ai/agent/search \
  -H 'Content-Type: application/json' \
  -d '{"caller":"hpy","query":"日落","page_size":5}'
```

检查 Agent 响应中的 `run_id`，再请求 `GET /ai/agent/runs/<run_id>`；应能看到当时的 asset/version/rank/score。

## 7. 未完成项与下一步

E0/F0 有意保持最小，下面能力仍未实现：

- 用户认证、ACL、Dataset scope 到 SQL filter/cursor 的强制注入；
- MiniDrive V3 Metadata 的真实对象版本、删除 tombstone 与 event 同步；
- quality/duplicate 人审、完整 Processor lineage、Dataset 导出 manifest；
- Agent 经审批创建 Dataset、调用 LLM、执行 RAG 答案合成；
- Iceberg/Spark/Flink 批流治理与大规模资产分析。

下一步应先由你运行完整上传 → Redis event → Worker → PostgreSQL index → 搜索 → Dataset/Agent 的手工链路；
确认结果和运行环境都稳定后，再决定做权限/质量治理，还是继续 MiniDrive V3 控制面工作。
