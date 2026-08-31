# CineLake AI Compose 运维设计（2026-08-21）

## 目标

将 AI 运行面从“手工启动 PostgreSQL、Redis、API、Stream Worker、Asset Worker 的多个终端”收口为一条命令：

```bash
docker compose -f ai-app-lite/compose.yaml up -d --build
```

这里的“一键”只覆盖 AI 子系统；MiniDrive V2 C++ Gateway/DataNode 继续沿用现有本机或 Node Agent 部署。这样
不会为了演示方便而重写已经独立运行的数据面，也保留后续将 MiniDrive 单独容器化的选择。

## 拓扑

```text
宿主机
  ├─ MiniDrive Gateway :8080 / DataNode（现有二进制或 Node Agent）
  │        │ 发布 FILE_UPLOAD_COMMITTED 到 127.0.0.1:${AI_REDIS_HOST_PORT}
  │        ▼
  └─ Docker Compose 网络
       ├─ redis :6379
       ├─ postgres :5432 (pgvector, volume)
       ├─ stream-worker ─ Redis Stream → PostgreSQL Job
       ├─ worker ─ PostgreSQL Job → host.docker.internal:8080 → PostgreSQL Asset/vector
       └─ api :18290 ─ PostgreSQL search/Dataset/Agent API
```

容器到宿主机 Gateway 使用 `host.docker.internal`，Linux 通过 Compose 的 `host-gateway` 映射实现；宿主机
Gateway 到容器 Redis 使用映射端口。容器内部不使用 `127.0.0.1` 访问 Redis/PostgreSQL，而是使用服务名
`redis`、`postgres`，避免“每个容器自己的 localhost”这一常见错误。

前提是宿主机 Gateway 监听 `0.0.0.0:8080` 或宿主机 bridge 地址，而不只是 `127.0.0.1:8080`；容器经
`host-gateway` 进入时不是 loopback 来源。若当前 Gateway 只绑定 loopback，应先调整其监听地址，或在开发
环境显式将 `AI_MINIDRIVE_GATEWAY_URL` 指向一个容器可达的主机地址。

## 服务职责与生命周期

| 服务 | 持久状态 | 重启行为 | 职责 |
|---|---|---|---|
| `postgres` | `cinedata_pgdata` volume | `unless-stopped` | pgvector、Asset、Job、Dataset、Agent audit 真源 |
| `redis` | `cinedata_redisdata` volume + AOF | `unless-stopped` | Gateway outbox 的 Stream；数据库账本保证消费幂等 |
| `api` | 无 | `unless-stopped` | `/ai/search`、Dataset、Agent、health API |
| `stream-worker` | 无 | `unless-stopped` | Redis Stream → PostgreSQL 去重 Job |
| `worker` | 无 | `unless-stopped` | 读取 MiniDrive 对象，生成 metadata/vector，完成 Job |

`api`、`stream-worker`、`worker` 都等待 PostgreSQL 与 Redis healthcheck 成功后再启动。Worker/ingestor 的故障
不会丢弃已提交对象：Redis 消息确认发生在 PostgreSQL 去重账本和 Job 成功写入之后；Job 状态也持久化在
PostgreSQL。

## 配置原则

将 `ai-app-lite/.env.example` 复制成未纳入 Git 的 `ai-app-lite/.env`：

```text
POSTGRES_PASSWORD          本机开发外必须更换
AI_REDIS_HOST_PORT         宿主机提供给 Gateway 的 Redis 端口，默认 6379
AI_REDIS_BIND_ADDRESS       Redis 暴露地址；多机时绑定 AI 主机的 Tailscale IP
AI_API_BIND_ADDRESS/PORT    AI API 暴露地址/端口；供 Gateway Web UI 所在的浏览器访问
AI_MINIDRIVE_GATEWAY_URL   容器读取对象时访问的 Gateway 地址
AI_CORS_ALLOW_ORIGIN       Web UI 来源
AI_EMBEDDING_PROVIDER      默认 hash；真实 OpenCLIP 不在此最小镜像内
```

必须保持下面两个端口视角一致：

```text
Gateway（宿主机） → Redis：127.0.0.1:${AI_REDIS_HOST_PORT}
AI 容器            → Redis：redis:6379
AI 容器            → PostgreSQL：postgres:5432
浏览器（宿主机）    → API：http://127.0.0.1:18290
AI 容器            → Gateway：http://host.docker.internal:8080
```

## 验收与故障定位

```bash
docker compose -f ai-app-lite/compose.yaml up -d --build
docker compose -f ai-app-lite/compose.yaml ps
curl http://127.0.0.1:18290/healthz
docker compose -f ai-app-lite/compose.yaml logs -f stream-worker worker
```

最小完成链路：启动 Gateway/DataNode → 上传 JPEG → Gateway 发布 Redis event → `stream-worker` 入 PostgreSQL
Job → `worker` 完成索引 → `/ai/search` 返回 Asset → Dataset/Agent API 记录版本化引用。

| 现象 | 首先检查 |
|---|---|
| API 不健康 | `docker compose ps`、`logs api`、PostgreSQL health |
| 有上传但没有索引 | Gateway 的 Redis 地址/端口是否与 `.env` 一致；`logs stream-worker` |
| Job failed | `logs worker`；Gateway 在容器内是否能以 `host.docker.internal:8080` 访问 |
| Gateway 连接 Redis 失败 | `AI_REDIS_HOST_PORT` 是否冲突或未同步到 Gateway 配置 |
| Compose 重启后结果还在 | PostgreSQL volume 正常；不要执行 `down -v` |

## 刻意未做的事情

- 不将 C++ Gateway/DataNode 与 AI 组件强绑定到同一个容器网络；
- 不把 OpenCLIP 权重、GPU 运行时塞进默认小镜像；
- 不对 Redis Stream 做“永不丢数据”的虚假承诺，可靠账本仍以 PostgreSQL 的 event/job 去重记录为准；
- 不将 `.env`、数据库 volume、对象 Body 或生产密码提交 Git。

后续若 MiniDrive V3 的 HA 拓扑稳定，可增加独立的 `compose.full.yaml`：它只负责演示全栈启动，不取代 Node
Agent、真实多机器部署或控制面故障测试。
