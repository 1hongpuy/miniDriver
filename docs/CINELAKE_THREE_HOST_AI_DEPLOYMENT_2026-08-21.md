# CineLake 三主机 AI 索引与检索部署配置（2026-08-21）

## 1. 已知拓扑

本配置基于当前 Tailscale 地址；以下地址只用于 Tailnet 内部通信，不应在公网暴露 Redis、PostgreSQL 或 AI API。

| 主机 | Tailscale IP | 职责 |
|---|---|---|
| `gateway` | `100.75.93.124` | MiniDrive Gateway、Web UI、File Commit 事件发布者 |
| `node-d` | `100.75.72.15` | MiniDrive DataNode；如启用缩略图 Worker，也会消费 Redis |
| `ubuntu22data1` | `100.89.50.125` | Redis、PostgreSQL/pgvector、AI API、Stream Worker、Asset Worker |

```text
Gateway 100.75.93.124:18081
   │  XADD FILE_UPLOAD_COMMITTED
   ▼
Redis 100.89.50.125:6379
   │
   ▼
AI stream-worker / worker / PostgreSQL 100.89.50.125
   │  获取 manifest / 读取已提交对象
   ▼
Gateway 100.75.93.124:18081 → DataNode

浏览器 → Gateway Web UI :18081
浏览器 → AI API 100.89.50.125:18290
```

本文以仓库 V2 示例常用的 Gateway 端口 `18081` 编写。**如果你的实际 Gateway 端口不同，所有出现
`18081` 的地方必须一起替换**；可在 Gateway 主机执行 `ss -lntp` 确认。

## 2. 端口与访问方向

| 来源 | 目标 | 地址 | 用途 |
|---|---|---|---|
| Gateway | Redis | `100.89.50.125:6379` | 发布 `minidrive:file-events` |
| node-d（若启用 thumbnail worker） | Redis | `100.89.50.125:6379` | 缩略图任务队列 |
| AI Stream Worker | Redis | Compose 内部 `redis:6379` | `XREADGROUP`/`XACK` |
| AI Worker | Gateway | `http://100.75.93.124:18081` | manifest 与对象读取路由 |
| AI API | PostgreSQL | Compose 内部 `postgres:5432` | metadata/vector/Dataset/Agent audit |
| 浏览器 | AI API | `http://100.89.50.125:18290` | 搜索、Dataset、Agent API |

PostgreSQL `54329` 保持仅本机绑定；不需要让 Gateway、node-d 或浏览器直接访问它。

## 3. 在 ubuntu22data1（100.89.50.125）启动 AI Compose

进入仓库后创建本机私有配置：

```bash
cd ~/miniKV_v2/miniDriver
cp ai-app-lite/.env.example ai-app-lite/.env
```

将 `ai-app-lite/.env` 调整为：

```dotenv
# 现有 cinedata_pgdata volume 是以 cinedata 初始化的，首次沿用此值。
# 新建 volume 时可直接改为强密码；已有 volume 的密码轮换见本节后文。
POSTGRES_PASSWORD=cinedata

# 仅绑定该主机的 Tailscale 网卡，不要写 0.0.0.0。
AI_REDIS_BIND_ADDRESS=100.89.50.125
AI_REDIS_HOST_PORT=6379
AI_API_BIND_ADDRESS=100.89.50.125
AI_API_HOST_PORT=18290

# Worker 从 AI 容器经 Tailnet 回读 Gateway。
AI_MINIDRIVE_GATEWAY_URL=http://100.75.93.124:18081

# Gateway 提供网页的 origin；端口必须等于实际 Gateway Web 端口。
AI_CORS_ALLOW_ORIGIN=http://100.75.93.124:18081

AI_REDIS_FILE_EVENT_STREAM=minidrive:file-events
AI_EMBEDDING_PROVIDER=hash
AI_EMBEDDING_DIM=256
```

启动并检查：

```bash
docker compose -f ai-app-lite/compose.yaml up -d --build
docker compose -f ai-app-lite/compose.yaml ps
curl http://100.89.50.125:18290/healthz
docker compose -f ai-app-lite/compose.yaml logs -f api stream-worker worker
```

Compose 会把 Redis 与 API 分别绑定到 `100.89.50.125:6379`、`100.89.50.125:18290`，不绑定公网网卡。
Redis 开启 AOF 并使用 Compose volume；它没有密码认证，因为当前 C++ Redis Publisher 未实现 AUTH。必须通过
Tailscale ACL 或主机防火墙仅允许 `100.75.93.124`、`100.75.72.15` 与本机访问 6379。

注意：`POSTGRES_PASSWORD` 仅在 PostgreSQL **首次初始化空 volume** 时生效。当前已有 `cinedata_pgdata`
volume 时，直接把 `.env` 改成新密码会使 AI 容器无法连接；要轮换密码，先执行 `ALTER ROLE`，再同步 `.env` 并
重启 AI 服务，或在确认测试数据可删除后执行 `docker compose ... down -v` 后重新初始化。

## 4. 在 gateway（100.75.93.124）配置 Node Agent

Gateway 由 Node Agent 拉起时，Redis 地址来自 Node Agent YAML 的 `cluster.redis`；Node Agent 会将其传入
Gateway 的 `MINIKV_V2_REDIS_ADDRESS` 与 `MINIKV_V2_REDIS_PORT`。

在 Gateway 当前 YAML 中保留已有 `node`、`services`、数据目录和密钥配置，只将 `cluster` 的相关部分设为：

```yaml
cluster:
  secretFile: /path/to/cluster.secret
  gatewayAddress: 100.75.93.124
  gatewayPort: 18081
  redis:
    address: 100.89.50.125
    port: 6379
    thumbnailStream: media:thumbnail
    streamMaxLen: 100000
```

Gateway 的 AI Stream 默认名是 `minidrive:file-events`。目前 Node Agent 没有单独下发
`MINIKV_V2_REDIS_AI_STREAM`，因此不要修改此默认流名；Compose 的 `AI_REDIS_FILE_EVENT_STREAM` 必须保持相同。

重启 Node Agent 管理的 Gateway 后，在 Gateway 主机验证连通性（若安装了 `redis-cli`）：

```bash
redis-cli -h 100.89.50.125 -p 6379 PING
```

预期为 `PONG`。若失败，先检查 Tailscale 网络与 AI 主机的 6379 防火墙规则，而不是修改 AI Worker。

## 5. 在 node-d（100.75.72.15）配置

普通 DataNode 的 Gateway 控制地址仍然是：

```yaml
cluster:
  gatewayAddress: 100.75.93.124
  gatewayPort: 18081
```

如果 node-d **没有**启用 `thumbnail_worker`，其 Redis 字段对当前 DataNode 主路径不是关键；保持集群配置一致即可。
如果启用了 `thumbnail_worker`，同样设置：

```yaml
cluster:
  redis:
    address: 100.89.50.125
    port: 6379
    thumbnailStream: media:thumbnail
    streamMaxLen: 100000
```

不要把 `gatewayAddress` 写成 `100.89.50.125`：AI 主机不是 MiniDrive Gateway。

## 6. 网页前端的 AI API 地址

当前 `www-v2/app.js` 默认请求“网页所在主机的 18290 端口”。网页位于 Gateway `100.75.93.124`、AI API 位于
`100.89.50.125` 时必须覆盖它，否则浏览器会错误请求 `100.75.93.124:18290`。

在 Gateway 所服务的 `www-v2/index.html` 中、加载 `app.js` **之前**加入：

```html
<script>
  window.MINIDRIVE_AI_API = "http://100.89.50.125:18290";
</script>
<script src="app.js"></script>
```

不要重复加载 `app.js`；如果 HTML 已有对应的 script 标签，只在它之前插入第一段配置脚本即可。浏览器请求的
Origin 是 `http://100.75.93.124:18081`，这必须与 `.env` 的 `AI_CORS_ALLOW_ORIGIN` 完全一致。

## 7. 完整验收顺序

1. 在 `ubuntu22data1` 执行 Compose，确认 `postgres`、`redis`、`api` 为 healthy，两个 Worker 为 Up；
2. 在 Gateway 重启 Node Agent/Gateway，确认日志没有 `redis_ai_event_publish_failed`；
3. 上传一张 JPEG，检查：

   ```bash
   # ubuntu22data1
   docker exec cinedata-redis redis-cli XLEN minidrive:file-events
   docker compose -f ai-app-lite/compose.yaml logs --tail=100 stream-worker worker
   ```

4. 在浏览器工作台搜索，或直接请求：

   ```bash
   curl -X POST http://100.89.50.125:18290/ai/search \
     -H 'Content-Type: application/json' \
     -d '{"query":"日落","page_size":5}'
   ```

5. 搜索返回 Asset 后，继续执行 E0/F0 的 Dataset 与 Agent 验收。

## 8. 安全与当前限制

- 三个 `100.x` 地址属于 Tailscale 网络，但这不等于自动完成应用层授权；请用 Tailnet ACL/防火墙限制 Redis
  `6379` 和 AI API `18290` 的来源；
- 当前 Redis 无 AUTH/TLS，原因是现有 C++ Publisher/Consumer 还没有认证配置；不要把 6379 映射到公网；
- 当前 AI API 也没有身份认证；仅允许可信 Tailnet 设备访问，`owner/caller` 只是审计字段；
- `hash` provider 只证明事件、队列和检索链路。真实语义检索需后续单独准备 OpenCLIP 模型/GPU Worker 镜像；
- Gateway 端口、Node Agent 服务名、实际数据目录因你现有部署可能不同；不要用本文示例覆盖整份 YAML，只合并
  `cluster.redis` 与前端 AI API 配置。
