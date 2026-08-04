# V2 D2 Redis 任务投递部署

## Redis 所在机器

Redis 可以先部署在具备较多磁盘和内存的 Node C，但它是独立服务，不属于 Gateway
进程。Gateway 通过 Tailscale 地址连接 Redis；Redis 只保存可重投递的 Stream 消息，
任务状态仍保存在 Gateway 的 LevelDB。

Ubuntu 安装并启动：

```bash
sudo apt update
sudo apt install redis-server libhiredis-dev
sudo systemctl enable --now redis-server
```

生产网络策略应只允许 Gateway 的 Tailscale 地址访问 Redis 端口。优先使用 Tailscale ACL
或主机防火墙；不要将 6379 暴露到公网。`protected-mode yes` 保持开启。第一版使用共享
内网和集群内部密钥边界，Redis ACL 在后续安全阶段补充。

## Node Agent YAML

Gateway 所在机器的 Node Agent 配置必须声明 `cluster.redis`。示例不含真实 IP、密钥或
路径：

```yaml
node:
  nodeId: node-gateway
  advertiseAddress: 100.x.y.z
  capabilities: [storage]

cluster:
  secretFile: /secure/minikv/cluster.secret
  gatewayAddress: 100.x.y.z
  gatewayPort: 18081
  redis:
    address: 100.x.y.w
    port: 6379
    thumbnailStream: media:thumbnail
    streamMaxLen: 100000

services:
  - id: gateway-0
    type: gateway
    enabled: true
    listenPort: 18081
    dataDir: /data/minikv/gateway
```

`node.capabilities` 已被解析并保留给 D3 Worker 调度配置；D2 不改变 DataNode 注册的
`storage` 行为。D3 启用 thumbnail Worker 时才将能力上报给 Gateway。

Node Agent 将 Redis 配置转换为 Gateway 子进程环境变量：

```text
MINIKV_V2_REDIS_ADDRESS
MINIKV_V2_REDIS_PORT
MINIKV_V2_REDIS_THUMBNAIL_STREAM
MINIKV_V2_REDIS_STREAM_MAXLEN
```

## 故障行为与观察

- Redis 未启动时，Gateway 仍继续提供上传和下载；后台 Publisher 线程重试投递。
- Publisher 内存队列受限；满时只拒绝本次内存投递，`j:{jobId}` 仍是 `PENDING`。
- Gateway 每 5 秒扫描一小批应投递任务，30 秒后可补投一次，避免扫描风暴。
- 查看 Redis 消息：

```bash
redis-cli XREAD COUNT 10 STREAMS media:thumbnail 0
```

Stream 使用 `MAXLEN ~` 近似裁剪。Worker 崩溃或 Redis 重启后，D3 将使用 consumer group
和 `XAUTOCLAIM`；即使消息被裁剪，Gateway 账本扫描仍会重新发布未完成任务。
