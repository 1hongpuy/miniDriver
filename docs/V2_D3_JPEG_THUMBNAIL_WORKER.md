# V2 D3 JPEG 缩略图 Worker

## 已实现边界

本阶段只处理已经提交的 `.jpg` 和 `.jpeg` 对象，并为每个源对象创建两个独立、幂等的
派生任务：

| Profile | 最大边长 | JPEG 质量 | 用途 |
| --- | ---: | ---: | --- |
| `thumb-512-jpeg-v1` | 512px | 82 | 目录瀑布流/缩略图 |
| `preview-2048-jpeg-v1` | 2048px | 88 | 浏览器预览弹窗 |

它不会阻塞 Gateway 或 DataNode 的上传路径。Worker 是 Node Agent 启动的独立进程：

```text
Gateway LevelDB job + ThumbnailMeta
  -> Gateway Redis Streams Publisher
  -> thumbnail_worker Redis consumer thread
  -> Gateway claim lease
  -> manifest / DataNode Chunk GET / 临时源文件
  -> JPEG CPU thread（按 profile 缩放）
  -> internal derived-upload / DataNode Chunk PUT
  -> Gateway atomic READY update / XACK
```

Redis 只传递通知。任务状态、租约、失败次数、下一次重试时间和最终缩略图引用都在
Gateway LevelDB 中，因此 Worker 或 Redis 重启不会丢失最终事实。

## 配置示例

先在某一台内网机器启动 Redis；常见的初始部署是 Node C。该机器不需要是 Gateway：

```bash
sudo apt update
sudo apt install redis-server libhiredis-dev
sudo systemctl enable --now redis-server
redis-cli ping
```

只允许集群内 Tailscale 地址访问 6379，不要暴露到公网。

Gateway 的 Node Agent YAML 需要连接到同一 Redis：

```yaml
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

运行 Worker 的机器也使用同一段 `cluster.redis`，并声明能力和服务：

```yaml
node:
  nodeId: node-c
  advertiseAddress: 100.x.y.w
  capabilities: [storage, thumbnail]

cluster:
  secretFile: /secure/minikv/cluster.secret
  gatewayAddress: 100.x.y.z
  gatewayPort: 18081
  redis:
    address: 100.x.y.w
    port: 6379
    thumbnailStream: media:thumbnail

services:
  - id: datanode-0
    type: datanode
    enabled: true
    listenPort: 9002
    dataDir: /data/minikv/datanode

  - id: thumbnail-worker-0
    type: thumbnail_worker
    enabled: true
    dataDir: /data/minikv/thumbnail-worker
    tempDir: /data/minikv/tmp/thumbnail
    maxConcurrentJobs: 1
```

`maxConcurrentJobs` 目前必须为 `1`。这是刻意的 V2 上限：一个 Worker 在同一时刻只处理
一个 JPEG，避免媒体解码争抢 DataNode 的 CPU、内存和磁盘。多机器扩展时，启动更多
Worker 进程即可，它们作为同一 Redis consumer group 的竞争消费者领取不同任务。

## 状态和失败语义

目录 API 的文件项会分别带有 `thumbnail` 和 `preview` 元数据。两者使用相同的状态字段：

```json
"thumbnail": {
  "profile": "thumb-512-jpeg-v1",
  "state": "PENDING|RUNNING|READY|FAILED|UNSUPPORTED",
  "objectId": "only-present-when-ready"
}
```

- `PENDING`：Gateway 已持久化任务，尚未被 Worker 领取。
- `RUNNING`：Worker 已拿到 300 秒租约。
- `READY`：派生对象和 `ThumbnailMeta` 已通过同一个 Gateway WriteBatch 提交。
- `FAILED`：网络、Redis、DataNode 或临时 I/O 的可重试错误；Gateway 稍后重新发布。
- `UNSUPPORTED`：不是可解码 JPEG、超过输入/像素限制等，相同源字节重试没有意义。

派生对象不会出现在用户目录；它只由源文件的 `ThumbnailMeta` 引用。相同源内容出现在
多个逻辑目录时，每种 profile 仍只生成一份派生图。

源对象始终是“下载原始文件”的目标。预览弹窗在 `preview.state=READY` 时读取 2048px
派生 JPEG，但下载按钮仍读取源对象。对于新上传对象，`PENDING/RUNNING` 预览会显示
生成中，不会退回下载原图作为预览；旧对象没有 `preview` 元数据时保持原有预览行为，
以后由回填任务补齐。

## 手动验证

1. 用 Node Agent 启动 Gateway、至少一个 DataNode 和一个 `thumbnail_worker`。
2. 上传一个新的 JPEG，等待数秒。
3. 检查 Worker 日志中是否有两条 `event=derived_image_ready`，profile 分别为 512 和 2048。
4. 查询 `GET /api/v2/catalog?path=/目标目录`，确认源文件的 `thumbnail.state`、
   `preview.state` 都为 `READY`。
5. 用对应的 `objectId` 请求 `/api/v2/objects/{objectId}/manifest`，按普通 Chunk 下载流程
   读取 JPEG；最长边分别不超过 512px 和 2048px。

当前不支持 RAW、PNG、HEIC 或视频。它们不会由 Gateway 投递派生图任务；后续加入各自
解码器后沿用同一任务、派生对象和状态模型。
