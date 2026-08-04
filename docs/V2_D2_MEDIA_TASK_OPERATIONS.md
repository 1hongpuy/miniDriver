# V2 D2 媒体任务运行说明

## 已实现边界

D2 已提供可复用的任务基础设施，不解码图片，也不启动 thumbnail Worker：

```text
JPEG 文件提交
  -> Gateway LevelDB 写 j:{jobId} 与 d:{fileHash}:thumbnail:{profile}
  -> 有界 Publisher 后台线程 XADD media:thumbnail
  -> 后续 Worker claim / complete / fail
```

上传、Chunk 复制和下载不等待 Redis。Redis 不可用时，文件仍可 `AVAILABLE`；任务保持
`PENDING`，Gateway 的 5 秒补偿扫描会在 30 秒节流窗口后重新尝试投递。

## Gateway 内部接口

每个接口都要求：

```text
X-Cluster-Internal-Token: <cluster secret>
```

```text
POST /internal/v2/media/jobs/{jobId}/claim
body: {"leaseSeconds":60}

POST /internal/v2/media/jobs/{jobId}/complete
body: {"leaseToken":"...","derivedObjectId":"...","derivedFileHash":"..."}

POST /internal/v2/media/jobs/{jobId}/fail
body: {"leaseToken":"...","unsupported":0,"error":"...","retryAfterSeconds":60}
```

`claim` 返回 `sourceFileHash`、`profile`、`leaseUntil` 和随机 `leaseToken`。`complete`、
`fail` 只有在租约尚未过期且携带当前 lease token 时才能更新状态。旧 Worker 在租约过期后
不能覆盖新 Worker 的结果。

## 验证

构建并运行 D2 相关测试：

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
ctest --test-dir build -R '^(media_job|gateway_media_jobs|redis_task_publisher|v2_media_task_redis|node_agent_logging_config)$' --output-on-failure
```

`v2_media_task_redis` 会在 `127.0.0.1:16379` 临时启动 Redis，确认 Publisher 真实写入
`media:thumbnail:test`。端口被占用时可覆盖：

```bash
MINIKV_TEST_REDIS_PORT=16380 ctest --test-dir build -R '^v2_media_task_redis$' --output-on-failure
```

## D3 接续点

D3 添加 `minikv_v2_thumbnail_worker`：它使用 Redis consumer group 读取消息，调用 `claim`，
经 Gateway manifest 和 DataNode Chunk GET 下载原图，生成 `thumb-512-jpeg-v1` 后上传派生
对象，最后调用 `complete`。目录 API 的 `thumbnail` 字段、JPEG 解码、RAW preview 和前端
缩略图展示均属于 D3，未在 D2 实现。
