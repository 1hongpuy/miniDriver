# V2 派生媒体任务与 JPEG 缩略图设计

## 目标与范围

本设计将 V2 的后台派生媒体能力拆成两个连续阶段：

```text
D2  通用派生任务基础设施
D3  JPEG thumb-512-jpeg-v1 生成与目录展示
```

本轮只支持 JPEG 输入，生成最长边为 512 像素、质量为 82 的 JPEG 缩略图。缩略图用于
目录列表和网格浏览，不替换原始文件下载，也不改变 DataNode 的 Chunk 格式或上传协议。

本轮不做：RAW 完整解码、RAW 内嵌预览、视频抽帧、2048px 预览图、AI 标签、向量检索、
多 Gateway 或任务优先级调度。

## 设计原则

- Gateway 的 LevelDB 是任务和派生结果的事实来源；Redis Streams 仅负责投递和消费者组。
- DataNode 只存取 Chunk 字节，不执行媒体解码，也不理解缩略图业务。
- Worker 只能通过 Gateway manifest 和 DataNode HTTP Chunk GET 读取原始文件，不能读取
  DataNode 的 `disk0.data`、物理 offset 或 LevelDB。
- 派生缩略图按普通 V2 对象存储，但不创建用户目录条目，因此不显示为用户文件。
- 同一 `fileHash` 内容只生成一份相同 profile 的缩略图；多个逻辑目录引用可复用它。
- 所有等待 Redis、网络和 CPU 解码的阻塞操作均不进入 DataNode 或 Gateway 的 EventLoop。

## D2：任务基础设施

### 任务账本

Gateway LevelDB 新增两类键：

```text
j:{jobId}
  -> MediaJob

d:{sourceFileHash}:thumbnail:thumb-512-jpeg-v1
  -> ThumbnailMeta
```

`MediaJob` 保存：

```text
jobId, type, sourceFileHash, profile,
state(PENDING/RUNNING/READY/FAILED/UNSUPPORTED),
attempts, leaseUntil, nextRetryAt, lastError, createdAt, updatedAt
```

`ThumbnailMeta` 是面向目录查询的派生结果索引，保存：

```text
sourceFileHash, profile, state,
derivedObjectId, derivedFileHash,
jobId, updatedAt, lastError
```

同一 source/profile 重复请求时：`READY` 直接复用；`PENDING` 或 `RUNNING` 不重复创建；
`FAILED` 在退避时间到期后才能重试；`UNSUPPORTED` 不自动重试。

### Redis Streams

第一版按任务类别拆分 Stream，避免一种 Worker 领取错误类型的任务：

```text
media:thumbnail    group=thumbnail-workers
```

消息只包含 `jobId`、任务类型和 profile 等路由信息；完整参数必须由 Worker 向 Gateway
查询。Gateway 创建或重投递任务时严格按顺序：

```text
LevelDB WriteBatch: job=PENDING + ThumbnailMeta=PENDING
  -> Redis XADD media:thumbnail
```

Worker 收到消息后先向 Gateway claim。Gateway 对 `PENDING` 或已过期 `RUNNING` 的任务
签发短租约并返回任务详情。Worker 成功将结果写回 Gateway 后才 `XACK`；失败不把 Redis
消息视为唯一恢复依据，Gateway 的补偿扫描会根据账本重新投递。

### Worker 进程模型

新增独立二进制：

```text
minikv_v2_thumbnail_worker
```

它由现有 `node_agent` 通过 YAML 服务项启动。单个 Worker 进程内部划分：

```text
RedisConsumer 线程 (XREADGROUP)
  -> 有界 MediaJobQueue
  -> JPEG CPU worker，初始并发 1
  -> EventLoop + AsyncHttpClient
       Gateway 控制请求 / DataNode Chunk GET / 派生对象上传
```

Redis 阻塞消费和 JPEG 解码均不占用 HTTP EventLoop。与 DataNode 同机运行时，这是独立
进程、独立 epoll 实例和独立内存空间；只能共享机器资源，不能共享 EventLoop 或 C++ 对象。

YAML 中能力和服务实例均由配置声明：

```yaml
node:
  capabilities: [storage, thumbnail]

services:
  - id: thumbnail-worker-0
    type: thumbnail_worker
    enabled: true
    concurrency: 1
    tempDir: /data/minikv/tmp/thumbnail
```

Gateway 节点注册必须保留 YAML 中上报的 capabilities，不能再把能力覆盖为仅
`storage`。

### 故障与补偿

- Worker 崩溃：任务租约过期后变为可重新 claim；Redis pending message 可由
  `XAUTOCLAIM` 回收。
- Redis 重启或消息被裁剪：Gateway 周期性扫描 `PENDING` 和租约过期的 `RUNNING` 任务，
  重新 `XADD`。
- JPEG 不支持或解码失败：记录明确错误；真正的格式不支持进入 `UNSUPPORTED`，不无限重试。
- 历史文件：上线时运行一次限速回填，按 LevelDB 游标扫描 JPEG 文件；发现无对应
  `ThumbnailMeta(READY)` 即创建任务。之后低频补偿扫描继续使用持久游标。

该扫描器不是 GC。GC 负责删除不再有源对象引用的派生对象和 Chunk。

## D3：JPEG 缩略图

### 执行流

```text
File commit
  -> Gateway enqueue thumbnail job
  -> Worker claim
  -> Gateway manifest
  -> 从健康 DataNode 顺序读取源文件 Chunk 到 tempDir
  -> stb_image 解码 JPEG
  -> 应用 EXIF orientation
  -> 等比缩放，最长边 <= 512px
  -> stb_image_write 编码 JPEG (quality=82)
  -> 以内部派生写入流程存为普通 V2 对象
  -> Worker result 回写 Gateway
  -> ThumbnailMeta = READY
```

解码前必须检查图片像素上限和临时文件大小，避免伪造 JPEG 解压成不成比例的内存使用。
初始限制为可配置项；默认只允许一个解码任务，优先保证存储上传不被媒体任务抢占。

派生对象没有 `path:` 用户目录映射，只由 `ThumbnailMeta.derivedObjectId` 引用。目录中的
多个 ObjectMeta 如果有相同 `fileHash`，都引用同一缩略图。

### Gateway 和前端 API

Catalog 文件项新增可选字段：

```json
"thumbnail": {
  "state": "PENDING|READY|FAILED|UNSUPPORTED",
  "profile": "thumb-512-jpeg-v1",
  "derivedObjectId": "..."
}
```

前端行为：

- `READY`：懒加载派生对象 manifest 和小 JPEG，显示在目录项中。
- `PENDING/RUNNING`：显示固定尺寸占位，不触发原图下载。
- `FAILED/UNSUPPORTED`：显示格式图标；原图下载仍可用。

第一版只在目录列表显示 512px 缩略图。现有预览弹窗继续使用原始文件；后续
`preview-2048-jpeg-v1` 复用相同 D2 机制，但属于独立任务 profile。

### 删除语义

删除用户 ObjectMeta 后，Gateway 必须在判断源 `fileHash` 是否仍被任何逻辑对象引用时
同时考虑派生对象关系：

- 仍存在源对象：保留 ThumbnailMeta 和派生缩略图。
- 最后一个源对象被删除：删除 ThumbnailMeta，创建派生对象清理记录，由后续 GC 物理删除。

本轮不改变现有直接删除的可见语义；只保证缩略图不会重新显示为用户目录文件。

## 验收

1. JPEG 上传完成后，Gateway 账本存在一个 `thumbnail` PENDING 任务且 Redis 有一条消息。
2. 一个 configured thumbnail Worker 能完成任务，目录在刷新后显示 512px JPEG。
3. 同一内容上传到两个路径，只产生一个派生缩略图任务与一个派生对象。
4. Worker 被停止后，任务租约到期可由同类 Worker 接管；Redis 重启后账本扫描可重投递。
5. Worker 的 JPEG 解码不会阻塞 DataNode EventLoop，且并发超过配置上限时在 Worker 队列受控等待。
6. 不支持格式不会无限重试，目录仍可下载原始文件。

