# MiniDrive 前端与 AI-App-Lite 使用说明（2026-08-20）

## 1. 定位与版本边界

MiniDrive 的核心仍是 C++ 对象存储：Gateway 管理上传会话、路由和元数据，客户端向 DataNode
直传 Chunk，DataNode 负责本地写入与链式副本。`www-v2` 是该存储系统的摄影素材工作台。

本次新增的 AI-App-Lite 是一个**独立的 Python 最小原型**，用于验证“素材元数据 → 图文向量 →
自然语言检索”的闭环；它不参与文件上传、双副本或元数据一致性，也不改变 V2 数据面。

```text
已发布 V2.0.1
  └─ www-v2：素材目录中的 AI 搜索界面与快捷标签

当前 V3 工作区原型
  └─ ai-app-lite：SQLite Job Queue、Worker、Embedding、检索与可选文案 API
```

`v2.0.1` 只包含前端界面改动。AI-App-Lite 尚未作为 V2 发布的一部分，也尚未实现“上传完成自动索引”。

## 2. 整体架构

```text
浏览器 / www-v2
  ├─ /api/v2/* ───────────────────────────────→ MiniDrive Gateway
  │                                                └→ DataNode 数据面
  │
  └─ http://<same-host>:18290/ai/search ──────→ AI-App-Lite API
                                                   └→ SQLite AssetStore
                                                        ├→ SQLite Job Queue
                                                        ├→ Asset Worker
                                                        ├→ EXIF / 基础元数据
                                                        └→ Embedding Provider
                                                             ├→ hash-smoke（默认）
                                                             └→ OpenCLIP（可选）
```

两条请求路径严格分开：

- `www-v2 → Gateway`：目录、上传、下载、预览、节点状态等已有对象存储功能；
- `www-v2 → AI-App-Lite`：只发送文本查询，返回已索引素材的 `asset_id`、元数据与相似度；
- AI-App-Lite **不读取或转发上传 Body**，也不会进入 DataNode 的副本、背压与磁盘写路径。

## 3. 摄影素材工作台（`www-v2`）

### 3.1 原有能力

工作台面向 RAW、JPEG、视频等摄影素材，主要包含：

- 文件级选择、分块上传、上传进度与可重试失败状态；
- 目录树、面包屑、新建/删除目录；
- 素材卡片、缩略图/预览状态、图片预览、下载与删除；
- Gateway 汇总的 DataNode 状态展示；
- 浏览器侧的 Chunk SHA-256 计算与传输会话恢复。

上传与下载仍使用 `/api/v2`。AI 搜索失败或服务没有启动时，不会影响这些原有功能。

### 3.2 新增 AI FIND 搜索区

素材目录顶部增加 `AI FIND / 02.1`：

```text
文字描述 + 快捷标签
      ↓
POST /ai/search
      ↓
相似度、索引元数据、当前目录预览入口
```

提供的初始标签是：`日落`、`寺庙`、`城市夜景`、`人像`、`山景`、`鸟类`。

- 标签可多选；选中的标签会追加到搜索框，用户可继续自由补充描述；
- 提交时发送 `{ "query": "...", "top_k": 9 }`；
- 页面展示 AI 返回的 `object_key`、`scene/camera_model/capture_time/dir_path` 摘要及相似度；
- 若返回的 `asset_id` 等于**当前目录**某素材的 `objectId`，结果卡片会显示“在目录中预览”，并复用既有预览器；
- 若当前目录没有该对象，只显示检索结果和提示，避免前端猜测对象所在路径。

默认 AI 服务地址是：

```text
http(s)://当前网页主机:18290
```

部署时可在加载 `app.js` 之前定义：

```html
<script>
  window.MINIDRIVE_AI_API = "https://ai.example.internal";
</script>
```

以覆盖 AI 服务地址。该地址应由可信部署配置注入，而不是让普通用户在页面输入。

## 4. AI-App-Lite（`ai-app-lite/`）

### 4.1 目标

AI-App-Lite 是用于验证链路的最小实现：

```text
登记素材
  → 进入 SQLite Job Queue
  → Worker 提取基础元数据/EXIF
  → 生成图片向量
  → 保存资产与向量
  → 文本向量检索 + 可选元数据过滤
  → 可选 LLM 文案
```

它有意采用 SQLite 与标准库 HTTP Server，避免在第一阶段就引入 PostgreSQL、Kafka、Flink 或 Kubernetes。
这些属于数据基础设施后续阶段，而不是当前 MVP 的必要依赖。

### 4.2 组件职责

| 模块 | 职责 |
|---|---|
| `cinedata_ai/server.py` | HTTP API、请求校验、CORS、搜索与文案请求编排 |
| `cinedata_ai/store.py` | SQLite assets/jobs 表、任务认领、向量余弦相似度与元数据过滤 |
| `cinedata_ai/worker.py` | 拉取 pending Job，做元数据提取、hash 和图片向量构建 |
| `cinedata_ai/exif.py` | 在 Pillow 可用时提取 EXIF；不可解析文件不会使服务崩溃 |
| `cinedata_ai/embeddings.py` | 可切换的 Embedding Provider |
| `cinedata_ai/config.py` | 环境变量配置 |

### 4.3 API

| API | 用途 | 主要请求字段 |
|---|---|---|
| `GET /healthz` | 服务健康检查 | — |
| `POST /ai/assets` | 异步登记待索引素材 | `asset_id`、`object_key`、`local_path`、`metadata` |
| `GET /ai/jobs/{job_id}` | 查询索引任务状态 | — |
| `POST /ai/search` | 文本检索 | `query`、可选 `top_k`、`filters` |
| `POST /ai/caption` | 对选中的素材生成文案 | `asset_ids`、可选 `instruction` |

登记 MiniDrive 素材时应遵循下面的映射：

```json
{
  "asset_id": "<MiniDrive objectId>",
  "object_key": "<虚拟目录/文件名>",
  "local_path": "<Worker 可访问的本地文件路径>",
  "metadata": {
    "object_id": "<MiniDrive objectId>",
    "dir_path": "/travel/2026",
    "scene": "temple sunset",
    "camera_model": "Nikon D610"
  }
}
```

`asset_id = objectId` 是前端可从搜索结果回到对象预览的关键约定。

### 4.4 Embedding 的现实边界

默认 `AI_EMBEDDING_PROVIDER=hash` 使用确定性 `hash-smoke` 向量：

- 优点：零模型下载、零 GPU 依赖，适合验证 API、队列、结果格式与前端交互；
- 限制：**不具备真实的图文语义能力**，不能据此评价“日落/寺庙”检索质量。

要验证真实的图片—文字语义检索，需要安装 PyTorch 和 OpenCLIP，并设置：

```bash
pip install torch open_clip_torch
export AI_EMBEDDING_PROVIDER=openclip
```

OpenCLIP 把图像与文本编码到同一向量空间；当素材量增大时，再把 SQLite 的 JSON 向量迁移到
PostgreSQL + pgvector 或专业向量数据库。

## 5. 原理与知识点说明

这一章回答“这个前端和 AI 应用为什么这样设计”，而不是只列出接口。

### 5.1 前端标签不是分类器，而是查询构造器

页面里的“日落”“寺庙”等标签本身不做模型推理，也没有把文件自动分类为某个标签。它的职责更简单：

```text
用户点击「日落」+「寺庙」
  → 前端维护 activeTags 集合
  → 合并到搜索框，得到「日落 寺庙」
  → fetch POST /ai/search
  → 后端将文本编码为查询向量
```

这样设计有两个好处：

1. 标签是快捷输入，用户仍可以补充“傍晚、暖色、无人”等自由文本；
2. 前端不绑定模型。以后从 Hash provider 换成 OpenCLIP、从 SQLite 换成 pgvector，页面请求格式仍可保持。

因此应区分两个概念：

| 概念 | 当前实现 | 后续可扩展 |
|---|---|---|
| 查询标签 | 用户主动选取，组成文本 query | 推荐标签、历史搜索、自动补全 |
| 自动标签 | 当前未实现 | Worker 用模型生成 `scene=temple sunset` 等元数据 |
| 语义检索 | API 支持；质量取决于 Embedding Provider | OpenCLIP、多模态 rerank |

### 5.2 浏览器的异步请求与状态管理

浏览器 JavaScript 运行在单一 UI 线程。若点击搜索后同步等待网络响应，页面会无法继续响应；因此使用
`fetch` 与 `async/await`：

```text
点击检索
  → 禁用按钮、状态显示「正在检索」
  → 发起异步 fetch
  → 浏览器仍可绘制页面、响应其他事件
  → Promise 完成后渲染结果或错误信息
```

前端给每次搜索分配递增 `requestId`。如果用户连续搜索 A、B，但慢请求 A 比 B 更晚返回，则 A 的结果会被
丢弃，避免旧结果覆盖新查询。这是典型的前端异步竞态保护。

搜索结果中的“在目录中预览”并不重新实现下载：它通过 `asset_id = MiniDrive objectId` 找到当前目录已有
对象，然后调用原来的 `selectObject/openObjectPreview`。这样 AI 层只负责找对象，文件读取、Manifest、Chunk
校验与预览仍由原 MiniDrive 路径完成。

### 5.3 Same-Origin 与 CORS

网页一般由 Gateway 的 HTTP 端口提供，而 AI-App-Lite 默认在 `18290` 端口。即使主机相同，**端口不同也
是不同 Origin**：

```text
http://127.0.0.1:8080   !=   http://127.0.0.1:18290
```

浏览器会在真正的 JSON `POST` 前发送 `OPTIONS` 预检，询问 AI 服务是否允许这个来源、`POST` 方法和
`Content-Type` 请求头。AI 服务在响应中返回：

```text
Access-Control-Allow-Origin
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

开发默认值是 `*`，只为简化本地验证。生产环境应将 `AI_CORS_ALLOW_ORIGIN` 固定为可信工作台地址，并补充
身份认证；CORS 只限制浏览器跨站读取，**不是访问控制或鉴权机制**。

### 5.4 为什么索引要放在 Worker，而不是 HTTP 请求内

素材分析可能涉及读取大文件、解析 EXIF、加载模型和 GPU/CPU 推理。若 `POST /ai/assets` 直接完成所有工作：

- HTTP Handler 会长时间占用连接与线程；
- 多个上传同时完成时会把推理/磁盘资源瞬间压满；
- 一次进程重启可能丢失正在执行的工作。

所以当前使用生产者—消费者模型：

```text
API（生产者）                 Worker（消费者）
POST /ai/assets                循环领取 pending Job
    ↓                                   ↓
SQLite jobs: pending  ──→ processing ──→ done / failed
```

API 只负责校验并返回 `202 Accepted + job_id`，说明“任务已接受”，而不是“向量已经生成”。Worker 在后台完成
索引；前端或调用方可查询 `/ai/jobs/{job_id}` 获得最终状态。这是异步任务系统最基本的语义。

SQLite 目前通过 `status='pending'` 的条件更新来避免两个 Worker 同时成功认领同一个 Job。它适合单机 MVP；
分布式 Worker 下需要 Redis Streams/Kafka 的 consumer group、可见性超时、重试和死信队列。

### 5.5 元数据、Embedding 与向量相似度

一条素材在 AI 层由三类信息构成：

```text
对象身份：asset_id / object_key
结构化元数据：相机、时间、目录、EXIF、场景标签
语义向量：embedding = [x1, x2, ..., xd]
```

Embedding 是模型把图片或文本映射成固定长度浮点向量的过程。真实图文模型（例如 OpenCLIP）的训练目标是让
语义相近的图片和文本在同一向量空间距离更近：

```text
图片「夕阳下的寺庙」 ─→ [0.12, -0.08, ...]
文字「暖色寺庙日落」 ─→ [0.11, -0.07, ...]
```

当前 Store 使用余弦相似度排序：

```text
cosine(q, v) = (q · v) / (||q|| × ||v||)
```

- `q`：查询文本的向量；
- `v`：某素材图片的向量；
- 值越接近 `1`，两个向量方向越接近，模型认为语义越相近。

检索先按 `camera_model`、`capture_time` 等元数据过滤候选，再在候选中按余弦相似度取 Top K。这就是
“结构化过滤 + 向量召回”的最小形式。小数据量直接遍历 SQLite 中全部向量是正确且易解释的；百万级向量时
才需要 pgvector 的索引、Faiss/HNSW 或 Milvus 等近似最近邻索引。

### 5.6 为什么默认 Hash provider 不能叫 AI 检索

默认 `hash-smoke` Provider 只是把输入稳定地转换为一个伪向量，目的是测试：

```text
请求格式是否正确
Job 是否被 Worker 消费
向量是否保存、排序和返回
前端是否能展示结果
```

它不理解图片像素，也不理解“寺庙”这个词，因此相似度没有业务语义。真正演示 AI 检索前必须换成 OpenCLIP
或其他图文模型，并准备有代表性的图片集与标注/人工检查结果。面试时应如实表述：当前已完成**可替换的
检索基础设施闭环**，真实语义质量依赖后续接入的 Embedding 模型。

### 5.7 对象存储与 AI 索引的正确边界

MiniDrive 的数据面保证“文件怎样可靠存储”；AI-App-Lite 负责“已存文件怎样派生元数据和向量”。它们之间
不能通过前端猜测上传是否成功，而要用已提交的对象事件连接：

```text
Gateway File Commit 成功
  → 发布 FILE_UPLOAD(objectId, hash, path, metadataVersion)
  → 索引 Worker 以 objectId 做幂等键
  → 读取已提交、可验证的对象
  → 生成/更新向量和元数据
  → 标记 Asset ready
```

这里的关键知识点：

- **Commit 后再发事件**：避免索引到未完成或最终会失败的对象；
- **objectId 作为幂等键**：重复投递事件不会创建重复资产；
- **metadataVersion**：对象元数据变化后可判断旧索引是否需要重建；
- **Worker 不走浏览器下载路径**：应通过内部受控读取接口或对象存储客户端读取；
- **事件不带文件 Body**：消息系统只传小元数据，文件仍留在对象存储中。

这也是后续 Redis Streams/Kafka 的真正价值：它们解耦 Commit 的低延迟控制路径和可能较慢的 AI 推理路径。

## 6. 本地启动与验证

在仓库根目录：

```bash
export PYTHONPATH="$PWD/ai-app-lite"
export AI_APP_DB="$PWD/ai-app-lite/data/ai.sqlite3"

# 终端一：AI API（前端默认访问 18290）
python3 -m cinedata_ai.main api --port 18290

# 终端二：索引 Worker
python3 -m cinedata_ai.main worker
```

如果网页不是从 `http://127.0.0.1:8080` 提供，开发环境可设置允许来源：

```bash
export AI_CORS_ALLOW_ORIGIN="http://127.0.0.1:8080"
```

然后按顺序验证：

1. 通过 `POST /ai/assets` 登记一条素材；
2. 轮询 `GET /ai/jobs/{job_id}`，等待状态变为 `done`；
3. 在 `www-v2` 目录页选择标签或输入描述，点击“检索”；
4. 确认结果显示，并在 `asset_id/objectId` 一致且对象位于当前目录时测试预览按钮。

当前自动化验证包括 AI pipeline 两个单元测试，以及前端调用所需的 CORS `OPTIONS` 冒烟检查。

## 7. 当前未实现的内容

以下项目明确不应被误称为已完成：

- 上传 Commit 自动发布 `FILE_UPLOAD` 事件；
- Redis Streams/Kafka 消费与重试语义；
- Worker 从 DataNode 或对象存储安全地读取对象内容；
- OpenCLIP 默认启用、GPU batch 推理；
- PostgreSQL/pgvector、Milvus/Faiss、Iceberg、Spark/Flink；
- 生产身份认证、租户隔离、细粒度权限或生产级 CORS 策略；
- 跨目录全局对象定位与搜索结果缩略图。

## 8. 推荐下一步

下一步优先实现 MiniDrive Commit 到 AI 索引的自动桥接，而不是继续堆叠更多前端控件：

```text
Gateway File Commit
  → 可靠地发布 FILE_UPLOAD(objectId, path, hash, metadataVersion)
  → Redis Stream
  → AI Worker 消费、幂等建索引
  → 写入 asset_id = objectId
  → www-v2 可立即自然语言检索并预览
```

该步骤完成后，系统才具备真正可演示的“上传摄影素材 → 后台索引 → 标签/自然语言搜索 → 打开原图”闭环。
