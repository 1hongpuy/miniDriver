# CineLake Phase C Lite：语义图像索引架构与测试报告（2026-08-21）

## 1. 结论与范围

Phase C Lite 已在 Phase B 自动事件链路之上增加“可替换的真实图文模型接口、批量索引、可解释图像质量元数据、
zero-shot 标签与检索评测工具”。它的目标是让 JPEG/PNG/WebP/TIFF 素材具备真实图文向量检索能力，而不是把
现有 Hash smoke 流程误称为 AI 语义搜索。

自动化测试使用确定性语义 Stub 验证协议、批处理、模型隔离、标签落库和 Recall@K 计算；真实 OpenCLIP 结果
必须来自固定素材集而不是截图。本报告已在第 6 节追加一次真实 CPU 运行；该结果仅代表该素材集、该模型和该
机器，不外推为通用模型质量或生产吞吐。

当前支持范围：

| 能力 | 状态 | 说明 |
|---|---|---|
| OpenCLIP 图像/文本 embedding | 已实现，可选依赖 | 默认 `ViT-B-32-quickgelu/openai`，按配置加载 |
| 批量图片推理 | 已实现 | `AI_WORKER_BATCH_SIZE`，默认 4；有界 SQLite Job 批次 |
| Zero-shot 标签 | 已实现 | 日落、山景、寺庙、城市夜景、人像、鸟类、海岸、森林 |
| EXIF + 图像质量 + pHash | 已实现 | Pillow best effort；质量指标为启发式而非训练模型 |
| 模型版本隔离 | 已实现 | 查询只比较相同 `embedding_model` 的向量 |
| JPEG/PNG/WebP/TIFF | 已实现 | OpenCLIP 只接收可解码 raster image |
| RAW/视频/OCR | 未实现 | 需要 preview/keyframe/OCR 派生 Worker，明确失败而非错误索引 |
| GPU 多进程调度/真实性能报告 | 未实现 | 后续根据真实负载再做 |

## 2. Phase C 架构

```text
                         www-v2 AI FIND
                                │ POST /ai/search
                                ▼
                    AI Query API + model filter
                                │ embed_text(model_id)
                                ▼
                    SQLite assets / cosine Top-K
                                │
                                ▼
                 asset_id = MiniDrive objectId

Browser upload
  → Gateway File Commit → LevelDB Outbox → Redis FILE_UPLOAD_COMMITTED
                                                 │
                                                 ▼
                                   stream-worker / consumer group
                                                 │ durable idempotent Job
                                                 ▼
                             AssetWorker (bounded batch, default=4)
                              │                         │
                   MiniDriveObjectReader         local debug path
                 manifest → replica chunks                 │
                              └──────────────┬─────────────┘
                                             ▼
                EXIF + media kind + brightness/contrast/sharpness/pHash
                                             │
                           ┌─────────────────┴─────────────────┐
                           ▼                                   ▼
             Hash smoke (pipeline only)          OpenCLIP image batch + zero-shot labels
                           │                                   │
                           └─────────────────┬─────────────────┘
                                             ▼
             assets(embedding, embedding_provider, embedding_model, metadata, READY)
```

边界原则：Gateway 和 Redis 从不传递图片 Body；质量/标签/向量均为可重建派生数据，不能修改原对象；模型推理不在
Gateway 或 DataNode Reactor 线程中执行。

## 3. 关键设计

### 3.1 模型身份是向量契约

每个 Provider 公开 `name` 和精确 `model_id`：

```text
hash-smoke@sha256-token-v1/d256
openclip:ViT-B-32-quickgelu:openai
```

Asset 记录 `embedding_provider` 与 `embedding_model`，Query API 使用当前 Provider 的 `model_id` 过滤结果。
这避免把不同维度或不同语义空间的向量混合计算余弦相似度。模型升级后，旧 Asset 不会误入新模型查询；应通过
新 `objectVersion + processor/model version` 策略重建。

### 3.2 批量与失败隔离

Worker 先有界地认领一批 `pending` Job，再 materialize 各对象。准备阶段失败的 Job 独立记为 `failed`，不会阻断
其他 Job；有效 raster 图像再交给 Provider 一次性 batch encode。若整个模型 batch 调用失败，该批 Job 全部记录
同一个 inference error，保留可观测性而不会写入半成品向量。

这不是 GPU 调度器：默认 batch=4 只是限制内存和模型调用次数。GPU 数、显存、吞吐和队列高水位必须在真实模型
与真实素材集上测得后再扩大。

### 3.3 质量、近似去重与标签

Pillow 分析会写入：

```text
brightness / contrast / sharpness / exposure / quality_score / perceptual_hash
quality_method = pillow-heuristic-v1
```

`quality_score` 是亮度居中、对比度和边缘强度组合得到的启发式分数，不能替代审美模型或自动删除素材。
`perceptual_hash` 可用于后续寻找近似候选，当前只记录、不自动去重；相似图片可能是连拍、裁剪或不同曝光，
不能仅因 hash 接近就删除。

OpenCLIP zero-shot 标签以稳定的中文显示名和英文 prompt 计算 image/text cosine，记录标签、分数、prompt 与
model source。低于 `AI_SEMANTIC_TAG_MIN_SCORE` 的候选不会写入；标签是模型预测，不能当成人工事实。

### 3.4 非 Raster 的正确失败

当 Provider 是 OpenCLIP 时，RAW 和视频没有预览/关键帧，就不能将其原始字节强塞给图片模型。Worker 会将 Job
标记 `failed` 并说明需要 derivative Worker。未来正确 DAG 为：

```text
RAW → EXIF → embedded preview extraction → OpenCLIP(preview)
video → probe → keyframes → per-frame embedding → aggregation
PDF → text/OCR → text chunks → text embedding
```

## 4. 配置与运行

安装真实模型依赖后：

```bash
pip install -r ai-app-lite/requirements.txt
pip install torch open_clip_torch

export PYTHONPATH="$PWD/ai-app-lite"
export AI_EMBEDDING_PROVIDER=openclip
export AI_OPENCLIP_MODEL=ViT-B-32-quickgelu
export AI_OPENCLIP_PRETRAINED=openai
export AI_WORKER_BATCH_SIZE=4
export AI_SEMANTIC_TAG_TOP_K=3
export AI_SEMANTIC_TAG_MIN_SCORE=0.20
python3 -m cinedata_ai.main worker
```

固定评测集格式：

```json
[
  {"query":"寺庙日落", "expected_asset_ids":["object-temple-01"]},
  {"query":"鸟类", "expected_asset_ids":["object-bird-02"], "filters":{"camera_model":"Nikon"}}
]
```

在目标对象已完成索引后运行：

```bash
python3 -m cinedata_ai.main evaluate --cases ./cases.json --top-k 5
```

输出 `embedding_model`、每条 query 的命中列表、`recall_at_k` 与 `mrr_at_k`。任何报告必须同时保存素材集版本、
case 文件、模型/权重、CPU/GPU、batch size、索引耗时和原始 JSON。

## 5. 测试结果

本次在当前环境运行：

```text
PYTHONPATH=ai-app-lite python3 -m unittest discover -s ai-app-lite/tests -v
结果：7 tests passed，1 Redis live test 在无测试 Redis 时按预期 skip
```

新增 `test_phase_c_semantic_pipeline.py` 覆盖：

| 测试 | 结果 | 证明 |
|---|---:|---|
| 两个图像的 bounded batch | Pass | Job 批量认领、向量/标签分别写入 Asset |
| 模型版本字段 | Pass | `embedding_model` 持久化并作为查询过滤条件 |
| 质量与 pHash | Pass | raster 元数据生成、同一 hash 的 Hamming distance 为 0 |
| 固定 Query Case | Pass | 语义 Stub 下 `Recall@1=1.0`、`MRR@1=1.0`，证明评测计算与模型隔离 |
| 视频边界 | Pass | semantic provider 将无 keyframe 的视频显式记录为 failed |

Phase B 回归仍应一并执行：

```bash
cmake --build build -j2
ctest --test-dir build --output-on-failure -R 'gateway_ai_index_outbox|v2_ai_file_event_redis'
```

注意：Stub 的 1.0 结果只验证测试协议；它**不是** OpenCLIP 的实际检索指标。真实模型指标必须由上节固定图片集
独立产生。

## 6. 真实 OpenCLIP 评测：本地摄影测试集（2026-08-21）

### 6.1 数据集与方法

用户提供的本地 `测试/` 目录只读导入，未修改原始照片：

| 项目 | 值 |
|---|---:|
| 图片 | 56 张 JPEG，约 572.03 MiB |
| 分辨率 | 1449×1087 ～ 4016×6016 |
| 人工目录标签 | 人像、佛像或寺庙、城市夜景、天空、山景、鸟类、其他干扰项；每类 8 张 |
| 参与 retrieval 评测 | 前六类；“其他干扰项”仅作为负样本 |
| 模型 | `openclip:ViT-B-32-quickgelu:openai`，CPU |
| 中文查询 | 通过明确映射为对应英文 CLIP prompt；不宣称任意中文长句均被完整翻译 |
| Worker | `OMP_NUM_THREADS=2`、`MKL_NUM_THREADS=2`、batch=1 |
| 真值 | 每个标签文件夹内的 8 张照片；`object_key`/Asset ID 由相对路径确定性生成 |

指标定义：`Recall@K` 为每条 query 在 Top-K 中找回的相关图片数除以 8，再对六条 query 求平均；
`Query Hit Rate@K` 表示每条 query 是否至少命中一张相关图；`MRR@K` 依据第一张相关图的排名计算。
因此每类有 8 张相关图时，Top-5 的单类 Recall 上限为 `5/8=0.625`，不能把 0.625 误读为模型只能识别
62.5% 的单张图片。

### 6.2 索引结果

```text
56 / 56 Job done
embedding_model = openclip:ViT-B-32-quickgelu:openai
带 zero-shot 标签的 Asset = 51 / 56
带 Pillow quality/pHash 元数据的 Asset = 56 / 56
总索引耗时 = 161.72 s（包含模型加载与全部 JPEG 读取）
平均 = 2.89 s / 图
进程峰值 RSS = 1,501,756 KiB，约 1.43 GiB
```

这是 3.5 GiB 内存、无 GPU 测试机上的安全 CPU 配置；batch=4 尚未在本机尝试，不能因为代码支持就宣称更高
吞吐。

### 6.3 检索结果

| Top-K | Macro Recall@K | Query Hit Rate@K | MRR@K |
|---:|---:|---:|---:|
| 1 | 0.104 | 0.833 | 0.833 |
| 5 | 0.500 | 1.000 | 0.867 |
| 8 | 0.750 | 1.000 | 0.867 |

Top-5 的按类结果：

| Query | Top-5 相关图数 | Recall@5 | 首个相关图排名 | 观察 |
|---|---:|---:|---:|---|
| 人像 | 5/5 | 0.625 | 1 | Top-5 全为人像 |
| 佛像或寺庙 | 5/5 | 0.625 | 1 | Top-5 全为该标签 |
| 城市夜景 | 3/5 | 0.375 | 1 | 两张天空图被夜景/天空视觉相似性召回 |
| 天空 | 5/5 | 0.625 | 1 | Top-5 全为天空 |
| 山景 | 1/5 | 0.125 | 5 | 大面积天空导致四张天空图排在山景前，是本集最大失败样例 |
| 鸟类 | 5/5 | 0.625 | 1 | Top-5 全为鸟类 |

因此可得出的结论是：模型对人像、鸟类、佛像/寺庙等主体类别有明确正向信号；“山景”与“天空”需加入更细
prompt、rerank 或更多具有山体主体的样本后再评价。不能依据 56 张、单一摄影风格的小集推断通用视觉检索能力。

原始 SQLite、生成的 case 和 Top-5 JSON 位于本次临时评测目录：

```text
/tmp/minidriver-phase-c-20260821/{ai.sqlite3,cases.json,results-top5.json}
```

## 7. 下一步

1. 扩充受许可摄影评测集并增加人工 relevance 标注，比较 prompt、模型和 batch 的质量/耗时；
2. 将 RAW preview、视频 keyframe、OCR 做成独立有界派生 Job，而不是扩张当前图片 Worker；
3. 增加 Job lease、重试、DLQ 与索引新鲜度指标；
4. 数据量证明 SQLite 不足后，再迁移 PostgreSQL + pgvector，并测试过滤条件下的 Recall@K/P95。
