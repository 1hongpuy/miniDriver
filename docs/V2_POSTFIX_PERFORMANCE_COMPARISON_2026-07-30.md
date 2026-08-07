# MiniKVCine V2 修复前后本机性能对比

> 日期：2026-07-30
> 对比对象：2026-07-29 的旧 V2 数据面基线，与提交 `a9e46ba` 后的 V2 修复版。
> 结论口径：只比较同机 loopback、两个 DataNode、双副本链、4 MiB Chunk 的结果；不把
> Tailscale、浏览器、Nginx 或不同机器的网络带宽混入本报告。

## 1. 两份报告与原始产物

| 名称 | 内容 | 位置 |
|---|---|---|
| 修复前完整报告 | 64 MiB/1 GiB、1/2/4 并发与历史火焰图分析 | [V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md](V2_LOCAL_PERFORMANCE_REPORT_2026-07-29.md) |
| 修复前结论 | 控制面、混合读写、已知 Placement 错误 | [V2_FINAL_PERFORMANCE_ASSESSMENT_2026-07-29.md](V2_FINAL_PERFORMANCE_ASSESSMENT_2026-07-29.md) |
| 修复后原始 CSV | 本次 64 MiB 矩阵与 1 GiB 失败样本 | `/tmp/minikv-v2-postfix-matrix-20260730-164927/` |
| 修复后代码与测试说明 | lease、admission、manifest snapshot、extent/sendfile 的修复范围 | [V2_FIX_PLAN_BEFORE_PERFORMANCE_REWORK.md](V2_FIX_PLAN_BEFORE_PERFORMANCE_REWORK.md) |

本次在 `build-perf`、`RelWithDebInfo` 下执行，测试前 `ctest` 为 **12/12 通过**。隔离集群为：

```text
Gateway:      127.0.0.1:18181
DataNode A:   127.0.0.1:19102
DataNode C:   127.0.0.1:19103
副本策略:     A -> C
Chunk:        4 MiB
```

## 2. 先说明可比性限制

这不是严格的单变量 A/B 实验，原因有两个：

1. 旧版 CMake 会强制 `Debug`，旧报告里即使命令写了 `RelWithDebInfo`，实际仍缺少 `-O2`。
   本次修正了构建逻辑，因而新旧速度差同时包含“正常优化构建”和“代码修复”的影响。
2. 修复后 Gateway 使用写入租约，DataNode 使用本地两个写槽的 admission control。旧版会在
   心跳滞后窗口内超额签发；因此高并发的“全部成功”不再是同一种语义。

因此，1/2 并发可用来观察修复后可用吞吐；4 并发主要用于验证容量保护，不应用它声称吞吐
线性提升。

## 3. 64 MiB 文件对比

### 3.1 单文件阶段指标

| 文件并发 | 版本 | 成功数 | 上传中位数 | 上传吞吐 | 下载中位数 | 下载吞吐 |
|---:|---|---:|---:|---:|---:|---:|
| 1 | 修复前 | 3/3 | 230.519 ms | 277.6 MiB/s | 360.898 ms | 177.3 MiB/s |
| 1 | 修复后 | 3/3 | 163.271 ms | 392.0 MiB/s | 373.319 ms | 171.4 MiB/s |
| 2 | 修复前 | 6/6 | 316.397 ms | 202.3 MiB/s/文件 | 553.823 ms | 115.6 MiB/s/文件 |
| 2 | 修复后 | 6/6 | 240.174 ms | 266.5 MiB/s/文件 | 390.883 ms | 163.7 MiB/s/文件 |
| 4 | 修复前 | 12/12 | 610.282 ms | 104.9 MiB/s/文件 | 987.919 ms | 64.8 MiB/s/文件 |
| 4 | 修复后 | 6/12 | 261.001 ms* | 245.2 MiB/s/文件* | 508.574 ms* | 125.8 MiB/s/文件* |

`*` 修复后 4 并发有 6 个文件在 `routes` 阶段收到预期的 `503 no DataNode write capacity available`。
这里的中位数只对 6 个成功文件计算，不能与旧版 12 个超额并发成功文件作容量结论。

### 3.2 1/2 并发的变化

| 指标 | 修复前 | 修复后 | 变化 |
|---|---:|---:|---:|
| 1 并发上传吞吐 | 277.6 MiB/s | 392.0 MiB/s | +41.2% |
| 1 并发下载吞吐 | 177.3 MiB/s | 171.4 MiB/s | -3.3% |
| 2 并发每文件上传吞吐 | 202.3 MiB/s | 266.5 MiB/s | +31.7% |
| 2 并发每文件下载吞吐 | 115.6 MiB/s | 163.7 MiB/s | +41.6% |
| 1 并发端到端聚合吞吐中位数 | 81.08 MiB/s | 90.92 MiB/s | +12.1% |
| 2 并发端到端聚合吞吐中位数 | 117.38 MiB/s | 154.03 MiB/s | +31.2% |

修复后 2 并发的三轮端到端聚合吞吐为 `187.99 / 154.03 / 139.28 MiB/s`；仍有波动，不能写为
“稳定 188 MiB/s”。相较旧版 `119.06 / 117.38 / 105.97 MiB/s`，整体上升明显，但本机页缓存、
CPU 频率和 Debug -> O2 构建变化都会影响该数值。

## 4. 容量保护的行为变化

### 修复前

旧版使用每 8 秒一次的 `activeUploads` 心跳作为硬门槛。它既不能在瞬时并发时阻止超额路由，
也会在写入已经结束后因旧心跳误拒绝新路由。历史上出现过：第一轮 8 并发完成、下一轮所有
routes 都被拒绝的情况。

### 修复后

Gateway 在签发 route 时立即占用 `WriteLease`，DataNode 在接收 HTTP Body 时再执行本地
`WriteAdmission`。当前每个节点固定 2 个写槽；双副本链上的一个 Chunk 会占用两端各一个槽。

因此，两节点双副本链的稳定并行度是 **2 个正在落盘的 Chunk**。本次 4 文件并发出现：

```text
12 个文件尝试
6 个端到端成功
6 个在 routes 阶段返回 HTTP 503 + Retry-After
0 个静默卡死、0 个元数据伪成功
```

这不是吞吐回退，而是把旧版“偶然超额执行或心跳误判”的错误改成可预测、可重试的资源边界。
当前 benchmark 尚未实现收到 `Retry-After` 后的排队重试，所以 4 并发测试不是队列吞吐测试。

## 5. 1 GiB 长文件结果

修复前已有一条有效结果：

```text
1 GiB, 1 并发: 上传 153.6 MiB/s，下载 108.6 MiB/s，端到端 51.71 MiB/s。
```

本次 1 GiB 上传已完成 256 个 Chunk 并完成 Gateway commit；下载阶段失败：

```text
FAILED: cannot write downloaded chunk
/tmp 所在文件系统：40 GiB 总量，测试时 100% 已用，剩余 0 B。
```

隔离集群临时副本数据约占 3.4 GiB，本轮 benchmark 输出约占 2.0 GiB。失败原因是测试盘空间耗尽，
不是 DataNode GET、manifest 或 SHA-256 校验错误。该轮不产生 1 GiB 性能数据，也不参与任何
吞吐比较。重测前至少为 `/tmp` 保留 **6 GiB** 可用空间：输入、下载输出及两个副本数据会同时存在。

## 6. 本次能确认的结论

1. 修复后 1/2 文件并发均 100% 成功，端到端吞吐高于旧基线。
2. `WriteLease + WriteAdmission` 消除了“由过期 heartbeat 决定是否可写”的核心错误；过载现在
   返回明确 `503`，客户端可按 `Retry-After` 重试。
3. 修复后的 extent + `sendfile` 下载路径已经通过单元测试和端到端 64 MiB 下载校验；本次下载
   速度仍受单 EventLoop、磁盘/页缓存和客户端整文件 SHA-256 校验影响。
4. V2 仍不是高并发存储。单 EventLoop 仍同步做 hash、`pwrite` 和副本状态转换；客户端同一文件
   的 Chunk window 固定为 1。

## 7. 下一次正式基准的执行条件

1. 清理隔离压测数据后，确认 `df -h /tmp` 至少有 6 GiB 空闲。
2. 重跑 1 GiB，至少 3 次，才讨论 P95/P99。
3. 为 benchmark 增加 `503 Retry-After` 排队重试后，才测试 4/8 文件并发的稳定吞吐。
4. 在私网机器上复用同一命令矩阵，单独形成网络基线；不得将 loopback 数字当成 Tailscale 吞吐。
5. 补充 EventLoop lag、pause 次数/时长与 outputBuffer peak 后，再判断背压是否真正有效。
