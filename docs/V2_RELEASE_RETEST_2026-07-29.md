# V2 Release 重新基线测试

> 日期：2026-07-29
>
> 目的：验证 CMake 不再强制 Debug 后，重新采集 V2 本机 loopback 性能。
> 本次只改变构建配置，不改变 Gateway、DataNode、协议或压测客户端逻辑。

## 1. 构建修复

原来 [CMakeLists.txt](/home/ubuntu/miniKV/v1.0/miniKVCine/CMakeLists.txt:5) 无条件执行：

```cmake
set(CMAKE_BUILD_TYPE "Debug")
```

所以即使调用：

```bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=RelWithDebInfo
```

实际 `flags.make` 仍只有：

```text
-g -fno-omit-frame-pointer -g
```

修复后，未显式指定构建类型时默认使用 `RelWithDebInfo`，但显式 `Debug`、`Release`
和 `RelWithDebInfo` 都会被保留。最终本次 `build-perf` 的真实编译参数为：

```text
-O2 -g -DNDEBUG -fno-omit-frame-pointer -std=gnu++17
```

## 2. 测试环境与命令

隔离集群全部绑定 loopback：

```text
Gateway:  127.0.0.1:18181
DataNode: 127.0.0.1:19102
DataNode: 127.0.0.1:19103
```

每个文件是 16 MiB（4 个 4 MiB Chunk），每个并发档运行 3 轮：

```bash
./build-perf/bin/minikv_v2_bench local \
  --gateway 127.0.0.1:18181 \
  --work-dir /tmp/minikv-v2-release-matrix-20260729-222224/cN \
  --sizes 16MiB --runs 3 --concurrency N --remote-dir /benchmark
```

原始 CSV 位于：

```text
/tmp/minikv-v2-release-matrix-20260729-222224/c1
/tmp/minikv-v2-release-matrix-20260729-222224/c2
/tmp/minikv-v2-release-matrix-20260729-222224/c4
/tmp/minikv-v2-release-matrix-20260729-222224/c8
```

隔离进程已停止。数据保留在 `/tmp`，以便复查。

## 3. 结果

这里的上传和下载延迟是单文件端到端时间；吞吐由完整 16 MiB 文件大小计算。

| 文件并发 | 成功数 | 上传 P50 | 上传 P50 吞吐 | 下载 P50 | 下载 P50 吞吐 | 每轮聚合端到端吞吐 |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 / 3 | 55.604 ms | 287.75 MiB/s | 96.086 ms | 166.52 MiB/s | 73.87, 79.72, 80.31 MiB/s |
| 2 | 6 / 6 | 83.231 ms | 192.24 MiB/s | 131.184 ms | 121.97 MiB/s | 110.62, 121.67, 116.45 MiB/s |
| 4 | 12 / 12 | 184.153 ms | 86.88 MiB/s | 204.828 ms | 78.11 MiB/s | 121.05, 141.92, 132.74 MiB/s |
| 8 | 19 / 24 | 376.655 ms | 42.48 MiB/s | 280.650 ms | 57.01 MiB/s | 150.88, 139.05, 第三轮失败 |

### 对旧 Debug 基线的解释

旧 16 MiB、单文件上传中位数为 `60.55 ms`；本次为 `55.60 ms`，约改善 8%。下载
由 `92.76 ms` 变为 `96.09 ms`，该量级的差异可能受页缓存和机器瞬时负载影响，不能仅凭
三轮认定回归。

因此 CMake 修复的结论是：此前数据确实不是 Release 数据；但对该小文件、loopback、
缓存较强的基准，优化带来的可见增益是温和的，不能夸大为数量级提升。

## 4. 并发 8 失败不是性能饱和，而是正确性缺陷

第三轮有 5 个文件在最后一个 Chunk 申请路由时失败：

```text
Gateway POST /api/v2/upload/sessions/.../routes returned HTTP 400:
{"error":"no route available or invalid route request"}
```

这与 [V2_FIX_PLAN_BEFORE_PERFORMANCE_REWORK.md](/home/ubuntu/miniKV/v1.0/miniKVCine/docs/V2_FIX_PLAN_BEFORE_PERFORMANCE_REWORK.md)
中的 Fix 1 一致：Gateway 用滞后心跳 `activeUploads` 作为严格写槽限制，而不是在发
route 时做 lease reservation；两台节点都可能被短暂过滤掉。

因此：

- 并发 8 的成功文件吞吐可作观察样本；
- 不能把它作为 V2 的稳定性能能力；
- 下一项必须实施 Gateway 写槽租约与 DataNode 本地 admission control。

## 5. 回归验证

使用新 `build-perf` 二进制执行，全部通过：

```text
PASS: test_async_http_timeout
PASS: test_fast_data_store_read
PASS: test_http_stream_context
PASS: test_benchmark_types
PASS: test_benchmark_input
PASS: test_benchmark_http
PASS: test_benchmark_cli
```

## 6. 本轮结论

1. CMake 构建类型问题已经被修正并由实际编译参数验证。
2. 新基准应取代此前 Debug 基线作为后续优化对照，但需在修复路由写槽语义后再跑一次
   完整并发矩阵。
3. 当前推荐运行上限仍是文件并发 `1--2`、每文件 Chunk window `1`。
4. 下一个代码阶段是 Fix 1，而不是多 EventLoop 或零拷贝重构。
