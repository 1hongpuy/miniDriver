# V2 E4 本机数据面压测报告（2026-08-06）

## 1. 测试目标

本轮验证 V2 当前 DataNode/Gateway 数据面的容量边界，作为 Phase E 的
端到端基线。测试不经过浏览器和 Nginx，避免把浏览器调度、Tailscale
网络或公网带宽混入数据面结果。

测试链路为：

```text
benchmark client
  -> Gateway Session / route
  -> Primary DataNode Chunk PUT
  -> Replica DataNode
  -> Gateway Chunk commit
  -> Gateway manifest
  -> DataNode Chunk GET
  -> 客户端 Chunk SHA-256 + 文件 SHA-256 校验
```

## 2. 环境与方法

每个并发档位都使用全新的隔离集群和数据目录：

```text
Gateway:     127.0.0.1:<port>
DataNode A:  127.0.0.1:<port>
DataNode C:  127.0.0.1:<port>
对象大小:    16 MiB = 4 个 4 MiB Chunk
每档轮数:    3
副本数:      2
```

每个对象成功后都会下载并完成完整 SHA-256 校验。内容寻址的旧 Chunk 不会
污染下一档结果。

当前命令行 benchmark 的 Chunk 发送仍是串行的；因此本报告可以证明
DataNode 数据面和“多个文件并发”行为，但不能证明浏览器 E1 双 Chunk 窗口
的真实收益。浏览器测试仍需单独的真实集群 E2E 场景补测。

## 3. 结果

### C1：1 个文件并发

```text
成功: 3 / 3
上传 P50: 58.895 ms
上传 P95/P99: 60.883 / 60.883 ms
下载 P50: 50.796 ms
下载 P95/P99: 51.262 / 51.262 ms
```

按 16 MiB 对象折算，中位数约为：

```text
上传: 271.7 MiB/s
下载: 315.0 MiB/s
```

### C2：2 个文件并发

```text
成功: 6 / 6
上传 P50: 107.683 ms
上传 P95/P99: 125.437 / 125.437 ms
下载 P50: 55.693 ms
下载 P95/P99: 60.974 / 60.974 ms
```

按单文件统计，中位数约为：

```text
上传: 148.6 MiB/s
下载: 287.4 MiB/s
```

两条并发流均成功，说明当前固定写入准入可以吸收两条活动上传，而没有
产生数据校验错误。

### C4：4 个文件并发

```text
成功: 6 / 12
失败: 6 / 12
成功上传 P50: 112.705 ms
成功上传 P95/P99: 136.692 / 136.692 ms
成功下载 P50: 58.425 ms
成功下载 P95/P99: 71.684 / 71.684 ms
```

失败原因全部是：

```json
{"error":"no DataNode write capacity available"}
```

也就是 Gateway 在 route 阶段返回 HTTP 503，而不是 Chunk 数据损坏、Hash
错误、复制错误或连接崩溃。成功样本的上传中位数约为 142.2 MiB/s，下载
中位数约为 287.4 MiB/s。

## 4. 结论

当前 V2 的硬边界已经被重复测出：

```text
1 文件: 可靠完成
2 文件: 可靠完成
4 文件: 约一半请求在 Gateway 路由阶段被容量保护拒绝
```

这说明 `maxConcurrentWrites=2` 与双副本链共同形成了当前有效写入容量。
HTTP 503 是保护机制的预期结果，避免继续接收后造成无界内存增长；但当前
命令行 benchmark 不会自动等待并重试，因此它把容量等待记录为失败。

这也解释了浏览器此前的现象：如果浏览器同时启动过多文件，部分文件会看到
`no DataNode write capacity available`。这不应通过简单提高 DataNode 并发数
解决，必须同时观察磁盘队列、写入时延和失败率。

## 5. E4 尚未覆盖的内容

本轮已经覆盖：

- 1/2/4 文件并发；
- 上传、下载吞吐；
- P50/P95/P99 完成时间；
- 双副本完整校验；
- Gateway 容量拒绝边界。

本轮尚未覆盖：

- 浏览器真实双 Chunk 窗口的吞吐；
- 503 `Retry-After` 后的自动恢复完成率；
- 刷新页面后 Session 续传；
- DataNode EventLoop 延迟、磁盘队列峰值的聚合报告；
- perf 火焰图。

## 6. 下一步

E4 的下一轮应增加真实浏览器 E2E：

1. 启动本机隔离 Gateway 和两个 DataNode；
2. 用 HTTP/HTTPS 页面加载 `www-v2`；
3. 上传一个至少 16 MiB 的文件；
4. 记录同时活动的 Chunk PUT 数、总耗时和最终文件状态；
5. 人为让一个 Chunk 返回 503，验证释放槽位、退避和最终恢复；
6. 中断后重新选择相同文件，验证只发送未完成 Chunk。

只有这轮通过后，才能把浏览器 E1 的窗口收益和当前命令行数据面基线放在
同一份对比报告中。
