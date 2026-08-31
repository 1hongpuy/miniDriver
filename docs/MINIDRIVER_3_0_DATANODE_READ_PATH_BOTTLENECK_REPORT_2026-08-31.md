# MiniDriver 3.0：DataNode 下载路径瓶颈定位报告（2026-08-31）

> 状态：开发机单 DataNode、loopback、warm-cache 的服务端归因结果。它不是三主机 NIC/SSD 容量结论。

## 1. 目的与新增观测

此前 transport-only c16 的端到端 P95 仍明显高于 c8，但无法确认是 DataNode EventLoop、`sendfile`、socket backpressure、
元数据查找还是客户端共享 CPU 所致。

本轮为每个 `chunk_download_complete` 增加：

```text
handler_to_metadata_us     ChunkRecord / extent 查找
admission_us               下载准入锁与计数
response_prepare_us        设置文件响应前的本地准备
sendfile_calls             sendfile 调用次数
sendfile_syscall_us        所有 sendfile 系统调用自身耗时
sendfile_eagain            socket 不可写次数
sendfile_blocked_us        EAGAIN 后等待下一次可写事件的时间
sendfile_first_attempt_us  启动 sendfile 至第一次尝试的时间
total_ms                   DataNode 为该 Chunk 持有 DownloadLease 的全程时间
```

测量使用 transport-only + whole-chunk CRC32C、16 MiB 对象（4 x 4 MiB Chunk）、Keep-Alive、每 Worker 5 次、3 轮。
这避免了对象输出和 final SHA-256 掩盖服务端路径。

## 2. 服务端归因：默认 2 I/O EventLoop、256 KiB quantum

| DataNode Chunk 指标 | c8 | c16 |
|---|---:|---:|
| `total_ms` P95 | 6 ms | **33 ms** |
| metadata P95 | 10 us | 13 us |
| admission P95 | 1 us | 2 us |
| `sendfile_syscall_us` P95 | 2362 us | 3989 us |
| `sendfile_eagain` P95 | 0 | 0 |
| `sendfile_blocked_us` P95 | 0 | 0 |
| `sendfile_first_attempt_us` P95 | 3 us | 3 us |

结论：

1. **不是元数据查找或下载准入。** 两者都是微秒级。
2. **不是应用可见的 socket backpressure。** 所有记录的 `sendfile_eagain=0`，没有等待 TCP send buffer 释放。
3. c16 的服务端 Chunk P95 有约 33 ms，而实际 `sendfile` 系统调用累计 P95 仅约 4 ms。剩余时间是多连接在
   `sendFileQuantum=256 KiB` 之间由 EventLoop 轮转调度的等待，而不是单次 syscall 或磁盘读取本身。

因此此前“DataNode c8→c16 进入排队区”的说法现在可具体化为：**单 DataNode 的 sendfile quantum 公平轮转开始排队。**

## 3. 两个单变量调优实验

### 3.1 将 quantum 从 256 KiB 增至 1 MiB（2 I/O EventLoop）

| 指标 | 默认 256 KiB | 1 MiB |
|---|---:|---:|
| client transport-only P95 | 254.6 ms | 326.3 ms |
| aggregate 均值 | 1357.9 MiB/s | 1364.9 MiB/s |
| DataNode Chunk `total_ms` P95 | 33 ms | 44 ms |
| 每 Chunk sendfile calls 平均 | 约 18 | 约 7 |

较大 quantum 降低了调用次数，却让单条连接在 EventLoop 中占用更久，尾延迟变坏；不采用。

### 3.2 将 I/O EventLoop 从 2 增至 4（保持 256 KiB）

| 指标 | 2 loops | 4 loops |
|---|---:|---:|
| client transport-only P95 | 254.6 ms | 282.5 ms |
| aggregate 均值 | 1357.9 MiB/s | 1323.2 MiB/s |
| DataNode Chunk `total_ms` P95 | 33 ms | **21 ms** |
| DataNode sendfile syscall P95 | 3989 us | 2686 us |

4 loops 确实缩短了服务端单 Chunk 的 EventLoop 等待，但在同一台 8 vCPU VM 上，更多线程与 benchmark/Gateway 竞争，
最终用户可见 P95 和 aggregate 没有收益。因此它不能成为当前默认。

## 4. 当前处理决定

```text
默认保持：2 I/O EventLoop、256 KiB sendfile quantum、下载客户端 c8。
不默认使用：1 MiB quantum、4 I/O EventLoop。
```

已经解决的是“无法定位”的问题：DataNode 日志现在能直接区分 metadata、admission、sendfile syscall 与 socket backpressure。
尚未解决的是单 VM 下 c16 的用户可见尾延迟；它受服务端 EventLoop 时间片和同机客户端 CPU/loopback 竞争共同影响。

下一步应在真实三主机上重复 2 vs 4 I/O EventLoop：让 benchmark、Gateway、DataNode 分别占用独立主机/CPU，才能判断
服务端 4-loop 改善能否在没有同机竞争时转化为端到端收益。若仍无收益，则保留 2-loop；若收益稳定，再按 DataNode 核数
配置为 2 或 4，而不是一律增大线程。

## 5. 证据

```text
默认 2 loops / 256 KiB：
  /data-ssd/minidriver-v3-sdk-read-server-instrument-20260831/

2 loops / 1 MiB quantum：
  /data-ssd/minidriver-v3-sdk-read-server-quantum1m-20260831/

4 loops / 256 KiB：
  /data-ssd/minidriver-v3-sdk-read-server-iothreads4-20260831/
```
