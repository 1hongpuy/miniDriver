# V4.3 SharedBodyBlock：有界共享 Body 与性能验证（2026-08-12）

## 结论

本轮完成了 SharedBodyBlock 的最小安全接入：一次 HTTP Body 进入 DataNode 后只复制进一个来自
固定 BlockPool 的 64 KiB `SharedBlock`；磁盘 worker 与副本 pipe 可以共同持有它。Block 只有在
磁盘写入和副本背压引用都释放后才归还池。

这不是“网络零拷贝”：TCP `send()` 仍需要把用户数据提交到内核发送缓冲；收益发生在副本尚未可写或
受背压时，不再额外构造 `pendingBlocks_` 的 `std::string` 副本。内存上限、pause/resume 和
磁盘队列语义保持不变。

真实双节点 `/data` 压测全部成功：upload c1 3/3、upload c2 6/6、mixed c2 6/6、mixed c4 12/12。
端到端 16 MiB 上传后下载 SHA-256 冒烟也通过。并发 2 upload P50 为 **234.878 ms / 68.12 MiB/s**，
较连接池阶段的 253.725 ms 有正向信号；单流和 mixed 结果有机器波动，不能单独归因于本次改动。

## 实现

```text
TCP input Buffer
  └─ memcpy 一次 → DiskWriteExecutor::SharedBlock（64 KiB，来自固定 128 块池）
                     ├─ DiskWriteExecutor worker → pwrite
                     └─ ReplicaUploadPipe
                         ├─ 可写：交给 TCP send
                         └─ 背压：仅保存 SharedBlock 引用，不复制为 string
```

- `DiskWriteExecutor::SharedBlock` 内部持有原 `BlockLease`，由 `shared_ptr` 引用计数。
- `ChunkDiskWritePipeline::push()` 可将同一块返回给调用者，同时以 shared block 投递磁盘 worker。
- `ReplicaUploadPipe::pushShared()` 在 pending 队列中持有块引用和长度；原 `push()` 保留给短连接回退及
  兼容调用方。
- BlockPool 耗尽时先返回 `kPauseBeforeConsume`，不会把同一段 Body 错误送入副本；恢复后由 HTTP 输入
  Buffer 重试该段数据。

## 验证

- 新增 `disk_write_executor` 场景：磁盘 worker 已完成但副本引用仍在时，Block 不会提前归还；最后引用
  释放后才恢复可用块数。
- `persistent_http_session`、`replica_connection_pool`、`disk_write_executor`、
  `chunk_disk_write_pipeline` 均通过。
- 完整 CTest：47/48 通过；唯一失败为缺失 Python `playwright` 的 UI 测试。

## 性能口径与结果

环境：8 vCPU、约 2.8 GiB RAM、`/data` ext4、同机 loopback；Gateway 1、DataNode 2、双副本，
每节点上传槽 2、`MINIKV_V4_IO_THREADS=2`，16 MiB，3 轮。

| 模式 | 并发 | 成功/请求 | 上传 P50 / P95 | 下载 P50 / P95 |
|---|---:|---:|---:|---:|
| upload-only | 1 | 3/3 | 318.748 / 381.582 ms；50.20 MiB/s | — |
| upload-only | 2 | 6/6 | 234.878 / 262.628 ms；68.12 MiB/s | — |
| mixed | 2（1U+1D） | 6/6 | 285.637 / 331.869 ms；56.02 MiB/s | 167.326 / 183.479 ms；95.62 MiB/s |
| mixed | 4（2U+2D） | 12/12 | 214.508 / 318.446 ms；74.59 MiB/s | 216.229 / 257.830 ms；74.00 MiB/s |

相同连接池阶段的 upload-only P50 为 c1 330.589 ms、c2 253.725 ms；本轮为 c1 318.748 ms、
c2 234.878 ms。样本很小且 loopback 受页缓存影响，报告仅记录可重复完成且无回归，不宣称严格的
因果收益。

原始 CSV、日志和集群状态目录：

```text
/data/minikv-v2/v4-shared-block-20260812/
```

## 下一步

下一步才评估 `Chunk window=2`：每个文件最多两个未完成 Chunk，并在提交前同时申请上传槽、
SharedBlock 容量、磁盘队列和副本连接配额。不得直接放开所有 Chunk 并发；要先以 P95/P99、
BlockPool 低水位、pause、RSS 和错误率决定是否保留该窗口。
