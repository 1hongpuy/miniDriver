# V2 下载速率显示设计

## 目标

在 V2 浏览器客户端下载期间，显示整个文件的已下载进度和平均下载速率，使用户能够
区分网络慢、下载仍在进行和下载失败。

本功能只修改 `www-v2/app.js`。不改变 Gateway API、DataNode `GET /v2/chunks/{hash}`
协议、Chunk SHA-256 校验、零拷贝服务端下载或 Blob fallback 行为。

## 现有数据流

```text
Browser -> Gateway object manifest
        -> DataNode direct Chunk GET
        -> browser verifies every Chunk hash
        -> File System Access API or Blob fallback saves the file
```

浏览器已知 manifest 的文件总大小与每个 Chunk 的预期大小，因此可以在读取 response
body 时精确累计收到的字节数。

## 设计

下载函数创建一个只属于当前下载的 progress reporter，状态为：

```text
startedAt
receivedBytes
totalBytes
lastRenderedAt
```

每当 `ReadableStream` 交付新的 body block：

1. 累加 `receivedBytes`。
2. 用 `receivedBytes / totalBytes` 得到整体百分比。
3. 用 `receivedBytes / (now - startedAt)` 得到整个文件的平均 MiB/s。
4. 节流更新下载按钮/状态文字，避免每个 64 KiB block 都造成 DOM 重绘。

进度不在每个 4 MiB Chunk 时重置。这样网络波动不会让界面显示的速率没有意义，也能
与 DataNode 的 `body_receive_ms` 日志按同一文件尺度对照。

## UI 状态

```text
空闲      下载
下载中    下载中 42% · 0.38 MiB/s
成功      已下载 · 平均 0.41 MiB/s
失败      下载失败：Chunk 3，HTTP 404
```

下载中的按钮禁用，防止同一对象被同一次点击重复下载。完成或失败后恢复可点击状态。

## 两条保存分支

`showSaveFilePicker` 可用时，读取到的 verified Chunk 直接顺序写入文件句柄；不可用时
保留当前 Blob fallback，在内存中收集已校验的 Chunk 并创建浏览器下载 URL。两条路径
都调用相同的 progress reporter，显示保持一致。

## 错误与范围

- Gateway manifest 失败、DataNode HTTP 错误、Chunk hash 不匹配均进入失败状态。
- 本轮不做断点下载、跨刷新恢复、取消下载、瞬时滑动窗口速率或多文件下载队列。
- 速率是浏览器从 DataNode 收到 body 的应用层平均速率，不包括用户在系统保存对话框停留
  的时间。

## 验证

1. 静态检查确认下载逻辑仍请求 object manifest、仍验证 Chunk hash、仍保留 Blob fallback。
2. 在浏览器下载一个多 Chunk 文件，确认进度从 0% 单调到 100%，速率非负且完成后显示平均值。
3. 让其中一个 Chunk 返回非 200，确认显示具体失败 Chunk，并恢复按钮状态。
