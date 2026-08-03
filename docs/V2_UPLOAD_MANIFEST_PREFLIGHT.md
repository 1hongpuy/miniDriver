# V2 上传预检与内容复用

## 目标

在照片字节发送到 DataNode 前，让 Gateway 判断三个事实：

1. 目标逻辑路径是否已经存在对象。
2. 整个有序文件内容是否已存在。
3. 若文件内容尚不存在，哪些 Chunk 已经存在可复用。

这避免旧流程先传完几十 GiB 后才在 `commitFile()` 发现路径冲突，也避免重新上传相同素材。

## 内容身份

`manifestHash` 是以下 ASCII 文本的 SHA-256：

```text
minikv-manifest-v1\n
<fileSize>\n
<chunkSize>\n
<index>:<chunkHash>:<chunkSize>\n
...
```

它包含大小、固定 Chunk 大小、顺序、每块 hash 和每块大小。因此相同字节但不同切片规则不会误判为同一内容；不同目录和文件名不会改变内容身份。

## 请求流程

```text
Browser
  -> 固定 4 MiB 顺序读取，计算每个 chunkHash
  -> 计算 manifestHash
  -> POST /api/v2/upload/preflight

Gateway
  -> path:{owner,path,name} 已存在: 409 PATH_CONFLICT
  -> f:{manifestHash} 已存在: 新建 obj/path 引用，CONTENT_EXISTS
  -> 否则查询 c:{chunkHash}
       已有至少一个真实副本: 写入 Session.completed，跳过该 Chunk
       不存在: 返回 missingIndices，浏览器请求路由后上传
```

`UPLOAD_REQUIRED` 会持久化 `s:{sessionId}`。其中保存 `manifestHash` 和已复用/已确认的 Chunk，刷新浏览器后只有相同 manifest 的本地 Session 才会续传。

## 元数据边界

- `f:{manifestHash}`：一份内容的 Chunk 顺序与文件属性。
- `c:{chunkHash}`：该 Chunk 已被确认的 DataNode 副本。
- `obj:{objectId}` 和 `path:{owner,path}`：目录中可见的逻辑文件。
- `s:{sessionId}`：未完成上传的临时账本。

因此 `copy.NEF` 放到 `/archive` 并不复制 Chunk 字节，只新增一个 ObjectMeta/path 映射；`/shoots/source.NEF` 已存在则在预检中失败，前端不会开始数据传输。

## 兼容性

旧 `POST /api/v2/upload/sessions` 仍保留。旧会话没有 `manifestHash`，在 commit 时继续使用原有的 legacy hash 规则，避免已有断点续传 Session 失效。新浏览器上传走 `/upload/preflight`。

## 验证

```bash
cmake --build build --target test_gateway_upload_preflight -j2
./build/bin/test_gateway_upload_preflight
```

测试覆盖：全新内容、完整内容复用、局部 Chunk 复用和同路径冲突。
