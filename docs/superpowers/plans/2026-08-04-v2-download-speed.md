# V2 Download Speed Display Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Display file-level browser download progress and average MiB/s for both File System Access and Blob fallback downloads.

**Architecture:** Keep `downloadFile()` as the orchestration point. Replace whole-response `blob()` reads with one shared stream reader that reports delivered bytes before returning a verified Blob. A small DOM state helper owns button disable/restore and status text, while the existing manifest, replica selection, SHA-256 validation and save branches remain unchanged.

**Tech Stack:** Browser Fetch/ReadableStream, existing vanilla JavaScript UI, shell static checks.

## Global Constraints

- Modify only `www-v2/app.js` and its focused shell test.
- Do not change Gateway/DataNode APIs or Chunk SHA-256 validation.
- Progress uses whole-file received bytes divided by manifest `fileSize`; speed is whole-file average bytes divided by elapsed monotonic time.
- Preserve the existing `showSaveFilePicker` and Blob fallback paths.
- Status DOM updates are throttled to at most once per 100 ms.

---

### Task 1: Establish a static regression check for streamed download reporting

**Files:**
- Modify: `test/test_v2_blob_download_fallback.sh`
- Test: `test/test_v2_blob_download_fallback.sh`

**Interfaces:**
- Consumes: `www-v2/app.js`.
- Produces: a static check that requires `response.body.getReader()`, the progress callback argument and the existing Blob fallback.

- [ ] **Step 1: Write the failing test**

Append these checks before the final PASS line:

```bash
if ! grep -Fq 'response.body.getReader()' "${app_js}"; then
    echo "FAIL: Chunk download does not consume the response body incrementally" >&2
    exit 1
fi

if ! grep -Fq 'onBytes(value.byteLength)' "${app_js}"; then
    echo "FAIL: Chunk download does not report delivered body bytes" >&2
    exit 1
fi

if ! grep -Fq '下载中 ${percent}% · ${formatRate(rate)}' "${app_js}"; then
    echo "FAIL: Download UI does not render whole-file speed" >&2
    exit 1
fi
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
bash test/test_v2_blob_download_fallback.sh
```

Expected: failure stating `Chunk download does not consume the response body incrementally`.

- [ ] **Step 3: Commit the failing test**

```bash
git add test/test_v2_blob_download_fallback.sh
git commit -m "test: require streamed V2 download progress"
```

### Task 2: Stream a verified Chunk while reporting delivered bytes

**Files:**
- Modify: `www-v2/app.js:322-330`
- Test: `test/test_v2_blob_download_fallback.sh`

**Interfaces:**
- Consumes: `chunk.index`, `chunk.hash`, `chunk.replicas` and optional `onBytes(number)`.
- Produces: `fetchVerifiedChunk(chunk, onBytes) -> Promise<Blob>`.

- [ ] **Step 1: Replace whole-body Blob reading with a streamed reader**

Replace the function with:

```js
async function fetchVerifiedChunk(chunk, onBytes = () => {}) {
  const replica = chunk.replicas && chunk.replicas[0];
  if (!replica) throw new Error(`Chunk ${chunk.index} 没有可读副本`);
  const response = await fetch(`http://${replica.address}:${replica.httpPort}/v2/chunks/${chunk.hash}`);
  if (!response.ok) throw new Error(`Chunk ${chunk.index} 下载失败: HTTP ${response.status}`);
  if (!response.body) throw new Error(`Chunk ${chunk.index} 下载响应没有 body stream`);

  const reader = response.body.getReader();
  const blocks = [];
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      blocks.push(value);
      onBytes(value.byteLength);
    }
  } finally {
    reader.releaseLock();
  }
  const blob = new Blob(blocks, { type: "application/octet-stream" });
  if (await sha256(blob) !== chunk.hash) throw new Error(`Chunk ${chunk.index} SHA-256 校验失败`);
  return blob;
}
```

- [ ] **Step 2: Run the focused test to verify it passes**

Run:

```bash
bash test/test_v2_blob_download_fallback.sh
```

Expected: it still fails only on the missing download UI speed text.

- [ ] **Step 3: Commit the streamed Chunk implementation**

```bash
git add www-v2/app.js test/test_v2_blob_download_fallback.sh
git commit -m "feat: stream V2 chunk download progress"
```

### Task 3: Render download progress and whole-file average rate

**Files:**
- Modify: `www-v2/app.js:348-367` and the file-row download click handler near `www-v2/app.js:443`
- Test: `test/test_v2_blob_download_fallback.sh`

**Interfaces:**
- Consumes: catalog row download button, `manifest.fileSize`, `manifest.chunks`, `fetchVerifiedChunk(chunk, onBytes)`.
- Produces: `downloadFile(item, button)` that disables the active button while downloading and restores it on success or failure.

- [ ] **Step 1: Add a download status renderer**

Immediately before `downloadFile`, add:

```js
function createDownloadProgress(button, totalBytes) {
  const startedAt = performance.now();
  let receivedBytes = 0;
  let lastRenderedAt = 0;
  const render = (force = false) => {
    const now = performance.now();
    if (!force && now - lastRenderedAt < 100) return;
    lastRenderedAt = now;
    const percent = Math.min(100, Math.floor((receivedBytes / totalBytes) * 100));
    const elapsedSeconds = Math.max(0.001, (now - startedAt) / 1000);
    const rate = receivedBytes / elapsedSeconds;
    button.textContent = `下载中 ${percent}% · ${formatRate(rate) || "计算中"}`;
  };
  button.disabled = true;
  render(true);
  return {
    onBytes: (count) => { receivedBytes += count; render(false); },
    complete: () => {
      const elapsedSeconds = Math.max(0.001, (performance.now() - startedAt) / 1000);
      button.textContent = `已下载 · 平均 ${formatRate(receivedBytes / elapsedSeconds)}`;
      button.disabled = false;
    },
    fail: (error) => {
      button.textContent = `下载失败：${error.message}`;
      button.disabled = false;
    },
  };
}
```

- [ ] **Step 2: Make `downloadFile` use the reporter in both save branches**

Replace the current body with:

```js
async function downloadFile(item, button) {
  const manifest = await request(`/objects/${item.objectId}/manifest`, { method: "GET" });
  const progress = createDownloadProgress(button, manifest.fileSize);
  const fileName = downloadName(item);
  try {
    if (window.isSecureContext && window.showSaveFilePicker) {
      const handle = await window.showSaveFilePicker({ suggestedName: fileName });
      const writable = await handle.createWritable();
      try {
        for (const chunk of manifest.chunks) await writable.write(await fetchVerifiedChunk(chunk, progress.onBytes));
        await writable.close();
      } catch (error) {
        await writable.abort();
        throw error;
      }
    } else {
      const chunkBlobs = [];
      for (const chunk of manifest.chunks) chunkBlobs.push(await fetchVerifiedChunk(chunk, progress.onBytes));
      saveBlob(fileName, chunkBlobs);
    }
    progress.complete();
  } catch (error) {
    progress.fail(error);
    throw error;
  }
}
```

Change the file-row event handler to pass its button:

```js
downloadButton.addEventListener("click", async () => {
  try { await downloadFile(file, downloadButton); }
  catch (_) { /* createDownloadProgress renders the failure state */ }
});
```

- [ ] **Step 3: Run focused test to verify it passes**

Run:

```bash
bash test/test_v2_blob_download_fallback.sh
```

Expected: `PASS: V2 frontend has a Blob download fallback`.

- [ ] **Step 4: Manually verify in browser**

Download a multi-Chunk object through the V2 page. Expected:

```text
下载中 0% · 计算中
下载中 42% · 0.38 MiB/s
已下载 · 平均 0.41 MiB/s
```

Make one DataNode Chunk unavailable. Expected: the same button shows the specific `Chunk N 下载失败` error and becomes clickable again.

- [ ] **Step 5: Commit the UI change**

```bash
git add www-v2/app.js test/test_v2_blob_download_fallback.sh
git commit -m "feat: show V2 browser download speed"
```

## Plan Self-Review

- Spec coverage: Tasks 2 and 3 preserve both save branches, Chunk validation and failure behavior; Task 1 supplies regression checks.
- Placeholder scan: no TBD/TODO steps remain.
- Interface consistency: Task 2 provides the optional callback used by Task 3; Task 3 passes the active DOM button into `downloadFile`.
