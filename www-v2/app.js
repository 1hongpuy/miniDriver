(() => {
  "use strict";

  const API = "/api/v2";
  // AI-App-Lite is intentionally independent from the Gateway data plane. Override
  // this in a deployment with `window.MINIDRIVE_AI_API = "https://..."` before app.js.
  const AI_API = (window.MINIDRIVE_AI_API || `${window.location.protocol}//${window.location.hostname}:18290`).replace(/\/$/, "");
  const SESSION_PREFIX = "minikv-v2:session:";
  const UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024;
  const MAX_PARALLEL_FILES = 1;
  const MAX_INFLIGHT_CHUNKS_PER_FILE = 2;
  const MAX_ACTIVE_CHUNK_UPLOADS = 2;
  const MAX_CAPACITY_RETRIES = 120;
  const nodeStates = ["RECOVERING", "ONLINE", "SUSPECT", "OFFLINE", "DRAINING"];
  const $ = (selector) => document.querySelector(selector);
  const fileInput = $("#file-input");
  const uploadForm = $("#upload-form");
  const activeDirectoryPath = $("#active-directory-path");
  const startButton = $("#start-upload");
  const selectionSummary = $("#selection-summary");
  const queue = $("#transfer-queue");
  const queueCount = $("#queue-count");
  const nodeList = $("#node-list");
  const clusterState = $("#cluster-state");
  const clusterDetail = $("#cluster-detail");
  const directoryTree = $("#directory-tree");
  const breadcrumbs = $("#breadcrumbs");
  const catalogEntries = $("#catalog-entries");
  const objectDetail = $("#object-detail");
  const deleteDirectoryButton = $("#delete-directory");
  const preview = $("#object-preview");
  const previewTitle = $("#preview-title");
  const previewMeta = $("#preview-meta");
  const previewStage = $("#preview-stage");
  const previewStatus = $("#preview-status");
  const previewDownload = $("#preview-download");
  const aiSearchForm = $("#ai-search-form");
  const aiSearchInput = $("#ai-search-input");
  const aiSearchSubmit = $("#ai-search-submit");
  const aiSearchState = $("#ai-search-state");
  const aiSearchHint = $("#ai-search-hint");
  const aiSearchResults = $("#ai-search-results");
  const entries = new Map();
  const catalogState = {
    activeDirectory: "/", catalog: null, selectedObject: null,
    requestId: 0, renderId: 0, thumbnailUrls: new Map(),
  };
  const previewState = { object: null, url: null, scale: 1, image: null, requestId: 0 };
  const aiSearch = {
    activeTags: new Set(), requestId: 0, results: [], query: "", nextCursor: null,
    totalCandidates: 0, loadingMore: false,
  };
  let mediaRefreshTimer = null;

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes)) return "未知容量";
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let value = Math.max(0, bytes); let unit = 0;
    while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit += 1; }
    return `${value >= 10 || unit === 0 ? value.toFixed(0) : value.toFixed(1)} ${units[unit]}`;
  }

  function formatDuration(seconds) {
    if (!Number.isFinite(seconds) || seconds < 1) return "不足 1 秒";
    if (seconds < 60) return `${Math.round(seconds)} 秒`;
    return `${Math.floor(seconds / 60)} 分 ${Math.round(seconds % 60)} 秒`;
  }

  function formatRate(bytesPerSecond) {
    return Number.isFinite(bytesPerSecond) && bytesPerSecond > 0 ? `${formatBytes(bytesPerSecond)}/s` : "";
  }

  function fingerprint(file, dir) { return `${file.name}:${file.size}:${file.lastModified}:${dir}`; }
  function sessionKey(file, dir) { return `${SESSION_PREFIX}${fingerprint(file, dir)}`; }

  async function request(path, options = {}) {
    const response = await fetch(`${API}${path}`, {
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    });
    const text = await response.text();
    let body = {};
    try { body = text ? JSON.parse(text) : {}; } catch (_) { body = { error: text }; }
    if (!response.ok) {
      const error = new Error(body.error || `Gateway returned HTTP ${response.status}`);
      error.status = response.status;
      error.retryAfter = response.headers.get("Retry-After");
      throw error;
    }
    return body;
  }

  async function aiRequest(path, options = {}) {
    const response = await fetch(`${AI_API}${path}`, {
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    });
    const text = await response.text();
    let body = {};
    try { body = text ? JSON.parse(text) : {}; } catch (_) { body = { error: text }; }
    if (!response.ok) throw new Error(body.error || `AI 服务返回 HTTP ${response.status}`);
    return body;
  }

  function setAiSearchState(label, state = "idle") {
    aiSearchState.textContent = label;
    aiSearchState.dataset.state = state;
  }

  function updateAiQueryFromTags() {
    const tags = [...aiSearch.activeTags];
    if (!tags.length) return;
    const current = aiSearchInput.value.trim();
    const words = new Set(current ? current.split(/\s+/) : []);
    tags.forEach((tag) => words.add(tag));
    aiSearchInput.value = [...words].join(" ");
  }

  function relatedCatalogFile(result) {
    const assetId = String(result.asset_id || result.metadata?.object_id || "");
    return (catalogState.catalog?.files || []).find((file) => String(file.objectId) === assetId) || null;
  }

  function metadataSummary(metadata = {}) {
    const candidates = [metadata.scene, metadata.camera_model, metadata.capture_time, metadata.dir_path]
      .filter((value) => value !== undefined && value !== null && String(value).trim());
    return candidates.length ? candidates.join(" · ") : "已建立向量索引";
  }

  function renderAiSearchResults(results) {
    aiSearchResults.replaceChildren();
    if (!results.length) {
      aiSearchResults.append(emptyState("没有匹配的已索引素材；可换一个描述或先完成素材索引。"));
      return;
    }
    const grid = document.createElement("div");
    grid.className = "ai-result-grid";
    for (const result of results) {
      const card = document.createElement("article");
      card.className = "ai-result";
      const title = document.createElement("strong");
      title.textContent = result.object_key || result.asset_id || "未命名素材";
      const detail = document.createElement("span");
      detail.textContent = metadataSummary(result.metadata);
      const score = document.createElement("span");
      score.className = "ai-result__score";
      score.textContent = `相似度 ${(Number(result.score || 0) * 100).toFixed(1)}%`;
      const file = relatedCatalogFile(result);
      if (file) {
        const open = document.createElement("button");
        open.type = "button";
        open.className = "catalog-row__command";
        open.textContent = "在目录中预览";
        open.addEventListener("click", () => selectObject(file).catch((error) => window.alert(error.message)));
        card.append(title, detail, score, open);
      } else {
        const unavailable = document.createElement("span");
        unavailable.className = "ai-result__note";
        unavailable.textContent = "不在当前目录；切换至对应目录后可预览";
        card.append(title, detail, score, unavailable);
      }
      grid.append(card);
    }
    aiSearchResults.append(grid);
    if (aiSearch.nextCursor) {
      const more = document.createElement("button");
      more.type = "button";
      more.className = "command-button ai-search__more";
      more.textContent = aiSearch.loadingMore ? "正在加载…" : "加载更多结果";
      more.disabled = aiSearch.loadingMore;
      more.addEventListener("click", () => loadMoreAiSearch().catch((error) => {
        setAiSearchState("服务不可用", "error");
        aiSearchHint.textContent = `无法继续加载：${error.message}`;
      }));
      aiSearchResults.append(more);
    }
  }

  async function requestAiSearchPage(query, cursor = null) {
    const body = { query, page_size: 9 };
    if (cursor) body.cursor = cursor;
    return aiRequest("/ai/search", { method: "POST", body: JSON.stringify(body) });
  }

  function mergeAiSearchResults(previous, incoming) {
    const known = new Set(previous.map((result) => String(result.asset_id)));
    return previous.concat((incoming || []).filter((result) => !known.has(String(result.asset_id))));
  }

  async function runAiSearch() {
    updateAiQueryFromTags();
    const query = aiSearchInput.value.trim();
    if (!query) {
      aiSearchHint.textContent = "请输入描述，或选择一个快捷标签。";
      aiSearchInput.focus();
      return;
    }
    const requestId = aiSearch.requestId + 1;
    aiSearch.requestId = requestId;
    aiSearch.loadingMore = false;
    aiSearch.nextCursor = null;
    aiSearchSubmit.disabled = true;
    setAiSearchState("正在检索", "loading");
    aiSearchHint.textContent = `正在通过 ${AI_API} 查询已索引素材。`;
    aiSearchResults.replaceChildren(emptyState("AI 正在比对图文向量与素材元数据。"));
    try {
      const response = await requestAiSearchPage(query);
      if (requestId !== aiSearch.requestId) return;
      aiSearch.results = response.results || [];
      aiSearch.query = response.query || query;
      aiSearch.nextCursor = response.page?.next_cursor || null;
      aiSearch.totalCandidates = Number(response.page?.total_candidates || aiSearch.results.length);
      setAiSearchState(`${aiSearch.results.length}/${aiSearch.totalCandidates} 个结果`, "ready");
      aiSearchHint.textContent = `查询“${aiSearch.query}”完成（${Number(response.query_latency_ms || 0).toFixed(1)} ms）；结果来自跨目录的已完成 AI 索引素材。`;
      renderAiSearchResults(aiSearch.results);
    } catch (error) {
      if (requestId !== aiSearch.requestId) return;
      setAiSearchState("服务不可用", "error");
      aiSearchHint.textContent = `无法连接 AI-App-Lite：${error.message}`;
      aiSearchResults.replaceChildren(emptyState("先启动 ai-app-lite API 与 Worker，再重新检索。"));
    } finally {
      if (requestId === aiSearch.requestId) aiSearchSubmit.disabled = false;
    }
  }

  async function loadMoreAiSearch() {
    if (aiSearch.loadingMore || !aiSearch.nextCursor || !aiSearch.query) return;
    const requestId = aiSearch.requestId;
    aiSearch.loadingMore = true;
    renderAiSearchResults(aiSearch.results);
    try {
      const response = await requestAiSearchPage(aiSearch.query, aiSearch.nextCursor);
      if (requestId !== aiSearch.requestId) return;
      aiSearch.results = mergeAiSearchResults(aiSearch.results, response.results);
      aiSearch.nextCursor = response.page?.next_cursor || null;
      aiSearch.totalCandidates = Number(response.page?.total_candidates || aiSearch.totalCandidates);
      setAiSearchState(`${aiSearch.results.length}/${aiSearch.totalCandidates} 个结果`, "ready");
      aiSearchHint.textContent = `已加载跨目录检索结果；排序按相似度和对象 ID 固定。`;
    } finally {
      if (requestId === aiSearch.requestId) {
        aiSearch.loadingMore = false;
        renderAiSearchResults(aiSearch.results);
      }
    }
  }

  function retryDelayMilliseconds(error, attempt) {
    const advertisedSeconds = Number(error.retryAfter);
    const advertisedDelay = Number.isFinite(advertisedSeconds) && advertisedSeconds >= 0
      ? advertisedSeconds * 1000 : 1000;
    const exponentialDelay = 1000 * (2 ** Math.min(attempt - 1, 4));
    return Math.min(30000, Math.max(advertisedDelay, exponentialDelay));
  }

  function sleep(milliseconds) {
    return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
  }

  class ChunkUploadScheduler {
    constructor(limit) {
      this.limit = limit;
      this.active = 0;
      this.waiters = [];
    }

    acquire() {
      if (this.active < this.limit) {
        this.active += 1;
        return Promise.resolve();
      }
      return new Promise((resolve) => this.waiters.push(resolve));
    }

    release() {
      const next = this.waiters.shift();
      if (next) {
        next();
        return;
      }
      this.active -= 1;
    }
  }

  const chunkUploadScheduler = new ChunkUploadScheduler(MAX_ACTIVE_CHUNK_UPLOADS);

  function makeQueueEntry(file) {
    const element = $("#queue-item-template").content.firstElementChild.cloneNode(true);
    const entry = {
      file, element,
      name: element.querySelector(".queue-item__name"),
      detail: element.querySelector(".queue-item__detail"),
      state: element.querySelector(".queue-item__state"),
      track: element.querySelector(".progress-track"),
      bar: element.querySelector(".progress-track__bar"),
      transfer: { startedAt: 0, lastSampleAt: 0, lastBytes: 0, smoothedBytesPerSecond: 0 },
    };
    entry.name.textContent = file.name;
    entry.detail.textContent = formatBytes(file.size);
    queue.append(element);
    entries.set(file, entry);
    return entry;
  }

  function setEntry(entry, state, label, percent = null, detail = null) {
    entry.element.dataset.state = state;
    entry.state.textContent = label;
    if (detail !== null) entry.detail.textContent = detail;
    if (percent !== null) {
      const bounded = Math.max(0, Math.min(100, percent));
      entry.bar.style.width = `${bounded}%`;
      entry.track.setAttribute("aria-valuenow", String(Math.round(bounded)));
    }
  }

  function observeTransfer(entry, uploadedBytes, totalBytes) {
    const now = performance.now();
    const metrics = entry.transfer;
    if (!metrics.startedAt) {
      metrics.startedAt = now;
      metrics.lastSampleAt = now;
      metrics.lastBytes = uploadedBytes;
      return { rate: 0, eta: null };
    }

    const elapsedSeconds = (now - metrics.lastSampleAt) / 1000;
    const byteDelta = Math.max(0, uploadedBytes - metrics.lastBytes);
    if (elapsedSeconds > 0 && byteDelta > 0) {
      const instantRate = byteDelta / elapsedSeconds;
      metrics.smoothedBytesPerSecond = metrics.smoothedBytesPerSecond
        ? metrics.smoothedBytesPerSecond * 0.75 + instantRate * 0.25
        : instantRate;
      metrics.lastSampleAt = now;
      metrics.lastBytes = uploadedBytes;
    }

    const rate = metrics.smoothedBytesPerSecond;
    return { rate, eta: rate > 0 ? Math.max(0, totalBytes - uploadedBytes) / rate : null };
  }

  function finishTransfer(entry, totalBytes) {
    const metrics = entry.transfer;
    if (!metrics.startedAt) return { elapsed: null, averageRate: 0 };
    const elapsed = Math.max(0.001, (performance.now() - metrics.startedAt) / 1000);
    return { elapsed, averageRate: totalBytes / elapsed };
  }

  function refreshSelection() {
    queue.replaceChildren(); entries.clear();
    const files = [...fileInput.files];
    files.forEach(makeQueueEntry);
    queueCount.textContent = `${files.length} 项`;
    selectionSummary.textContent = files.length ? `${files.length} 个素材，共 ${formatBytes(files.reduce((sum, file) => sum + file.size, 0))}` : "尚未选择素材";
    startButton.disabled = files.length === 0;
  }

  async function sha256(blob) {
    const buffer = await blob.arrayBuffer();
    if (window.crypto && window.crypto.subtle) {
      const digest = await window.crypto.subtle.digest("SHA-256", buffer);
      return [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, "0")).join("");
    }
    return sha256Fallback(new Uint8Array(buffer));
  }

  // HTTP over a Tailnet IP is not a secure browser context, so retain a compact fallback.
  // DataNode independently hashes every uploaded chunk; this also identifies
  // the manifest sent to the Gateway before any data-plane transfer starts.
  function sha256Fallback(bytes) {
    const rightRotate = (value, amount) => (value >>> amount) | (value << (32 - amount));
    const constants = [1116352408,1899447441,3049323471,3921009573,961987163,1508970993,2453635748,2870763221,3624381080,310598401,607225278,1426881987,1925078388,2162078206,2614888103,3248222580,3835390401,4022224774,264347078,604807628,770255983,1249150122,1555081692,1996064986,2554220882,2821834349,2952996808,3210313671,3336571891,3584528711,113926993,338241895,666307205,773529912,1294757372,1396182291,1695183700,1986661051,2177026350,2456956037,2730485921,2820302411,3259730800,3345764771,3516065817,3600352804,4094571909,275423344,430227734,506948616,659060556,883997877,958139571,1322822218,1537002063,1747873779,1955562222,2024104815,2227730452,2361852424,2428436474,2756734187,3204031479,3329325298];
    const message = new Uint8Array(((bytes.length + 9 + 63) >> 6) << 6);
    message.set(bytes); message[bytes.length] = 0x80;
    const bitLength = BigInt(bytes.length) * 8n;
    for (let i = 0; i < 8; i += 1) message[message.length - 1 - i] = Number((bitLength >> BigInt(i * 8)) & 255n);
    let h0 = 1779033703, h1 = 3144134277, h2 = 1013904242, h3 = 2773480762, h4 = 1359893119, h5 = 2600822924, h6 = 528734635, h7 = 1541459225;
    const words = new Uint32Array(64);
    for (let offset = 0; offset < message.length; offset += 64) {
      for (let i = 0; i < 16; i += 1) words[i] = (message[offset + i * 4] << 24) | (message[offset + i * 4 + 1] << 16) | (message[offset + i * 4 + 2] << 8) | message[offset + i * 4 + 3];
      for (let i = 16; i < 64; i += 1) { const a = words[i - 15], b = words[i - 2]; words[i] = (words[i - 16] + (rightRotate(a, 7) ^ rightRotate(a, 18) ^ (a >>> 3)) + words[i - 7] + (rightRotate(b, 17) ^ rightRotate(b, 19) ^ (b >>> 10))) >>> 0; }
      let a = h0, b = h1, c = h2, d = h3, e = h4, f = h5, g = h6, h = h7;
      for (let i = 0; i < 64; i += 1) { const s1 = rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25); const choice = (e & f) ^ (~e & g); const t1 = (h + s1 + choice + constants[i] + words[i]) >>> 0; const s0 = rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22); const majority = (a & b) ^ (a & c) ^ (b & c); h = g; g = f; f = e; e = (d + t1) >>> 0; d = c; c = b; b = a; a = (t1 + s0 + majority) >>> 0; }
      h0 = (h0 + a) >>> 0; h1 = (h1 + b) >>> 0; h2 = (h2 + c) >>> 0; h3 = (h3 + d) >>> 0; h4 = (h4 + e) >>> 0; h5 = (h5 + f) >>> 0; h6 = (h6 + g) >>> 0; h7 = (h7 + h) >>> 0;
    }
    return [h0,h1,h2,h3,h4,h5,h6,h7].map((value) => value.toString(16).padStart(8, "0")).join("");
  }

  const crc32cTable = (() => {
    const table = new Uint32Array(256);
    for (let value = 0; value < 256; value += 1) {
      let crc = value;
      for (let bit = 0; bit < 8; bit += 1) {
        crc = (crc >>> 1) ^ ((crc & 1) ? 0x82f63b78 : 0);
      }
      table[value] = crc >>> 0;
    }
    return table;
  })();

  async function crc32c(blob) {
    const bytes = new Uint8Array(await blob.arrayBuffer());
    let crc = 0xffffffff;
    for (const byte of bytes) crc = crc32cTable[(crc ^ byte) & 0xff] ^ (crc >>> 8);
    return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
  }

  function routeChain(route) { return route.chain.map((node) => `${node.nodeId}@${node.address}:${node.httpPort}`).join(";"); }

  function uploadChunk(route, chunk, index, onProgress) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      const storageIdentity = route.storageIdentity || route.chunkHash;
      xhr.open("PUT", `http://${route.primaryAddress}:${route.primaryPort}/v2/chunks/${storageIdentity}`);
      xhr.setRequestHeader("Content-Type", "application/octet-stream");
      xhr.setRequestHeader("X-Session-Id", route.sessionId);
      xhr.setRequestHeader("X-Chunk-Index", String(index));
      xhr.setRequestHeader("X-Commit-Owner", route.primaryNodeId);
      xhr.setRequestHeader("X-Gateway-Address", window.location.hostname);
      xhr.setRequestHeader("X-Gateway-Port", window.location.port || "80");
      xhr.setRequestHeader("X-Replica-Chain", routeChain(route));
      xhr.setRequestHeader("X-Replica-Position", "0");
      xhr.setRequestHeader("X-Upload-Token", route.uploadToken);
      xhr.upload.onprogress = (event) => { if (event.lengthComputable) onProgress(event.loaded); };
      xhr.onerror = () => reject(new Error("DataNode 网络请求失败；请检查 CORS、Tailscale 地址和节点状态"));
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else {
          const error = new Error(`DataNode returned HTTP ${xhr.status}: ${xhr.responseText}`);
          error.status = xhr.status;
          error.retryAfter = xhr.getResponseHeader("Retry-After");
          reject(error);
        }
      };
      xhr.send(chunk);
    });
  }

  function manifestCanonicalText(fileSize, chunkSize, chunks) {
    let value = `minikv-manifest-v1\n${fileSize}\n${chunkSize}\n`;
    for (const chunk of chunks) value += `${chunk.index}:${chunk.hash}:${chunk.size}\n`;
    return value;
  }

  async function buildUploadManifest(file, entry, dir) {
    const chunkSize = UPLOAD_CHUNK_SIZE;
    const totalChunks = Math.ceil(file.size / chunkSize);
    const chunks = [];
    for (let index = 0; index < totalChunks; index += 1) {
      const start = index * chunkSize;
      const chunk = file.slice(start, Math.min(file.size, start + chunkSize));
      setEntry(entry, "uploading", `校验 ${index + 1}/${totalChunks}`,
        (start / file.size) * 100, `${formatBytes(file.size)} / ${dir}`);
      chunks.push({ index, hash: await sha256(chunk), size: chunk.size });
    }
    const manifestHash = await sha256(new Blob([
      manifestCanonicalText(file.size, chunkSize, chunks),
    ], { type: "text/plain" }));
    return { fileName: file.name, dirPath: dir, fileSize: file.size, chunkSize, manifestHash, chunks };
  }

  async function resumeOrPreflight(file, dir, manifest) {
    const key = sessionKey(file, dir);
    const existing = localStorage.getItem(key);
    if (existing) {
      try {
        const saved = JSON.parse(existing);
        const session = await request(`/upload/sessions/${saved.sessionId}`, { method: "GET" });
        if (saved.manifestHash === manifest.manifestHash &&
            session.manifestHash === manifest.manifestHash) {
          return { ...session, sessionId: saved.sessionId, completed: new Set(session.completed || []) };
        }
        localStorage.removeItem(key);
      } catch (_) { localStorage.removeItem(key); }
    }
    const preflight = await request("/upload/preflight", {
      method: "POST", body: JSON.stringify(manifest),
    });
    if (preflight.status === "CONTENT_EXISTS") return { contentExists: true, object: preflight.object };
    if (preflight.status !== "UPLOAD_REQUIRED" || !preflight.sessionId) {
      throw new Error("Gateway 返回了无效的上传预检结果");
    }
    localStorage.setItem(key, JSON.stringify({
      sessionId: preflight.sessionId,
      manifestHash: manifest.manifestHash,
    }));
    return { ...preflight, completed: new Set(preflight.completed || []) };
  }

  async function uploadFile(file) {
    const entry = entries.get(file); const dir = catalogState.activeDirectory;
    setEntry(entry, "uploading", "建立内容清单", 0, `${formatBytes(file.size)} / ${dir}`);
    const manifest = await buildUploadManifest(file, entry, dir);
    const session = await resumeOrPreflight(file, dir, manifest);
    if (session.contentExists) {
      setEntry(entry, "completed", "已复用", 100,
        `${formatBytes(file.size)} / 内容已存在，未传输数据`);
      await loadCatalog(dir);
      return;
    }

    let confirmedBytes = 0;
    let sentBytes = 0;
    let confirmedChunks = 0;
    const pending = [];
    const inFlightBytes = new Map();
    for (const descriptor of manifest.chunks) {
      const index = descriptor.index;
      const chunk = file.slice(index * session.chunkSize,
        Math.min(file.size, (index + 1) * session.chunkSize));
      if (session.completed.has(index)) {
        confirmedBytes += chunk.size;
        confirmedChunks += 1;
        setEntry(entry, "uploading", `复用 ${confirmedChunks}/${session.totalChunks}`,
          (confirmedBytes / file.size) * 100);
        continue;
      }
      pending.push({ descriptor, chunk });
    }

    const updateProgress = (label) => {
      let activeBytes = 0;
      for (const loaded of inFlightBytes.values()) activeBytes += loaded;
      const visibleBytes = confirmedBytes + activeBytes;
      const transfer = observeTransfer(entry, visibleBytes, file.size);
      const detail = [`${formatBytes(file.size)} / ${dir}`];
      const rate = formatRate(transfer.rate);
      if (rate) detail.push(rate);
      if (transfer.eta !== null) detail.push(`剩余约 ${formatDuration(transfer.eta)}`);
      setEntry(entry, "uploading", `${label} ${confirmedChunks}/${session.totalChunks} · 并发 ${inFlightBytes.size}/${MAX_INFLIGHT_CHUNKS_PER_FILE}`,
        (visibleBytes / file.size) * 100, detail.join(" · "));
    };

    const uploadPendingChunk = async ({ descriptor, chunk }, shouldStop) => {
      const index = descriptor.index;
      for (let retryCount = 0;;) {
        if (shouldStop()) return false;
        updateProgress("等待传输槽位");
        await chunkUploadScheduler.acquire();
        inFlightBytes.set(index, 0);
        updateProgress("分配");
        let uploadError = null;
        try {
          const checksumType = session.checksumType || "sha256";
          const checksumDigest = checksumType === "crc32c" ? await crc32c(chunk) : descriptor.hash;
          const routeBody = { chunks: [{
            index, hash: descriptor.hash, size: descriptor.size,
            checksumType, checksumDigest,
          }] };
          const routes = await request(`/upload/sessions/${session.sessionId}/routes`, {
            method: "POST", body: JSON.stringify(routeBody),
          });
          const route = routes.routes && routes.routes[0];
          if (!route) throw new Error("Gateway 未返回 Chunk 路由");
          route.sessionId = session.sessionId;
          await uploadChunk(route, chunk, index, (loaded) => {
            inFlightBytes.set(index, loaded);
            updateProgress("写入");
          });
          inFlightBytes.delete(index);
          sentBytes += chunk.size;
          confirmedBytes += chunk.size;
          confirmedChunks += 1;
          updateProgress("已确认");
          return true;
        } catch (error) {
          uploadError = error;
          inFlightBytes.delete(index);
        } finally {
          chunkUploadScheduler.release();
        }
        if (shouldStop()) return false;
        if (uploadError.status !== 503 || retryCount >= MAX_CAPACITY_RETRIES) throw uploadError;
        retryCount += 1;
        const delay = retryDelayMilliseconds(uploadError, retryCount);
        updateProgress(`等待存储节点（重试 ${retryCount}）`);
        await sleep(delay);
      }
    };

    let nextPending = 0;
    let fatalUploadError = null;
    const worker = async () => {
      while (!fatalUploadError && nextPending < pending.length) {
        const item = pending[nextPending++];
        try {
          const uploaded = await uploadPendingChunk(item, () => fatalUploadError !== null);
          if (!uploaded) return;
        } catch (error) {
          fatalUploadError = error;
          return;
        }
      }
    };
    const workerCount = Math.min(MAX_INFLIGHT_CHUNKS_PER_FILE, pending.length);
    await Promise.all(Array.from({ length: workerCount }, worker));
    if (fatalUploadError) throw fatalUploadError;

    const result = await request(`/upload/sessions/${session.sessionId}/commit`, { method: "POST", body: "{}" });
    localStorage.removeItem(sessionKey(file, dir));
    const completion = finishTransfer(entry, sentBytes);
    const detail = [`${formatBytes(file.size)} / ${result.fileHash.slice(0, 16)}...`];
    if (completion.elapsed !== null) {
      detail.push(formatDuration(completion.elapsed));
      detail.push(`实际传输 ${formatBytes(sentBytes)}`);
      if (sentBytes) detail.push(`平均 ${formatRate(completion.averageRate)}`);
    }
    setEntry(entry, "completed", result.state || "AVAILABLE", 100, detail.join(" · "));
    await loadCatalog(dir);
  }

  async function uploadFiles(files) {
    let nextIndex = 0;
    async function worker() {
      while (nextIndex < files.length) {
        const file = files[nextIndex++];
        try {
          await uploadFile(file);
        } catch (error) {
          const entry = entries.get(file);
          setEntry(entry, "failed", "失败", null, error.message);
        }
      }
    }
    const workerCount = Math.min(MAX_PARALLEL_FILES, files.length);
    await Promise.all(Array.from({ length: workerCount }, worker));
  }

  async function fetchVerifiedChunk(chunk, onBytes = () => {}) {
    const replica = chunk.replicas && chunk.replicas[0];
    if (!replica) throw new Error(`Chunk ${chunk.index} 没有可读副本`);
    const storageIdentity = chunk.storageIdentity || chunk.hash;
    const response = await fetch(`http://${replica.address}:${replica.httpPort}/v2/chunks/${storageIdentity}`);
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

  function downloadName(item) {
    return item.name || item.fileName || "minikv-download.bin";
  }

  function saveBlob(fileName, chunkBlobs) {
    const objectUrl = URL.createObjectURL(new Blob(chunkBlobs, { type: "application/octet-stream" }));
    const anchor = document.createElement("a");
    anchor.href = objectUrl;
    anchor.download = fileName;
    anchor.hidden = true;
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 60000);
  }

  function createDownloadProgress(button, totalBytes) {
    const startedAt = performance.now();
    let receivedBytes = 0;
    let lastRenderedAt = 0;

    function render(force = false) {
      const now = performance.now();
      if (!force && now - lastRenderedAt < 100) return;
      lastRenderedAt = now;
      const percent = Math.min(100, Math.floor((receivedBytes / totalBytes) * 100));
      const elapsedSeconds = Math.max(0.001, (now - startedAt) / 1000);
      const rate = receivedBytes / elapsedSeconds;
      button.textContent = `下载中 ${percent}% · ${formatRate(rate)}`;
    }

    return {
      onBytes(count) {
        receivedBytes += count;
        render();
      },
      complete() {
        const elapsedSeconds = Math.max(0.001, (performance.now() - startedAt) / 1000);
        button.textContent = `已下载 · 平均 ${formatRate(receivedBytes / elapsedSeconds)}`;
        button.disabled = false;
      },
      fail(error) {
        button.textContent = "下载失败";
        button.title = error.message;
        button.disabled = false;
      },
    };
  }

  async function downloadFile(item, button) {
    button.disabled = true;
    button.textContent = "获取下载清单";
    let progress = null;
    try {
      const manifest = await request(`/objects/${item.objectId}/manifest`, { method: "GET" });
      progress = createDownloadProgress(button, manifest.fileSize);
      const fileName = downloadName(item);
      if (window.isSecureContext && window.showSaveFilePicker) {
        const handle = await window.showSaveFilePicker({ suggestedName: fileName });
        const writable = await handle.createWritable();
        try {
          for (const chunk of manifest.chunks) {
            await writable.write(await fetchVerifiedChunk(chunk, progress.onBytes));
          }
          await writable.close();
        } catch (error) {
          await writable.abort();
          throw error;
        }
      } else {
        const chunkBlobs = [];
        for (const chunk of manifest.chunks) {
          chunkBlobs.push(await fetchVerifiedChunk(chunk, progress.onBytes));
        }
        saveBlob(fileName, chunkBlobs);
      }
      progress.complete();
    } catch (error) {
      if (progress) progress.fail(error);
      else {
        button.textContent = "下载失败";
        button.title = error.message;
        button.disabled = false;
      }
      throw error;
    }
  }

  function emptyState(message) {
    const paragraph = document.createElement("p");
    paragraph.className = "empty-state";
    paragraph.textContent = message;
    return paragraph;
  }

  function releaseCatalogThumbnails() {
    for (const url of catalogState.thumbnailUrls.values()) URL.revokeObjectURL(url);
    catalogState.thumbnailUrls.clear();
  }

  function thumbnailLabel(thumbnail) {
    const state = thumbnail?.state || "UNSUPPORTED";
    return {
      READY: "缩略图就绪",
      PENDING: "缩略图生成中",
      RUNNING: "缩略图生成中",
      FAILED: "缩略图生成失败",
      UNSUPPORTED: "暂无缩略图",
    }[state] || "暂无缩略图";
  }

  async function loadThumbnailUrl(objectId, renderId) {
    const cached = catalogState.thumbnailUrls.get(objectId);
    if (cached) return cached;
    const manifest = await request(`/objects/${encodeURIComponent(objectId)}/manifest`, { method: "GET" });
    const blobs = [];
    for (const chunk of manifest.chunks || []) blobs.push(await fetchVerifiedChunk(chunk));
    const url = URL.createObjectURL(new Blob(blobs, { type: "image/jpeg" }));
    if (renderId !== catalogState.renderId) {
      URL.revokeObjectURL(url);
      return null;
    }
    catalogState.thumbnailUrls.set(objectId, url);
    return url;
  }

  function createThumbnailSurface(file, renderId) {
    const surface = document.createElement("div");
    surface.className = "catalog-card__media";
    const thumbnail = file.thumbnail;
    const state = thumbnail?.state || "UNSUPPORTED";
    surface.dataset.state = state.toLowerCase();

    if (state !== "READY" || !thumbnail?.objectId) {
      const placeholder = document.createElement("span");
      placeholder.className = "catalog-card__placeholder";
      placeholder.textContent = state === "PENDING" || state === "RUNNING" ? "◇" : "—";
      surface.append(placeholder);
      return surface;
    }

    const image = document.createElement("img");
    image.className = "catalog-card__image";
    image.alt = `${file.name} 的缩略图`;
    surface.append(image);
    loadThumbnailUrl(thumbnail.objectId, renderId).then((url) => {
      if (!url || renderId !== catalogState.renderId || !image.isConnected) return;
      image.src = url;
    }).catch(() => {
      if (renderId !== catalogState.renderId || !surface.isConnected) return;
      surface.dataset.state = "failed";
      image.remove();
      const placeholder = document.createElement("span");
      placeholder.className = "catalog-card__placeholder";
      placeholder.textContent = "—";
      surface.append(placeholder);
    });
    return surface;
  }

  function pathName(path) {
    if (path === "/") return "根目录";
    const parts = path.split("/").filter(Boolean);
    return parts[parts.length - 1] || path;
  }

  function parentPath(path) {
    const parts = path.split("/").filter(Boolean);
    parts.pop();
    return parts.length ? `/${parts.join("/")}` : "/";
  }

  function renderDirectoryTree() {
    directoryTree.replaceChildren();
    const catalog = catalogState.catalog;
    if (!catalog) return;
    const root = document.createElement("button");
    root.type = "button"; root.className = "tree-entry";
    root.dataset.active = String(catalogState.activeDirectory === "/");
    root.textContent = "/ 素材库";
    root.addEventListener("click", () => loadCatalog("/"));
    directoryTree.append(root);
    for (const directory of catalog.directories || []) {
      const button = document.createElement("button");
      button.type = "button"; button.className = "tree-entry tree-entry--child";
      button.dataset.active = String(catalogState.activeDirectory === directory.path);
      button.textContent = pathName(directory.path);
      button.addEventListener("click", () => loadCatalog(directory.path));
      directoryTree.append(button);
    }
  }

  function renderBreadcrumbs() {
    breadcrumbs.replaceChildren();
    for (const crumb of (catalogState.catalog?.breadcrumbs || [])) {
      const button = document.createElement("button");
      button.type = "button"; button.className = "breadcrumb";
      button.textContent = crumb.name;
      button.addEventListener("click", () => loadCatalog(crumb.path));
      breadcrumbs.append(button);
    }
  }

  function renderCatalogEntries() {
    catalogEntries.replaceChildren();
    const catalog = catalogState.catalog;
    if (!catalog) return;
    if (!(catalog.directories || []).length && !(catalog.files || []).length) {
      catalogEntries.append(emptyState("此目录为空。可新建目录或直接导入素材。"));
      return;
    }
    for (const directory of catalog.directories || []) {
      const row = document.createElement("button");
      row.type = "button"; row.className = "catalog-row catalog-row--directory";
      const name = document.createElement("strong"); name.textContent = pathName(directory.path);
      const type = document.createElement("span"); type.textContent = "目录";
      row.append(name, type);
      row.addEventListener("click", () => loadCatalog(directory.path));
      catalogEntries.append(row);
    }
    const files = catalog.files || [];
    if (!files.length) return;
    const fileGrid = document.createElement("div");
    fileGrid.className = "catalog-files";
    const renderId = catalogState.renderId;
    for (const file of files) {
      const card = document.createElement("article");
      card.className = "catalog-card";
      card.dataset.objectId = file.objectId;
      card.dataset.thumbnailState = file.thumbnail?.state || "UNSUPPORTED";
      card.tabIndex = 0;
      card.setAttribute("role", "button");
      card.setAttribute("aria-label", `预览 ${file.name}`);
      const surface = createThumbnailSurface(file, renderId);
      const body = document.createElement("div");
      body.className = "catalog-card__body";
      const name = document.createElement("strong"); name.className = "catalog-card__name"; name.textContent = file.name;
      const meta = document.createElement("span"); meta.className = "catalog-card__meta";
      meta.textContent = `${formatBytes(file.fileSize)} · ${thumbnailLabel(file.thumbnail)}`;
      const previewButton = document.createElement("button");
      previewButton.type = "button"; previewButton.className = "catalog-row__command"; previewButton.textContent = "预览";
      previewButton.addEventListener("click", (event) => { event.stopPropagation(); selectObject(file); });
      const downloadButton = document.createElement("button");
      downloadButton.type = "button"; downloadButton.className = "catalog-row__command"; downloadButton.textContent = "下载";
      downloadButton.addEventListener("click", async (event) => {
        event.stopPropagation();
        try { await downloadFile(file, downloadButton); }
        catch (error) { window.alert(error.message); }
      });
      const deleteButton = document.createElement("button");
      deleteButton.type = "button"; deleteButton.className = "catalog-row__command catalog-row__command--danger";
      deleteButton.textContent = "删除"; deleteButton.setAttribute("aria-label", `删除 ${file.name}`);
      deleteButton.addEventListener("click", (event) => { event.stopPropagation(); deleteObject(file, deleteButton); });
      const actions = document.createElement("div"); actions.className = "catalog-row__actions";
      actions.append(previewButton, downloadButton, deleteButton);
      body.append(name, meta, actions);
      card.append(surface, body);
      card.addEventListener("click", () => selectObject(file));
      card.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault(); selectObject(file);
        }
      });
      fileGrid.append(card);
    }
    catalogEntries.append(fileGrid);
  }

  function previewableImage(name) {
    return /\.(avif|gif|jpe?g|png|webp)$/i.test(name);
  }

  function previewMimeType(name) {
    const extension = name.split(".").pop().toLowerCase();
    const types = {
      avif: "image/avif",
      gif: "image/gif",
      jpg: "image/jpeg",
      jpeg: "image/jpeg",
      png: "image/png",
      webp: "image/webp",
    };
    return types[extension] || "application/octet-stream";
  }

  function releasePreviewUrl() {
    if (previewState.url) URL.revokeObjectURL(previewState.url);
    previewState.url = null; previewState.image = null;
  }

  function updatePreviewScale() {
    if (previewState.image) previewState.image.style.transform = `scale(${previewState.scale})`;
  }

  function closePreview() {
    previewState.requestId += 1;
    preview.hidden = true;
    releasePreviewUrl();
    previewState.object = null;
  }

  async function openObjectPreview(file) {
    const requestId = previewState.requestId + 1;
    previewState.requestId = requestId;
    const hasPreview = Object.prototype.hasOwnProperty.call(file, "preview");
    const derivedPreview = file.preview;
    const previewReady = derivedPreview?.state === "READY" && derivedPreview.objectId;

    catalogState.selectedObject = file;
    releasePreviewUrl();
    previewState.object = file; previewState.scale = 1;
    preview.hidden = false;
    previewTitle.textContent = file.name;
    previewMeta.textContent = `${formatBytes(file.fileSize)} · ${file.state}`;
    previewStage.replaceChildren(previewStatus);
    previewDownload.disabled = false; previewDownload.textContent = "下载原始文件";

    if (hasPreview && !previewReady) {
      previewStatus.textContent = derivedPreview?.state === "FAILED"
        ? "预览图生成失败，可下载原始文件。"
        : "预览图正在生成，可下载原始文件。";
      return;
    }

    const previewObjectId = previewReady ? derivedPreview.objectId : file.objectId;
    const object = await request(`/objects/${encodeURIComponent(previewObjectId)}`, { method: "GET" });
    if (requestId !== previewState.requestId) return;
    previewStatus.textContent = previewableImage(object.name)
      ? "正在读取预览。"
      : "此格式不能在浏览器中直接预览，可下载原始文件。";
    if (!previewableImage(object.name)) return;

    const manifest = await request(`/objects/${object.objectId}/manifest`, { method: "GET" });
    if (requestId !== previewState.requestId) return;
    const blobs = [];
    for (const chunk of manifest.chunks) {
      blobs.push(await fetchVerifiedChunk(chunk));
      if (requestId !== previewState.requestId) return;
    }
    const image = document.createElement("img");
    image.className = "preview-stage__image";
    image.alt = object.name;
    const objectUrl = URL.createObjectURL(new Blob(blobs, { type: previewMimeType(object.name) }));
    if (requestId !== previewState.requestId) {
      URL.revokeObjectURL(objectUrl);
      return;
    }
    previewState.url = objectUrl;
    previewState.image = image;
    image.src = previewState.url;
    image.addEventListener("load", () => {
      if (requestId !== previewState.requestId || previewState.image !== image) return;
      previewStatus.remove(); updatePreviewScale();
    });
    image.addEventListener("error", () => {
      if (requestId !== previewState.requestId || previewState.image !== image) return;
      previewStatus.textContent = "浏览器无法解码此图片，可下载原始文件。";
      previewStage.append(previewStatus);
    });
    previewStage.append(image);
  }

  async function selectObject(file) {
    await openObjectPreview(file);
  }

  function hasPendingDerivedMedia(catalog) {
    return (catalog?.files || []).some((file) => {
      const states = [file.thumbnail?.state, file.preview?.state];
      return states.includes("PENDING") || states.includes("RUNNING");
    });
  }

  function scheduleMediaRefresh(catalog) {
    if (mediaRefreshTimer !== null) {
      window.clearTimeout(mediaRefreshTimer);
      mediaRefreshTimer = null;
    }
    if (!hasPendingDerivedMedia(catalog)) return;

    mediaRefreshTimer = window.setTimeout(async () => {
      mediaRefreshTimer = null;
      try {
        await loadCatalog(catalogState.activeDirectory);
      } catch (_) {
        // Keep trying while the current catalog still has unfinished derived media.
        scheduleMediaRefresh(catalogState.catalog);
      }
    }, 3000);
  }

  async function loadCatalog(path = catalogState.activeDirectory) {
    const requestId = catalogState.requestId + 1;
    catalogState.requestId = requestId;
    const catalog = await request(`/catalog?path=${encodeURIComponent(path)}`, { method: "GET" });
    if (requestId !== catalogState.requestId) return;
    releaseCatalogThumbnails();
    catalogState.renderId += 1;
    catalogState.activeDirectory = catalog.path;
    catalogState.catalog = catalog;
    catalogState.selectedObject = null;
    activeDirectoryPath.textContent = catalog.path;
    deleteDirectoryButton.hidden = catalog.path === "/";
    deleteDirectoryButton.disabled = false;
    objectDetail.hidden = true;
    renderDirectoryTree(); renderBreadcrumbs(); renderCatalogEntries();
    scheduleMediaRefresh(catalog);
  }

  async function createDirectory() {
    const name = window.prompt(`在 ${catalogState.activeDirectory} 中新建目录`);
    if (name === null) return;
    await request("/directories", { method: "POST", body: JSON.stringify({ parentPath: catalogState.activeDirectory, name }) });
    await loadCatalog(catalogState.activeDirectory);
  }

  async function deleteObject(file, button) {
    if (!window.confirm(`删除“${file.name}”吗？此操作会立即从素材目录移除。`)) return;
    button.disabled = true; button.textContent = "删除中";
    try {
      await request(`/objects/${encodeURIComponent(file.objectId)}`, { method: "DELETE" });
      if (previewState.object?.objectId === file.objectId) closePreview();
      await loadCatalog(catalogState.activeDirectory);
    } catch (error) {
      button.disabled = false; button.textContent = "删除";
      window.alert(error.message);
    }
  }

  async function deleteActiveDirectory() {
    const path = catalogState.activeDirectory;
    if (path === "/") return;
    if (!window.confirm(`删除目录“${path}”及其全部内容吗？此操作不能从页面恢复。`)) return;
    deleteDirectoryButton.disabled = true;
    try {
      await request(`/directories?path=${encodeURIComponent(path)}`, { method: "DELETE" });
      closePreview();
      await loadCatalog(parentPath(path));
    } catch (error) {
      deleteDirectoryButton.disabled = false;
      window.alert(error.message);
    }
  }


  async function refreshNodes() {
    try {
      const data = await request("/admin/nodes", { method: "GET" });
      const nodes = data.nodes || []; nodeList.replaceChildren();
      const online = nodes.filter((node) => node.state === 1).length;
      clusterState.textContent = online ? "曝光稳定" : "等待节点";
      clusterDetail.textContent = `${online}/${nodes.length} 个节点在线`;
      if (!nodes.length) { nodeList.innerHTML = '<p class="empty-state">Gateway 尚未收到 DataNode 心跳。</p>'; return; }
      nodes.forEach((node) => {
        const state = nodeStates[node.state] || "UNKNOWN";
        const usedRatio = node.usedBytes + node.freeBytes ? (node.usedBytes / (node.usedBytes + node.freeBytes)) * 100 : 0;
        const row = document.createElement("article"); row.className = "node-row";
        row.innerHTML = `<div class="node-row__head"><strong class="node-row__name"></strong><span class="node-row__state"></span></div><div class="node-row__meter"><span></span></div><p class="node-row__detail"></p>`;
        row.querySelector(".node-row__name").textContent = node.nodeId;
        const stateElement = row.querySelector(".node-row__state"); stateElement.textContent = state; stateElement.dataset.state = state.toLowerCase();
        row.querySelector(".node-row__meter span").style.width = `${usedRatio}%`;
        row.querySelector(".node-row__detail").textContent = `${node.address}:${node.httpPort}  |  已用 ${formatBytes(node.usedBytes)}  |  可用 ${formatBytes(node.freeBytes)}`;
        nodeList.append(row);
      });
    } catch (error) { clusterState.textContent = "Gateway 不可达"; clusterDetail.textContent = error.message; nodeList.innerHTML = '<p class="empty-state">无法读取节点状态。</p>'; }
  }

  fileInput.addEventListener("change", refreshSelection);
  aiSearchForm.addEventListener("submit", (event) => { event.preventDefault(); runAiSearch(); });
  document.querySelectorAll(".ai-tag").forEach((button) => {
    button.addEventListener("click", () => {
      const tag = button.dataset.tag;
      if (aiSearch.activeTags.has(tag)) aiSearch.activeTags.delete(tag);
      else aiSearch.activeTags.add(tag);
      button.dataset.active = String(aiSearch.activeTags.has(tag));
      updateAiQueryFromTags();
      aiSearchHint.textContent = aiSearch.activeTags.size
        ? `已选择：${[...aiSearch.activeTags].join("、")}；可继续补充描述后检索。`
        : "标签会合并为查询描述；已建立 AI 索引的素材才会返回结果。";
    });
  });
  $("#refresh-nodes").addEventListener("click", refreshNodes);
  $("#open-library").addEventListener("click", () => document.querySelector(".archive-panel").scrollIntoView({ behavior: "smooth", block: "start" }));
  $("#new-directory").addEventListener("click", () => createDirectory().catch((error) => window.alert(error.message)));
  deleteDirectoryButton.addEventListener("click", () => deleteActiveDirectory());
  $("#preview-close").addEventListener("click", closePreview);
  preview.querySelector("[data-preview-close]").addEventListener("click", closePreview);
  $("#preview-zoom-out").addEventListener("click", () => { previewState.scale = Math.max(.25, previewState.scale - .25); updatePreviewScale(); });
  $("#preview-fit").addEventListener("click", () => { previewState.scale = 1; updatePreviewScale(); });
  $("#preview-zoom-in").addEventListener("click", () => { previewState.scale = Math.min(4, previewState.scale + .25); updatePreviewScale(); });
  previewDownload.addEventListener("click", async () => {
    if (!previewState.object) return;
    try { await downloadFile(previewState.object, previewDownload); }
    catch (error) { window.alert(error.message); }
  });
  document.addEventListener("keydown", (event) => { if (event.key === "Escape" && !preview.hidden) closePreview(); });
  uploadForm.addEventListener("submit", async (event) => {
    event.preventDefault(); const files = [...fileInput.files]; if (!files.length) return;
    startButton.disabled = true;
    await uploadFiles(files);
    startButton.disabled = false; refreshNodes();
  });
  loadCatalog().catch((error) => { catalogEntries.replaceChildren(emptyState(`无法读取素材目录：${error.message}`)); });
  refreshNodes(); window.setInterval(refreshNodes, 8000);
})();
