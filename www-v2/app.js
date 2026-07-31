(() => {
  "use strict";

  const API = "/api/v2";
  const SESSION_PREFIX = "minikv-v2:session:";
  const MAX_PARALLEL_FILES = 2;
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
  const entries = new Map();
  const catalogState = { activeDirectory: "/", catalog: null, selectedObject: null };
  const previewState = { object: null, url: null, scale: 1, image: null, requestId: 0 };

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes)) return "未知容量";
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let value = Math.max(0, bytes); let unit = 0;
    while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit += 1; }
    return `${value >= 10 || unit === 0 ? value.toFixed(0) : value.toFixed(1)} ${units[unit]}`;
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
    if (!response.ok) throw new Error(body.error || `Gateway returned HTTP ${response.status}`);
    return body;
  }

  function makeQueueEntry(file) {
    const element = $("#queue-item-template").content.firstElementChild.cloneNode(true);
    const entry = {
      file, element,
      name: element.querySelector(".queue-item__name"),
      detail: element.querySelector(".queue-item__detail"),
      state: element.querySelector(".queue-item__state"),
      track: element.querySelector(".progress-track"),
      bar: element.querySelector(".progress-track__bar"),
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
  // DataNode independently hashes every chunk; this only creates the route request hash.
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

  function routeChain(route) { return route.chain.map((node) => `${node.nodeId}@${node.address}:${node.httpPort}`).join(";"); }

  function uploadChunk(route, chunk, index, onProgress) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("PUT", `http://${route.primaryAddress}:${route.primaryPort}/v2/chunks/${route.chunkHash}`);
      xhr.setRequestHeader("Content-Type", "application/octet-stream");
      xhr.setRequestHeader("X-Session-Id", route.sessionId);
      xhr.setRequestHeader("X-Chunk-Index", String(index));
      xhr.setRequestHeader("X-Commit-Owner", route.primaryNodeId);
      xhr.setRequestHeader("X-Gateway-Address", window.location.hostname);
      xhr.setRequestHeader("X-Gateway-Port", window.location.port || "80");
      xhr.setRequestHeader("X-Replica-Chain", routeChain(route));
      xhr.setRequestHeader("X-Replica-Position", "0");
      xhr.setRequestHeader("X-Upload-Token", route.uploadToken);
      xhr.upload.onprogress = (event) => { if (event.lengthComputable) onProgress(event.loaded / event.total); };
      xhr.onerror = () => reject(new Error("DataNode 网络请求失败；请检查 CORS、Tailscale 地址和节点状态"));
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else reject(new Error(`DataNode returned HTTP ${xhr.status}: ${xhr.responseText}`));
      };
      xhr.send(chunk);
    });
  }

  async function resumeOrCreate(file, dir) {
    const key = sessionKey(file, dir);
    const existing = localStorage.getItem(key);
    if (existing) {
      try {
        const saved = JSON.parse(existing);
        const session = await request(`/upload/sessions/${saved.sessionId}`, { method: "GET" });
        return { ...session, sessionId: saved.sessionId, completed: new Set(session.completed || []) };
      } catch (_) { localStorage.removeItem(key); }
    }
    const session = await request("/upload/sessions", { method: "POST", body: JSON.stringify({ fileName: file.name, dirPath: dir, fileSize: file.size }) });
    localStorage.setItem(key, JSON.stringify({ sessionId: session.sessionId }));
    return { ...session, completed: new Set() };
  }

  async function uploadFile(file) {
    const entry = entries.get(file); const dir = catalogState.activeDirectory;
    setEntry(entry, "uploading", "建立会话", 0, `${formatBytes(file.size)} / ${dir}`);
    const session = await resumeOrCreate(file, dir);
    for (let index = 0; index < session.totalChunks; index += 1) {
      const start = index * session.chunkSize;
      const chunk = file.slice(start, Math.min(file.size, start + session.chunkSize));
      const chunkWeight = chunk.size / file.size;
      if (session.completed.has(index)) { setEntry(entry, "uploading", `续传 ${index + 1}/${session.totalChunks}`, ((start + chunk.size) / file.size) * 100); continue; }
      setEntry(entry, "uploading", `测光 ${index + 1}/${session.totalChunks}`, (start / file.size) * 100);
      const hash = await sha256(chunk);
      const routeBody = { chunks: [{ index, hash, size: chunk.size }] };
      const routes = await request(`/upload/sessions/${session.sessionId}/routes`, { method: "POST", body: JSON.stringify(routeBody) });
      const route = routes.routes && routes.routes[0];
      if (!route) throw new Error("Gateway 未返回 Chunk 路由");
      route.sessionId = session.sessionId;
      await uploadChunk(route, chunk, index, (fraction) => setEntry(entry, "uploading", `写入 ${index + 1}/${session.totalChunks}`, ((start / file.size) + fraction * chunkWeight) * 100));
      setEntry(entry, "uploading", `已确认 ${index + 1}/${session.totalChunks}`, ((start + chunk.size) / file.size) * 100);
    }
    const result = await request(`/upload/sessions/${session.sessionId}/commit`, { method: "POST", body: "{}" });
    localStorage.removeItem(sessionKey(file, dir));
    setEntry(entry, "completed", result.state || "AVAILABLE", 100, `${formatBytes(file.size)} / ${result.fileHash.slice(0, 16)}...`);
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

  async function fetchVerifiedChunk(chunk) {
    const replica = chunk.replicas && chunk.replicas[0];
    if (!replica) throw new Error(`Chunk ${chunk.index} 没有可读副本`);
    const response = await fetch(`http://${replica.address}:${replica.httpPort}/v2/chunks/${chunk.hash}`);
    if (!response.ok) throw new Error(`Chunk ${chunk.index} 下载失败: HTTP ${response.status}`);
    const blob = await response.blob();
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

  async function downloadFile(item) {
    const manifest = await request(`/objects/${item.objectId}/manifest`, { method: "GET" });
    const fileName = downloadName(item);
    if (window.isSecureContext && window.showSaveFilePicker) {
      const handle = await window.showSaveFilePicker({ suggestedName: fileName });
      const writable = await handle.createWritable();
      try {
        for (const chunk of manifest.chunks) await writable.write(await fetchVerifiedChunk(chunk));
        await writable.close();
        return;
      } catch (error) {
        await writable.abort();
        throw error;
      }
    }

    const chunkBlobs = [];
    for (const chunk of manifest.chunks) chunkBlobs.push(await fetchVerifiedChunk(chunk));
    saveBlob(fileName, chunkBlobs);
  }

  function emptyState(message) {
    const paragraph = document.createElement("p");
    paragraph.className = "empty-state";
    paragraph.textContent = message;
    return paragraph;
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
    for (const file of catalog.files || []) {
      const row = document.createElement("article");
      row.className = "catalog-row";
      const name = document.createElement("strong"); name.textContent = file.name;
      const meta = document.createElement("span"); meta.textContent = `${formatBytes(file.fileSize)} · ${file.state}`;
      const previewButton = document.createElement("button");
      previewButton.type = "button"; previewButton.className = "catalog-row__command"; previewButton.textContent = "预览";
      previewButton.addEventListener("click", () => selectObject(file.objectId));
      const downloadButton = document.createElement("button");
      downloadButton.type = "button"; downloadButton.className = "catalog-row__command"; downloadButton.textContent = "下载";
      downloadButton.addEventListener("click", async () => {
        downloadButton.disabled = true; downloadButton.textContent = "读取中";
        try { await downloadFile(file); downloadButton.textContent = "已开始"; }
        catch (error) { downloadButton.textContent = "失败"; window.alert(error.message); }
        finally { window.setTimeout(() => { downloadButton.disabled = false; downloadButton.textContent = "下载"; }, 1000); }
      });
      const deleteButton = document.createElement("button");
      deleteButton.type = "button"; deleteButton.className = "catalog-row__command catalog-row__command--danger";
      deleteButton.textContent = "删除"; deleteButton.setAttribute("aria-label", `删除 ${file.name}`);
      deleteButton.addEventListener("click", () => deleteObject(file, deleteButton));
      const actions = document.createElement("div"); actions.className = "catalog-row__actions";
      actions.append(previewButton, downloadButton, deleteButton);
      row.append(name, meta, actions);
      catalogEntries.append(row);
    }
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

  async function openObjectPreview(objectId) {
    const requestId = previewState.requestId + 1;
    previewState.requestId = requestId;
    const object = await request(`/objects/${encodeURIComponent(objectId)}`, { method: "GET" });
    if (requestId !== previewState.requestId) return;
    catalogState.selectedObject = object;
    releasePreviewUrl();
    previewState.object = object; previewState.scale = 1;
    preview.hidden = false;
    previewTitle.textContent = object.name;
    previewMeta.textContent = `${formatBytes(object.fileSize)} · ${object.state}`;
    previewStatus.textContent = previewableImage(object.name) ? "正在读取预览。" : "此格式不能在浏览器中直接预览，可下载原始文件。";
    previewStage.replaceChildren(previewStatus);
    previewDownload.disabled = false; previewDownload.textContent = "下载原始文件";
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

  async function selectObject(objectId) {
    await openObjectPreview(objectId);
  }

  async function loadCatalog(path = catalogState.activeDirectory) {
    const catalog = await request(`/catalog?path=${encodeURIComponent(path)}`, { method: "GET" });
    catalogState.activeDirectory = catalog.path;
    catalogState.catalog = catalog;
    catalogState.selectedObject = null;
    activeDirectoryPath.textContent = catalog.path;
    deleteDirectoryButton.hidden = catalog.path === "/";
    deleteDirectoryButton.disabled = false;
    objectDetail.hidden = true;
    renderDirectoryTree(); renderBreadcrumbs(); renderCatalogEntries();
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
    previewDownload.disabled = true; previewDownload.textContent = "正在读取";
    try { await downloadFile(previewState.object); previewDownload.textContent = "下载已开始"; }
    catch (error) { previewDownload.textContent = "下载失败"; window.alert(error.message); }
    finally { previewDownload.disabled = false; }
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
