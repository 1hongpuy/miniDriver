// =============================================
// MiniDrive 前端逻辑（支持虚拟目录）
// =============================================

const API_BASE = '/api/files';

// 当前状态
let allFiles    = [];           // 文件列表
let allFolders  = [];           // 子目录列表
let currentPath = '/';          // 当前浏览的目录

// ========== 页面初始化 ==========
document.addEventListener('DOMContentLoaded', () => {
    loadDirectory('/');

    document.getElementById('uploadBtn').addEventListener('click', uploadFiles);
    document.getElementById('refreshBtn').addEventListener('click', () => loadDirectory(currentPath));
    document.getElementById('searchBox').addEventListener('input', renderGallery);
    document.getElementById('filterType').addEventListener('change', renderGallery);
    document.getElementById('newFolderBtn').addEventListener('click', createFolder);

    // 面包屑根目录
    document.querySelector('.crumb[data-path="/"]').addEventListener('click', () => {
        loadDirectory('/');
    });

    // 拖拽上传
    const uploadArea = document.getElementById('upload-area');
    uploadArea.addEventListener('dragover', (e) => { e.preventDefault(); });
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        document.getElementById('fileInput').files = e.dataTransfer.files;
        uploadFiles();
    });

    document.getElementById('fileInput').addEventListener('change', (e) => {
        document.getElementById('uploadStatus').textContent =
            `已选择 ${e.target.files.length} 个文件`;
    });

    // 移动弹窗
    document.getElementById('moveConfirmBtn').addEventListener('click', confirmMove);
    document.getElementById('moveCancelBtn').addEventListener('click', hideMoveDialog);
});

// ========== 加载目录 ==========
async function loadDirectory(path) {
    currentPath = normalizePath(path);
    updateBreadcrumb(currentPath);
    document.getElementById('currentDirLabel').textContent = `上传到: ${currentPath}`;

    try {
        const resp = await fetch(`${API_BASE}?path=${encodeURIComponent(currentPath)}`);
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const data = await resp.json();

        allFolders = (data.folders || []).map(f => ({ name: f }));
        allFiles   = data.files || [];

        renderGallery();
    } catch (err) {
        console.error('加载目录失败:', err);
        document.getElementById('gallery').innerHTML =
            '<p style="text-align:center;color:#8b949e;padding:40px;grid-column:1/-1;">加载失败</p>';
    }
}

// ========== 面包屑 ==========
function updateBreadcrumb(path) {
    const container = document.getElementById('breadcrumbPath');
    container.innerHTML = '';

    if (path === '/') return;

    const parts = path.split('/').filter(p => p);
    let cumPath = '';
    parts.forEach((part, i) => {
        const sep = document.createElement('span');
        sep.className = 'crumb-sep';
        sep.textContent = ' / ';
        container.appendChild(sep);

        cumPath += '/' + part;
        const span = document.createElement('span');
        span.textContent = part;
        span.dataset.path = normalizePath(cumPath);

        if (i === parts.length - 1) {
            span.className = 'current';
        } else {
            span.addEventListener('click', () => loadDirectory(span.dataset.path));
        }
        container.appendChild(span);
    });
}

// ========== 上传文件 ==========
async function uploadFiles() {
    const input = document.getElementById('fileInput');
    const files = input.files;
    if (files.length === 0) {
        document.getElementById('uploadStatus').textContent = '请先选择文件';
        return;
    }

    const btn = document.getElementById('uploadBtn');
    const status = document.getElementById('uploadStatus');
    btn.disabled = true;

    let success = 0, fail = 0;
    const uploadPath = currentPath;  // 锁定当前目录，防止导航后路径变化

    for (const file of files) {
        status.textContent = `正在上传: ${file.name} (${formatSize(file.size)})...`;

        const formData = new FormData();
        formData.append('files', file);

        try {
            const resp = await fetch(
                `${API_BASE}?path=${encodeURIComponent(uploadPath)}`,
                { method: 'POST', body: formData }
            );
            if (resp.ok) success++; else fail++;
        } catch (err) {
            fail++;
        }
    }

    status.textContent = `完成: ${success} 成功, ${fail} 失败`;
    btn.disabled = false;
    input.value = '';
    loadDirectory(currentPath);
}

// ========== 新建目录（前端只做导航提示，目录由上传时自动创建） ==========
function createFolder() {
    const name = prompt('新建目录名：');
    if (!name || !name.trim()) return;

    // V1.0 虚拟目录：新建目录就是导航到一个新路径
    // 只要后续有文件上传到这里，目录就自动出现了
    const newPath = normalizePath(currentPath + '/' + name.trim());
    loadDirectory(newPath);
}

// ========== 渲染网格 ==========
function renderGallery() {
    const gallery = document.getElementById('gallery');
    const searchTerm = document.getElementById('searchBox').value.toLowerCase();
    const filterType = document.getElementById('filterType').value;
    gallery.innerHTML = '';

    // ---- 先渲染目录 ----
    let folders = allFolders;
    if (searchTerm) {
        folders = folders.filter(f => f.name.toLowerCase().includes(searchTerm));
    }

    if (filterType === 'all' || filterType === 'image') {
        for (const folder of folders) {
            gallery.appendChild(createFolderCard(folder));
        }
    }

    // ---- 再渲染文件 ----
    let files = allFiles.filter(f => {
        const name = f.file_name.toLowerCase();
        const matchSearch = !searchTerm || name.includes(searchTerm);
        const matchType = filterType === 'all' ||
            (filterType === 'image' && isImageType(f.file_name)) ||
            (filterType === 'video' && isVideoType(f.file_name));
        return matchSearch && matchType;
    });

    if (folders.length === 0 && files.length === 0) {
        gallery.innerHTML +=
            '<p style="text-align:center;color:#8b949e;padding:40px;grid-column:1/-1;">此目录为空</p>';
        return;
    }

    for (const file of files) {
        gallery.appendChild(createFileCard(file));
    }
}

// ========== 创建目录卡片 ==========
function createFolderCard(folder) {
    const card = document.createElement('div');
    card.className = 'folder-card';

    const icon = document.createElement('div');
    icon.className = 'folder-icon';
    icon.textContent = '📁';
    card.appendChild(icon);

    const info = document.createElement('div');
    info.className = 'info';
    info.innerHTML = `<div class="name">${escapeHtml(folder.name)}</div>`;
    card.appendChild(info);

    // 点击进入目录
    card.addEventListener('click', () => {
        const newPath = normalizePath(currentPath + '/' + folder.name);
        loadDirectory(newPath);
    });

    return card;
}

// ========== 创建文件卡片（含右键菜单） ==========
function createFileCard(file) {
    const card = document.createElement('div');
    card.className = 'file-card';

    // 缩略图 / 图标
    // 大文件（> 3MB 或无缩略图）只显示图标，不下载全尺寸原图
    const hasThumb = file.thumb_status === 3 && file.thumb_url;
    const isSmall  = file.thumb_status === 0 && file.file_size < 3*1024*1024;

    if ((hasThumb || isSmall) && isImageType(file.file_name) && !isRawType(file.file_name)) {
        const img = document.createElement('img');
        img.className = 'thumb';
        img.src = hasThumb ? file.thumb_url : downloadUrl(file);
        img.loading = 'lazy';
        img.alt = file.file_name;
        card.appendChild(img);
    } else if (isRawType(file.file_name)) {
        const icon = document.createElement('div');
        icon.className = 'file-icon';
        icon.textContent = '🎞️';
        icon.title = 'RAW 格式';
        card.appendChild(icon);
    } else if (isVideoType(file.file_name)) {
        const icon = document.createElement('div');
        icon.className = 'file-icon';
        icon.textContent = '🎬';
        card.appendChild(icon);
    } else {
        const icon = document.createElement('div');
        icon.className = 'file-icon';
        icon.textContent = '📄';
        card.appendChild(icon);
    }

    // 信息
    const info = document.createElement('div');
    info.className = 'info';
    const ext = getExt(file.file_name).toUpperCase();
    info.innerHTML = `
        <div class="name" title="${escapeHtml(file.file_name)}">${escapeHtml(file.file_name)}</div>
        <div class="meta">
            <span>${ext} · ${formatSize(file.file_size)}</span>
            <span>${file.file_time || ''}</span>
        </div>
    `;
    card.appendChild(info);

    // 操作菜单按钮（⋮）
    const menuBtn = document.createElement('button');
    menuBtn.className = 'card-menu-btn';
    menuBtn.textContent = '⋮';
    menuBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleMenu(file, menuBtn);
    });
    card.appendChild(menuBtn);

    // 点击预览
    card.addEventListener('click', (e) => {
        if (!e.target.closest('.card-menu-btn, .card-menu')) {
            previewFile(file);
        }
    });

    return card;
}

// ========== 右键/⋮ 菜单 ==========
function toggleMenu(file, btn) {
    // 关闭所有已打开的菜单
    document.querySelectorAll('.card-menu').forEach(m => m.remove());

    const menu = document.createElement('div');
    menu.className = 'card-menu show';
    menu.innerHTML = `
        <button class="preview-btn">👁 预览</button>
        <button class="download-btn">⬇ 下载</button>
        <button class="move-btn">📁 移动到...</button>
        <button class="delete-btn danger">🗑 删除</button>
    `;
    btn.parentElement.appendChild(menu);

    // 绑定操作
    menu.querySelector('.preview-btn').addEventListener('click', (e) => {
        e.stopPropagation(); closeAllMenus(); previewFile(file);
    });
    menu.querySelector('.download-btn').addEventListener('click', (e) => {
        e.stopPropagation(); closeAllMenus();
        window.open(downloadUrlForce(file), '_blank');
    });
    menu.querySelector('.move-btn').addEventListener('click', (e) => {
        e.stopPropagation(); closeAllMenus(); showMoveDialog(file);
    });
    menu.querySelector('.delete-btn').addEventListener('click', (e) => {
        e.stopPropagation(); closeAllMenus();
        if (confirm(`确定删除 ${file.file_name}？`)) deleteFile(file);
    });

    // 点击页面其他地方关闭菜单
    setTimeout(() => {
        document.addEventListener('click', closeAllMenus, { once: true });
    }, 0);
}

function closeAllMenus() {
    document.querySelectorAll('.card-menu').forEach(m => m.remove());
}

// ========== 移动文件 ==========

let moveFile = null;  // { old_path, old_name }

function showMoveDialog(file) {
    moveFile = { old_path: file.file_path, old_name: file.file_name };
    document.getElementById('moveFileName').textContent = file.file_name;
    document.getElementById('moveTargetPath').value = currentPath;
    document.getElementById('moveDialog').classList.remove('dialog-hidden');
}

function hideMoveDialog() {
    document.getElementById('moveDialog').classList.add('dialog-hidden');
    moveFile = null;
}

async function confirmMove() {
    if (!moveFile) return;
    const newPath = normalizePath(document.getElementById('moveTargetPath').value);
    const newName = document.getElementById('moveFileName').textContent;

    try {
        const resp = await fetch(`${API_BASE}/move`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                old_path: moveFile.old_path,
                old_name: moveFile.old_name,
                new_path: newPath,
                new_name: newName,
            }),
        });
        if (resp.ok) {
            hideMoveDialog();
            loadDirectory(currentPath);
        } else {
            alert('移动失败');
        }
    } catch (err) {
        alert('移动失败: ' + err.message);
    }
}

// ========== 删除文件 ==========
async function deleteFile(file) {
    try {
        const p = encodeURIComponent(file.file_path || currentPath);
        const n = encodeURIComponent(file.file_name);
        const resp = await fetch(`${API_BASE}?path=${p}&name=${n}`, { method: 'DELETE' });
        if (resp.ok) {
            loadDirectory(currentPath);
        } else {
            alert('删除失败');
        }
    } catch (err) {
        alert('删除失败: ' + err.message);
    }
}

// ========== 预览 ==========
function previewFile(file) {
    const url = downloadUrl(file);
    const preview = document.getElementById('preview');
    const content = document.getElementById('previewContent');
    const info = document.getElementById('previewInfo');

    if (isImageType(file.file_name) && !isRawType(file.file_name)) {
        content.innerHTML = `<img src="${url}" alt="${escapeHtml(file.file_name)}">`;
    } else if (isRawType(file.file_name)) {
        content.innerHTML = `<div style="color:#8b949e;font-size:18px;text-align:center;">
            <p style="font-size:64px;">🎞️</p>
            <p>RAW 格式无法在浏览器中直接预览</p>
            <p style="font-size:14px;">${escapeHtml(file.file_name)}</p>
            <a href="${url}" download style="color:#58a6ff;">点击下载</a>
        </div>`;
    } else if (isVideoType(file.file_name)) {
        content.innerHTML = `<video src="${url}" controls autoplay></video>`;
    } else {
        window.open(downloadUrlForce(file), '_blank');
        return;
    }

    info.textContent = `${file.file_name} — ${formatSize(file.file_size)}`;
    preview.classList.remove('preview-hidden');
}

function closePreview() {
    document.getElementById('preview').classList.add('preview-hidden');
    document.getElementById('previewContent').innerHTML = '';
}

// ========== 文件类型判断 ==========
const IMAGE_EXTS = new Set([
    'jpg','jpeg','png','gif','webp','bmp','svg','tiff','tif','heic','heif','ico'
]);
const RAW_EXTS = new Set([
    'nef','nrw','cr2','cr3','arw','sr2','srf','dng','orf','rw2',
    'pef','raf','3fr','dcr','kdc','k25','erf','mrw','mos','x3f','srw','rwl','raw'
]);
const VIDEO_EXTS = new Set([
    'mp4','mov','mkv','avi','wmv','webm','flv','m4v','mts','m2ts','ts','3gp','ogv','rmvb'
]);

function getFileType(fileName) {
    const ext = getExt(fileName);
    if (IMAGE_EXTS.has(ext)) return 'image';
    if (RAW_EXTS.has(ext))   return 'raw';
    if (VIDEO_EXTS.has(ext)) return 'video';
    return 'other';
}

function isImageType(fileName) { const t = getFileType(fileName); return t === 'image' || t === 'raw'; }
function isVideoType(fileName) { return getFileType(fileName) === 'video'; }
function isRawType(fileName)   { return getFileType(fileName) === 'raw'; }
function getExt(fileName)      { const d=fileName.lastIndexOf('.'); return d<0?'':fileName.slice(d+1).toLowerCase(); }

// ========== 工具 ==========
function formatSize(bytes) {
    if (!bytes) return '0 B';
    const u=['B','KB','MB','GB','TB'];
    const i=Math.floor(Math.log(bytes)/Math.log(1024));
    return i===0 ? bytes+' B' : (bytes/Math.pow(1024,i)).toFixed(1)+' '+u[i];
}
function escapeHtml(str) { const d=document.createElement('div'); d.textContent=str; return d.innerHTML; }
function downloadUrl(file) {
    const p = encodeURIComponent(file.file_path || '/');
    const n = encodeURIComponent(file.file_name);
    return `${API_BASE}/${file.file_hash}/download?path=${p}&name=${n}`;
}
function downloadUrlForce(file) {
    return downloadUrl(file) + '&dl=1';
}

function normalizePath(p) {
    if (!p) return '/';
    if (p[0] !== '/') p = '/' + p;                // 确保以 / 开头
    while (p.length > 1 && p[p.length-1] === '/')  // 去掉末尾所有 /
        p = p.slice(0, -1);
    while (p.includes('//'))                      // 合并多重 /
        p = p.replace('//', '/');
    return (p === '/') ? '/' : p + '/';            // 根目录不加双斜杠
}





