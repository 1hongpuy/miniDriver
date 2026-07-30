# Object Catalog Phase A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Gateway-owned logical directories and objects, then make `www-v2` browse them through a directory tree, breadcrumb and file list.

**Architecture:** Keep existing `f:{fileHash}` and `c:{chunkHash}` records as content metadata. Add persistent `dir:`, `path:` and `obj:` catalog records owned by `GatewayState`; new upload commits atomically create `FileMeta`, `ObjectMeta` and `PathEntry`. The browser obtains every library view from `GET /api/v2/catalog` and continues to use the existing direct-DataNode manifest download path after resolving an `objectId`.

**Tech Stack:** C++17, LevelDB WriteBatch, existing hand-written HTTP server, existing `www-v2` vanilla JavaScript and CSS, CTest.

## Global Constraints

- Keep `GET /api/v2/files/{fileHash}/manifest` unchanged for existing benchmark tooling.
- Keep `ownerId="admin"`, but include owner ID in all catalog keys.
- Do not expose DataNode offsets or physical storage paths in catalog responses.
- Phase A does not implement delete, rename, move, tombstones, versioning, thumbnails or operation history.
- Preserve legacy `f:{fileHash}` records by deterministic `legacy-{fileHash}` catalog backfill.
- Do not modify runtime `data/`, `/tmp` benchmark data, untracked config, or unrelated frontend assets.

---

### Task 1: Define and test catalog metadata primitives

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Create: `test/test_gateway_catalog.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces `DirectoryMeta`, `ObjectMeta`, `CatalogEntry`, `CatalogSnapshot`.
- Produces `GatewayState::createDirectory`, `GatewayState::listCatalog`, `GatewayState::getObject`, and `GatewayState::getObjectManifestSnapshot`.

- [x] **Step 1: Write failing catalog tests**

```cpp
MINIKV_CHECK(state.createDirectory("/", "2026", created));
MINIKV_CHECK(state.createDirectory("/2026", "Xian", created));
MINIKV_CHECK(!state.createDirectory("/2026", "Xian", duplicate));
MINIKV_CHECK(state.listCatalog("/2026", catalog));
MINIKV_CHECK(catalog.directories.size() == 1);
MINIKV_CHECK(catalog.directories[0].path == "/2026/Xian");
```

Also create a legacy `FileMeta`, reopen `GatewayState`, and assert that `/legacy-parent` lists exactly one backfilled object with `objectId == "legacy-" + fileHash`.

- [x] **Step 2: Register and run the failing test**

Add:

```cmake
add_executable(test_gateway_catalog test/test_gateway_catalog.cpp)
target_link_libraries(test_gateway_catalog PRIVATE minikv_gateway)
add_test(NAME gateway_catalog COMMAND test_gateway_catalog)
```

Run: `cmake --build build-perf -j2 && ctest --test-dir build-perf -R gateway_catalog --output-on-failure`  
Expected: compilation failure until the API exists.

- [x] **Step 3: Add normalized catalog types and key codecs**

Implement private serializers/parsers and exact keys:

```cpp
struct DirectoryMeta { std::string ownerId; std::string path; int64_t createdAt = 0; };
struct ObjectMeta {
    std::string objectId, ownerId, parentPath, name, fileHash, contentType;
    uint64_t fileSize = 0;
    FileState state = FileState::kProtecting;
    int64_t createdAt = 0;
};
```

Use `dir:{ownerId}:{path}`, `path:{ownerId}:{absoluteFilePath}`, and `obj:{objectId}`. Add strict helpers that normalize `/`, collapse repeated slashes, reject `.`/`..`, NUL and empty final segments.

- [x] **Step 4: Implement create/list/get and legacy backfill**

`createDirectory(parentPath, name, out)` verifies that the parent is root or has a `dir:` record, verifies neither `dir:` nor `path:` owns the child path, then writes one `dir:` record.

`listCatalog(path, out)` verifies root or directory existence, prefix-scans `dir:{owner}:` and `path:{owner}:`, returns direct children only, and sorts directories/files by name. `getObject` loads `obj:` through the in-memory catalog map.

At `open()`, after legacy files are loaded, create missing deterministic `legacy-{fileHash}` objects and parent directory records in a single `WriteBatch` per backfill batch.

- [x] **Step 5: Run catalog regression tests**

Run: `cmake --build build-perf -j2 && ctest --test-dir build-perf -R 'gateway_catalog|gateway_manifest_snapshot' --output-on-failure`  
Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add include/gateway/GatewayState.hpp src/gateway/GategayState.cpp \
  test/test_gateway_catalog.cpp CMakeLists.txt
git commit -m "feat: add gateway object catalog"
```

### Task 2: Make upload commit create an object/path atomically

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `test/test_gateway_catalog.cpp`

**Interfaces:**
- Changes `GatewayState::commitFile` to return `ObjectMeta` plus `FileMeta` fields needed by old clients.
- Produces `POST /api/v2/upload/sessions/{sessionId}/commit -> {objectId,fileHash,state}`.

- [x] **Step 1: Write failing commit-catalog tests**

Prepare a completed session and ChunkRoute entries. Assert that a successful commit creates exactly one catalog file entry at `/2026/Xian/DSC_0001.NEF`, and `getObject(objectId)` has the committed `fileHash`. Create a second session for the same path and assert commit returns a conflict status without overwriting the existing object.

- [x] **Step 2: Run the failing test**

Run: `ctest --test-dir build-perf -R gateway_catalog --output-on-failure`  
Expected: fail because `commitFile` cannot report a path conflict.

- [x] **Step 3: Add atomic commit behavior**

Build the manifest exactly as today. Before mutating maps, normalize session path/name and reject an existing `path:` key. Generate a new random `objectId`, then place `f:`, `obj:` and `path:` operations in one `leveldb::WriteBatch`. Only update `files_`, `objects_` and path maps after `db_->Write()` succeeds.

Existing `getFile(fileHash)` and old manifest API continue to use `files_`.

- [x] **Step 4: Expose response and error status**

Map path collision to HTTP `409`; keep malformed/incomplete session as `400`. Return escaped `objectId`, `fileHash` and `state` on success.

- [x] **Step 5: Run all Gateway tests**

Run: `cmake --build build-perf -j2 && ctest --test-dir build-perf -R 'gateway_(catalog|write_lease|manifest_snapshot)' --output-on-failure`  
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add include/gateway/GatewayState.hpp src/gateway/GategayState.cpp \
  src/gateway/gateway_main.cpp test/test_gateway_catalog.cpp
git commit -m "feat: add objects to upload commits"
```

### Task 3: Add catalog and object HTTP endpoints

**Files:**
- Modify: `src/gateway/gateway_main.cpp`
- Create: `test/test_gateway_catalog_http.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- `GET /api/v2/catalog?path=/...`
- `POST /api/v2/directories`
- `GET /api/v2/objects/{objectId}`
- `GET /api/v2/objects/{objectId}/manifest`

- [x] **Step 1: Define HTTP contract checks**

Use the existing HTTP test helpers to assert:

```text
GET /api/v2/catalog?path=/              -> 200 and root breadcrumb
POST /api/v2/directories                -> 200
POST duplicate directory                -> 409
GET /api/v2/catalog?path=/missing       -> 404
GET /api/v2/objects/{objectId}/manifest -> same chunks as legacy endpoint
```

- [x] **Step 2: Run endpoint smoke checks after implementation**

Add a `test_gateway_catalog_http` CTest target and run it.  
Expected: 404 until handlers are added.

- [x] **Step 3: Implement endpoint routing and JSON**

Parse query `path` using the existing request/query helper. JSON-escape every user-controlled path/name. Catalog responses include only direct child directory paths and object summary fields. `/objects/{id}/manifest` resolves `ObjectMeta.fileHash` and calls `buildManifestSnapshot`; do not duplicate manifest construction.

- [x] **Step 4: Run all HTTP and Gateway tests**

Run: `cmake --build build-perf -j2 && ctest --test-dir build-perf -R 'gateway_catalog|benchmark_http|http_stream_context' --output-on-failure`  
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/gateway/gateway_main.cpp test/test_gateway_catalog_http.cpp CMakeLists.txt
git commit -m "feat: expose object catalog API"
```

### Task 4: Replace the V2 recent-files UI with an interactive material library

**Files:**
- Modify: `www-v2/index.html`
- Modify: `www-v2/app.js`
- Modify: `www-v2/style.css`

**Interfaces:**
- Consumes the Task 3 catalog/object APIs.
- Upload session body sends the selected catalog directory as `dirPath`.

- [x] **Step 1: Preserve upload queue behavior in a browser fixture**

Create a small mock-fetch manual fixture in `www-v2` development comments or a test page that verifies `uploadOne()` uses `activeDirectory` rather than a free text path. Do not introduce a frontend framework or bundler.

- [x] **Step 2: Add view and catalog state**

Replace `RECENT_KEY` and `readRecent()` with:

```js
const state = { view: "library", activeDirectory: "/", catalog: null, selectedObject: null };
async function loadCatalog(path = state.activeDirectory) { /* GET /catalog */ }
```

Build DOM nodes with `textContent`; do not interpolate file or directory names through `innerHTML`.

- [x] **Step 3: Implement directory navigation**

Add visible Upload/Library navigation, a directory tree, breadcrumb buttons, direct folder/file list, and a create-directory dialog. Clicking a directory calls `loadCatalog`; selecting a file calls `GET /objects/{id}` and fills a detail panel.

- [x] **Step 4: Integrate upload and download**

Upload view displays the active catalog path and starts sessions there. On commit success, refresh the catalog and select the new object. Download calls `/objects/{id}/manifest`, reuses existing per-chunk retry logic, and names the browser download from `ObjectMeta.name`.

- [ ] **Step 5: Validate browser behavior**

Serve `www-v2` through the existing Nginx/static path against a local Gateway. Verify create, navigate, upload, browser refresh, second browser listing, and download. Capture a screenshot only after all text fits at desktop and mobile widths.

- [ ] **Step 6: Commit**

```bash
git add www-v2/index.html www-v2/app.js www-v2/style.css
git commit -m "feat: add v2 material library frontend"
```

### Task 5: Full verification and migration notes

**Files:**
- Modify: `docs/V2_LOCAL_BENCHMARK.md`
- Modify: `docs/superpowers/specs/2026-07-30-object-catalog-phase-a-design.md`

- [ ] **Step 1: Build and run the complete suite**

Run:

```bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON
cmake --build build-perf -j2
ctest --test-dir build-perf --output-on-failure
```

Expected: all prior tests plus catalog tests pass.

- [ ] **Step 2: Run isolated end-to-end smoke**

Start the isolated cluster, create `/2026/Xian`, upload one 4 MiB file through the benchmark/client path, inspect it by catalog API, retrieve the object manifest, and verify the downloaded SHA-256. Stop the cluster in the same shell invocation.

- [ ] **Step 3: Document API compatibility**

State that legacy `/files/{fileHash}/manifest` remains for benchmarks and that new browser code uses `/objects/{objectId}/manifest`. State that deletion and physical GC are intentionally absent.

- [ ] **Step 4: Commit**

```bash
git add docs/V2_LOCAL_BENCHMARK.md docs/superpowers/specs/2026-07-30-object-catalog-phase-a-design.md
git commit -m "docs: record object catalog verification"
```
