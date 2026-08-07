# V2 Direct Delete and FreeList Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make V2 objects and directory trees irreversibly deletable while reclaiming DataNode extents through a persistent FreeList.

**Architecture:** GatewayState remains the authority for object reachability and owns LevelDB-backed `DeleteTask` records. `gateway_main` is only an asynchronous dispatcher that sends internal DELETE requests to online DataNodes and ACKs task progress. FastDataStore owns the physical index and a persistent, coalescing FreeList; it never decides whether a Chunk is globally safe to delete.

**Tech Stack:** C++17, LevelDB, existing EventLoop/AsyncHttpRequest/HttpServer, OpenSSL, existing CTest executables, browser JavaScript.

## Global Constraints

- Direct delete is irreversible: no tombstones, restore endpoint, recycle bin, or grace period.
- A `DeleteTask` is operational state only and survives Gateway restart.
- Deleting a directory recursively deletes descendant active objects; deleting `/` is rejected.
- A physical Chunk is deleted only if no remaining active object references it.
- DataNode DELETE is idempotent: missing Chunk returns success and does not duplicate a FreeList extent.
- V2 reuses free extents but does not punch filesystem holes or introduce WAL/scrub/repair.
- Existing V2 upload/download routes, Chunk hashes, manifests, and node heartbeat APIs remain compatible.

---

## File Structure

| File | Responsibility |
|---|---|
| `include/gateway/GatewayState.hpp` | Delete task types, delete results, Gateway delete/scheduling APIs and in-memory task index. |
| `src/gateway/GategayState.cpp` | LevelDB serialization/load, atomic catalog mutation, shared-reference checks and task ACK state transitions. |
| `src/gateway/gateway_main.cpp` | Public delete routes and async internal DELETE dispatcher driven by timer/heartbeat. |
| `include/DataNode/FastDataStore.hpp` | Local `remove()` API and FreeList allocation helpers/state. |
| `src/DataNode/FastDataStore.cpp` | Persistent `free:` records, best-fit allocation, coalescing, idempotent extent removal. |
| `src/DataNode/datanode_main.cpp` | Authenticated internal DELETE endpoint that calls `FastDataStore::remove()`. |
| `www-v2/app.js` | File/directory delete commands, confirmation, catalog refresh and preview close. |
| `test/test_gateway_delete.cpp` | Gateway object/directory deletion, shared Chunk safety, persistence and pending task tests. |
| `test/test_fast_data_store_delete.cpp` | Local index deletion, FreeList reuse, coalescing, idempotency and restart tests. |
| `CMakeLists.txt` | Build and register the two new unit-test executables. |

---

### Task 1: Add Persistent DataNode FreeList

**Files:**
- Modify: `include/DataNode/FastDataStore.hpp`
- Modify: `src/DataNode/FastDataStore.cpp`
- Create: `test/test_fast_data_store_delete.cpp`
- Modify: `CMakeLists.txt`

**Consumes:** The existing local physical-index keys `e:{chunkHash}` with value `offset:length`.

**Produces:**

```cpp
bool FastDataStore::remove(const std::string& chunkHash, bool& removed);
uint64_t FastDataStore::reusableBytes() const;
```

`free:{offset}` keys have a decimal length value. The in-memory structure is
`std::map<uint64_t, uint64_t> freeExtents_`, ordered by offset.

- [ ] **Step 1: Write the failing FreeList tests**

Create `test/test_fast_data_store_delete.cpp`. Use three SHA-256-known payloads:

```cpp
CHECK(store.put(hashA, bytesA, alreadyExists));
CHECK(store.put(hashB, bytesB, alreadyExists));
CHECK(store.remove(hashA, removed));
CHECK(removed);
CHECK(!store.exists(hashA));
CHECK(store.reusableBytes() == bytesA.size());

CHECK(store.put(hashC, bytesA, alreadyExists));
FileRegion regionC;
CHECK(store.getRegion(hashC, regionC));
CHECK(regionC.offset == 0);  // best-fit reuses A's released extent

CHECK(store.remove(hashC, removed));
CHECK(store.remove(hashC, removed));
CHECK(!removed);             // repeat delete adds no second free extent
```

Add an adjacent-extent scenario: write equal-size A/B, delete A then B, write
a payload whose size is the sum of both, and assert it starts at A's offset.
Close and reopen the store before the final write to prove `free:` persistence.

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cmake -S . -B build
cmake --build build --target test_fast_data_store_delete -j"$(nproc)"
./build/bin/test_fast_data_store_delete
```

Expected: compilation failure because `remove()` and `reusableBytes()` do not
exist.

- [ ] **Step 3: Add FreeList state and load it at startup**

In `FastDataStore.hpp`, add:

```cpp
bool remove(const std::string& chunkHash, bool& removed);
uint64_t reusableBytes() const;

bool loadFreeExtentsLocked();
bool addFreeExtentLocked(const PhysicalExtent& extent);
bool allocateExtentLocked(uint64_t length, PhysicalExtent& extent);
std::map<uint64_t, uint64_t> freeExtents_;
```

In `open()`, load all `free:` keys after opening `physical_index`. Parse each
key suffix as an offset and each value as a positive length. Return false on
malformed persisted data rather than silently allocating overlapping extents.

- [ ] **Step 4: Implement coalescing and best-fit allocation**

`addFreeExtentLocked()` must find adjacent predecessor/successor ranges,
merge them, then write the final `free:{offset}` record and delete replaced
keys in one `leveldb::WriteBatch`.

`allocateExtentLocked()` scans `freeExtents_` for the smallest length at least
the requested length. In a `WriteBatch`, delete the selected key, reinsert a
tail key when larger, then update `freeExtents_` only after `Write()` succeeds.

Change `beginPut()` so that an absent hash uses `allocateExtentLocked()` first;
only if that fails does it reserve `[nextOffset_, expectedSize]` and advance
`nextOffset_`.

- [ ] **Step 5: Implement idempotent local physical deletion**

Under `mutex_`, look up `e:{chunkHash}`. If absent set `removed = false` and
return true. If present, atomically delete `e:{chunkHash}` and add its extent
to the FreeList through one combined `WriteBatch`. Set `removed = true` only
after the write succeeds.

- [ ] **Step 6: Run the new and existing FastDataStore tests**

Run:

```bash
cmake --build build --target test_fast_data_store_delete test_fast_data_store_read test_fast_data_store_region -j"$(nproc)"
ctest --test-dir build --output-on-failure -R 'fast_data_store_(delete|read|region)'
```

Expected: all three tests pass.

- [ ] **Step 7: Commit the focused DataNode storage change**

```bash
git add include/DataNode/FastDataStore.hpp src/DataNode/FastDataStore.cpp \
  test/test_fast_data_store_delete.cpp CMakeLists.txt
git commit -m "feat: reuse deleted DataNode extents"
```

---

### Task 2: Add Gateway DeleteTask Metadata and Catalog Mutation

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Create: `test/test_gateway_delete.cpp`
- Modify: `CMakeLists.txt`

**Consumes:** `ObjectMeta`, `FileMeta`, `ChunkRoute`, `obj:`, `path:`, `dir:`,
`f:` and `c:` records.

**Produces:**

```cpp
enum class DeleteStatus { kDeleted, kNotFound, kInvalidRequest };

struct DeleteTaskSnapshot {
    std::string chunkHash;
    std::vector<std::string> pendingNodeIds;
};

DeleteStatus deleteObject(const std::string& objectId);
DeleteStatus deleteDirectory(const std::string& path);
std::vector<DeleteTaskSnapshot> pendingDeletesForNode(const std::string& nodeId) const;
bool acknowledgeDelete(const std::string& chunkHash, const std::string& nodeId);
```

Persist tasks at `del:{chunkHash}`. Their encoded value contains the pending
node IDs plus created/updated timestamps. `loadDeleteTasksLocked()` rebuilds
the in-memory task map during `open()`.

- [ ] **Step 1: Write failing Gateway deletion tests**

Create `test/test_gateway_delete.cpp` using the session/route helpers from
`test/test_gateway_catalog.cpp`. Cover:

```cpp
CHECK(state.deleteObject(first.objectId) == DeleteStatus::kDeleted);
CHECK(!state.getObject(first.objectId, object));
CHECK(!state.listCatalog("/2026/Xian", catalog) || catalog.files.empty());
CHECK(!state.buildManifestSnapshot(first.fileHash, manifest));

const auto tasks = state.pendingDeletesForNode("node-a");
CHECK(tasks.size() == 1);
CHECK(tasks[0].chunkHash == "chunk-DSC_0001.NEF");
CHECK(state.acknowledgeDelete(tasks[0].chunkHash, "node-a"));
CHECK(state.pendingDeletesForNode("node-a").empty());
```

Create two active objects that share a `fileHash`; deleting one must create no
task and must leave the shared manifest readable through the surviving object.
Create `/trip/day1` with two files, delete `/trip`, and assert neither files
nor descendant directories remain visible. Assert `deleteDirectory("/")`
returns `kInvalidRequest`. Reopen GatewayState and assert an unacknowledged
task still appears for its node.

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cmake --build build --target test_gateway_delete -j"$(nproc)"
./build/bin/test_gateway_delete
```

Expected: compilation failure because delete/task APIs do not exist.

- [ ] **Step 3: Define DeleteTask storage and loading**

Add `DeleteStatus`, a private `DeleteTask`, `deleteTasks_`, serializer/parser,
`persistDeleteTaskLocked()`, `deleteDeleteTaskLocked()`, and
`loadDeleteTasksLocked()`. `open()` must load tasks after routes.

Task initialization copies the current `ChunkRoute.replicas` into
`pendingNodeIds`. An empty route is invalid and must not remove catalog state.

- [ ] **Step 4: Implement atomic active-reference calculation**

Inside the Gateway mutex, gather target objects first. For each target
`fileHash`, determine whether an object outside the target set uses it. Only a
file hash without an outside reference is physically eligible. For each
eligible `FileMeta::chunkHashes`, create a `DeleteTask` only when no remaining
eligible file references that hash.

This explicitly handles both whole-file deduplication and individual Chunk
deduplication without adding a new refcount database in this phase.

- [ ] **Step 5: Implement atomic object and recursive directory deletion**

Build a single `leveldb::WriteBatch` that deletes each selected `obj:` and
`path:` key, then deletes the selected `dir:` keys for a directory operation.
For eligible file hashes, delete `f:{fileHash}` and put `del:{chunkHash}`.
Do not delete `c:{chunkHash}` until every node ACKs.

Call `db_->Write()` before mutating `objects_`, `objectByPath_`, `directories_`,
`files_`, or `deleteTasks_`. Return `kNotFound` for unknown object/path and
`kInvalidRequest` for root deletion or invalid paths.

- [ ] **Step 6: Implement task query and ACK**

`pendingDeletesForNode(nodeId)` returns only tasks that list `nodeId`.
`acknowledgeDelete()` removes that node from exactly one task. If it becomes
empty, its `del:` key and `c:{chunkHash}` route key are deleted in one batch;
otherwise persist the shrunk task. An unknown task/node ACK returns false;
repeated ACK after successful removal may return true only if the task no
longer exists, preserving network idempotency.

- [ ] **Step 7: Run all Gateway tests**

Run:

```bash
cmake --build build --target test_gateway_delete test_gateway_catalog test_gateway_manifest_snapshot test_gateway_write_lease -j"$(nproc)"
ctest --test-dir build --output-on-failure -R 'gateway_(delete|catalog|manifest_snapshot|write_lease)'
```

Expected: all Gateway tests pass.

- [ ] **Step 8: Commit Gateway metadata changes**

```bash
git add include/gateway/GatewayState.hpp src/gateway/GategayState.cpp \
  test/test_gateway_delete.cpp CMakeLists.txt
git commit -m "feat: track irreversible object deletion"
```

---

### Task 3: Expose and Authenticate the DataNode Internal Delete Endpoint

**Files:**
- Modify: `src/DataNode/datanode_main.cpp`
- Modify: `test/test_fast_data_store_delete.cpp` only if response semantics
  require an additional local-store assertion.

**Consumes:** `FastDataStore::remove()`, current cluster secret setup, existing
`json()` helper and internal token convention.

**Produces:**

```text
DELETE /internal/v2/chunks/{chunkHash}
X-Cluster-Internal-Token: <cluster secret>

204 No Content  deleted or already absent
403              invalid cluster token
400              invalid chunk hash/path
```

- [ ] **Step 1: Add an endpoint behavior test harness**

Create a temporary integration shell test under `test/` only if the existing
test executable style cannot exercise HttpServer. Start a DataNode with a
temporary data directory and secret, PUT a known Chunk, issue DELETE with the
secret, assert GET returns 404, issue DELETE again and assert 204, then issue
DELETE without the secret and assert 403.

Name it `test/test_v2_datanode_delete.sh`; use a random free local port and a
trap that terminates the child process.

- [ ] **Step 2: Run the endpoint test and verify RED**

Run:

```bash
bash test/test_v2_datanode_delete.sh
```

Expected: DELETE route returns 404 because it is not implemented.

- [ ] **Step 3: Add the internal DELETE route**

Before browser CORS handling in `setHttpCallback`, match exactly
`DELETE /internal/v2/chunks/{hash}`. Validate the internal token with
`constantTimeEquals`; reject malformed or empty hashes. Call `store.remove()`.
For an existing or absent index entry return HTTP 204. Do not attach CORS
headers because this endpoint is not browser-accessible.

- [ ] **Step 4: Run the endpoint test and existing DataNode tests**

Run:

```bash
cmake --build build --target minikv_v2_datanode test_fast_data_store_delete -j"$(nproc)"
bash test/test_v2_datanode_delete.sh
ctest --test-dir build --output-on-failure -R 'fast_data_store_(delete|read|region)'
```

Expected: all pass.

- [ ] **Step 5: Commit the endpoint**

```bash
git add src/DataNode/datanode_main.cpp test/test_v2_datanode_delete.sh
git commit -m "feat: add internal DataNode chunk deletion"
```

---

### Task 4: Dispatch DeleteTasks from Gateway on Timer and Node Recovery

**Files:**
- Modify: `include/gateway/GatewayState.hpp`
- Modify: `src/gateway/GategayState.cpp`
- Modify: `src/gateway/gateway_main.cpp`
- Modify: `test/test_gateway_delete.cpp`

**Consumes:** `pendingDeletesForNode()`, `acknowledgeDelete()`, node runtime
state, `AsyncHttpRequest`, and the internal DataNode DELETE endpoint.

**Produces:** Gateway dispatches every pending task once for an online node,
without blocking EventLoop or issuing duplicates while a request is inflight.

- [ ] **Step 1: Add a failing scheduler-state test**

Extend `test_gateway_delete.cpp` with a state-level query:

```cpp
CHECK(state.pendingDeletesForNode("node-b").size() == 1);
NodeRuntime offline;
CHECK(state.heartbeat("node-b", offline));
CHECK(state.isNodeOnline("node-b"));
```

Add a narrow GatewayState API if necessary to expose an online snapshot for
the dispatcher. The test must show task persistence after reopen and task
removal only after the final node ACK.

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cmake --build build --target test_gateway_delete -j"$(nproc)"
./build/bin/test_gateway_delete
```

Expected: failure until the task state supports complete node-aware queries.

- [ ] **Step 3: Add a small dispatcher in `gateway_main.cpp`**

Create a local `dispatchPendingDeletes` lambda with:

```cpp
std::set<std::pair<std::string, std::string>> deleteInflight;
```

For each online `NodeSnapshot`, request
`state.pendingDeletesForNode(nodeId)`. For each non-inflight task, create an
`AsyncHttpRequest` with method `DELETE`, path
`/internal/v2/chunks/{chunkHash}`, `Connection: close`, the cluster token
header, a 10-second timeout, and no request body.

On 2xx response call `state.acknowledgeDelete(chunkHash, nodeId)`; erase the
inflight key on every completion/error. No blocking client or `poll()` may be
introduced.

Call the lambda after successful heartbeat processing and via
`loop.runEvery(5000, ...)`, alongside node-timeout checks.

- [ ] **Step 4: Verify dispatcher behavior manually**

Start one Gateway and two DataNodes. Upload a one-Chunk object, stop node B,
delete through Gateway, then verify `del:` remains using LevelDB inspection or
Gateway log instrumentation. Restart node B and verify its Chunk GET becomes
404; verify the task is then removed after node A/B ACKs.

- [ ] **Step 5: Run Gateway regression tests**

Run:

```bash
cmake --build build --target minikv_v2_gateway test_gateway_delete -j"$(nproc)"
ctest --test-dir build --output-on-failure -R 'gateway_(delete|catalog|manifest_snapshot|write_lease)'
```

Expected: all tests pass.

- [ ] **Step 6: Commit the dispatcher**

```bash
git add include/gateway/GatewayState.hpp src/gateway/GategayState.cpp \
  src/gateway/gateway_main.cpp test/test_gateway_delete.cpp
git commit -m "feat: dispatch pending physical chunk deletion"
```

---

### Task 5: Add Minimal Catalog Delete Controls

**Files:**
- Modify: `www-v2/app.js`
- Modify: `www-v2/style.css`
- Modify: `test/test_v2_blob_download_fallback.sh` or create
  `test/test_v2_catalog_delete_ui.py`

**Consumes:**

```text
DELETE /api/v2/objects/{objectId}
DELETE /api/v2/directories?path={absolutePath}
```

**Produces:** A delete command for each object and directory. Success closes an
open preview, reloads the appropriate parent catalog, and cannot be undone.

- [ ] **Step 1: Write the failing browser test**

Use Playwright with mocked `/api/v2/catalog` and DELETE responses. Assert:

```python
page.get_by_role("button", name="删除", exact=True).click()
page.on("dialog", lambda dialog: dialog.accept())
assert delete_request_url == "/api/v2/objects/image"
assert page.locator("#object-preview").is_hidden()
```

For a directory row, assert the request is
`/api/v2/directories?path=%2Ftrip` and browser confirmation explicitly says
the directory and descendants are permanently deleted.

- [ ] **Step 2: Run the browser test and verify RED**

Run the static frontend through the existing `with_server.py` wrapper and
execute the new test. Expected: no delete command exists.

- [ ] **Step 3: Implement destructive commands**

Add a `deleteCatalogEntry(kind, value, label)` helper. It must use
`window.confirm` with irreversible wording, issue the corresponding DELETE
request, call `closePreview()`, then reload the active catalog. Object rows
receive a compact `删除` command; directory rows receive a separate delete
command that stops propagation so it does not navigate into the directory.

Use the existing restrained command style; no recycle-bin UI or restore copy.

- [ ] **Step 4: Run UI regression tests**

Run:

```bash
python3 /home/ubuntu/.codex/skills/webapp-testing/scripts/with_server.py \
  --timeout 10 --server 'python3 -m http.server 18190 --directory www-v2' \
  --port 18190 -- python3 test/test_v2_catalog_delete_ui.py
python3 /home/ubuntu/.codex/skills/webapp-testing/scripts/with_server.py \
  --timeout 10 --server 'python3 -m http.server 18188 --directory www-v2' \
  --port 18188 -- python3 /tmp/minikv_download_name_ui_check.py
```

Expected: delete UI test and existing Blob download-name regression both pass.

- [ ] **Step 5: Commit the UI command**

```bash
git add www-v2/app.js www-v2/style.css test/test_v2_catalog_delete_ui.py
git commit -m "feat: delete catalog objects and directories"
```

---

### Task 6: Full Build and Multi-Node Verification

**Files:**
- Modify: none unless verification exposes a defect.

- [ ] **Step 1: Build all V2 targets and run CTest**

Run:

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
git diff --check
```

Expected: all CTest entries pass and whitespace check is clean.

- [ ] **Step 2: Verify direct deletion with two local DataNodes**

1. Start Gateway plus two DataNodes with distinct directories/ports.
2. Upload a file through the browser or V2 client.
3. Delete it from the catalog.
4. Verify object and manifest endpoints return 404.
5. Verify both DataNode `GET /v2/chunks/{hash}` endpoints return 404.
6. Upload a same-size new Chunk and verify `FastDataStore::getRegion()` shows
   an offset reused from the deleted extent.

- [ ] **Step 3: Verify offline retry**

1. Upload with two replicas.
2. Stop one DataNode.
3. Delete the object and verify it disappears from catalog immediately.
4. Restart the stopped node with the same data directory/nodeId.
5. Verify the Gateway dispatches DELETE after heartbeat and its local GET
   returns 404.

- [ ] **Step 4: Commit verification-only documentation only if needed**

Do not commit runtime data, temporary LevelDB directories, `perf.data*`, or
generated benchmark output. Commit only source/test/doc changes that were
intentionally added during this phase.

## Plan Self-Review

- Every design requirement maps to a task: catalog deletion (Task 2/5),
  persistent task/retry (Task 2/4), local physical deletion/FreeList (Task 1/3),
  and end-to-end behavior (Task 6).
- No tombstone, restore, hole punch, repair, or WAL work appears in an
  implementation task.
- Public APIs and LevelDB keys are defined before their consumers.
- Tests are written before each implementation task and every task has a
  specific verification command.
