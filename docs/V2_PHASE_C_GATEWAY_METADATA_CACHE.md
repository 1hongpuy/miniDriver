# V2 Phase C: Gateway Metadata Cache

> Status: implemented and verified. This document records C1-C5 so later V3/V4 work does not reintroduce eager metadata loading.

## Goal

The Gateway owns logical metadata. It must not load every file, route, directory, and object into a `std::map` at startup: a large photo library would otherwise make both restart time and memory proportional to all stored objects.

Phase C uses Cache-Aside LRU:

```text
Gateway read -> LRU hit: return immutable snapshot
             -> LRU miss: LevelDB point read or prefix scan -> parse -> LRU put -> return
```

LevelDB is the source of truth. Cache entries are disposable, and Gateway restart only loses cached copies.

## C1: Generic LRU

`include/gateway/LruCache.hpp` implements `LruCache<Key, Value>` with a hash index and recency list. It enforces entry and byte limits, TTL expiry, immutable `shared_ptr<const Value>` values, a mutex, and hit/miss/eviction/expiry/rejection counters. It has no LevelDB knowledge, so the Gateway owns cache keys and invalidation policy.

`test/test_lru_cache.cpp` verifies recency promotion, byte-pressure eviction, TTL expiry, replacement, and erase.

## C2: Object Cache

`GatewayState::getObject(objectId, out)` now checks `ObjectMetaCache`, then reads `obj:{objectId}` from LevelDB only on a miss. Object creation and object deletion erase that key.

- Limit: 4096 entries, 4 MiB, 10-minute TTL.
- Code: `include/gateway/GatewayState.hpp`, `src/gateway/GategayState.cpp`.
- Test: `test/test_gateway_object_cache.cpp` proves miss, hit, and deletion invalidation.

Object lookup is a direct-key, high-frequency bridge from catalog rows to download and deletion, so it was the first low-risk Cache-Aside conversion.

## C3: Catalog Cache

`listCatalog(path, out)` caches a complete immutable `CatalogSnapshot` by normalized directory path. A miss runs `buildCatalogSnapshotLocked`: it scans `dir:` and `obj:` records, filters direct children, sorts them, then caches the complete result.

- Limit: 512 snapshots, 8 MiB, 30-second TTL.
- Creating a directory erases the parent snapshot.
- File commit and catalog deletion clear the catalog cache, because one operation can create or remove several implicit parent directories.
- Test: `test/test_gateway_catalog_cache.cpp` verifies hit and parent-listing invalidation.

The snapshot is cached rather than individual directory records because the frontend needs a consistent, sorted list of children and breadcrumbs; caching only records would still require a scan and sort for each request.

## C4: Manifest Cache

`buildManifestSnapshot(fileHash, out)` caches the assembled download manifest: `FileMeta`, ordered `ChunkRoute` values, and replica `NodeRecord` values. On a miss, it uses the file and route caches, then resolves the small persistent node map.

- Limit: 256 manifests, 16 MiB, 60-second TTL.
- Static node registration/address changes clear all manifests.
- Route commit or final route deletion clear all manifests.
- File commit or final logical deletion erase that file hash.
- Test: `test/test_gateway_manifest_cache.cpp` verifies miss, hit, and address-change invalidation.

Manifest invalidation is deliberately conservative: stale node addresses or replica lists are worse than a cache miss.

## C5: Lazy Business Metadata

Removed these eager startup maps from `GatewayState`:

```text
files_
routes_
directories_
objects_
objectByPath_
```

`open()` now loads only live coordination state:

```text
n:    NodeRecord, needed for placement
s:    SessionState, needed for resumable upload
del:  DeleteTask, needed for deletion dispatch
```

Business data stays in LevelDB and is fetched only on demand:

| Helper | LevelDB key | Cache |
|---|---|---|
| `getFileLocked` | `f:{fileHash}` | FileMetaCache |
| `getRouteLocked` | `c:{chunkHash}` | ChunkRouteCache |
| `getDirectoryLocked` | `dir:{ownerId\\npath}` | direct point read |
| `getObjectLocked` | `obj:{objectId}` | ObjectMetaCache |
| `objectIdAtPathLocked` | `path:{ownerId\\npath}` | direct point read |

`backfillLegacyCatalogLocked()` streams `f:` records directly from LevelDB and writes missing `dir:`, `obj:`, and `path:` keys. It no longer depends on a loaded `files_` map.

Catalog listing and directory deletion scan `dir:` and `obj:` prefixes on cold reads. This is correct for the current key format and avoids a migration. If profiling later shows those scans dominate, V4 can add a child-prefix index; the catalog LRU already eliminates repeated hot-directory scans.

`test/test_gateway_metadata_lazy_load.cpp` reopens a populated database and proves every business cache starts at zero, then warms only requested object/file records.

## Limits

| Cache | Entries | Bytes | TTL | Cached value |
|---|---:|---:|---:|---|
| Object | 4096 | 4 MiB | 10 min | `ObjectMeta` |
| File | 2048 | 8 MiB | 10 min | `FileMeta` plus chunk hashes |
| Route | 8192 | 8 MiB | 10 min | `ChunkRoute` |
| Catalog | 512 | 8 MiB | 30 sec | `CatalogSnapshot` |
| Manifest | 256 | 16 MiB | 60 sec | `ManifestSnapshot` |

Both byte and entry limits are required because a small number of video/RAW manifests can contain many chunk hashes. TTL is only a safety fallback; correctness comes from write-path invalidation.

## Concurrency and Metrics

The lock order is fixed:

```text
GatewayState mutex -> LRU mutex
```

Cache values are immutable snapshots. V2 still serializes Gateway metadata work with the `GatewayState` mutex; Phase C reduces disk reads and startup memory but does not claim V4 parallel metadata QPS.

Read-only methods exist for tests and later admin metrics:

```cpp
objectCacheStats();
catalogCacheStats();
manifestCacheStats();
metadataCacheUsage();
```

No admin HTTP metrics endpoint or YAML tuning is added yet. Those are justified only after collecting real cache hit/miss data.

## Verification

New tests are registered in `CMakeLists.txt`:

- `gateway_catalog_cache`
- `gateway_manifest_cache`
- `gateway_metadata_lazy_load`

Run:

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure
```

Expected suite size after Phase C: 22 tests.

## Non-Goals

- No distributed cache: one Gateway only needs local invalidation.
- No DataNode physical-index cache change: that LevelDB is local allocator state, not object catalog metadata.
- No secondary directory-child index until cold-scan profiling proves it necessary.
- Sessions, node runtime, leases, and delete tasks remain in memory because they are live coordination state, not immutable catalog data.
