# V2 Direct Delete and FreeList Design

## Goal

Implement irreversible object deletion. Removing a file or directory makes it
unavailable immediately, removes physical Chunk replicas asynchronously, and
reuses the released extents for later Chunk writes.

## Scope

- Delete one object or recursively delete a directory tree.
- Remove deleted entries from the normal catalog immediately.
- Persist outstanding physical deletion work in Gateway LevelDB.
- Dispatch deletion to online DataNodes and retry when a pending node becomes
  online.
- Delete the local physical index entry and add its extent to a DataNode
  FreeList.
- Allocate future writes from a fitting free extent before appending at the end
  of `disk0.data`.
- Make repeated delete delivery idempotent.

## Explicitly Out of Scope

- Tombstones, a recycle-bin UI, restoration, or a seven-day grace period.
- `fallocate(...PUNCH_HOLE...)`, WAL/index snapshots, scrub, repair, and
  automatic removal of permanently lost nodes.
- Reclaiming bytes from a machine that never comes back online.

## Terms

`DeleteTask` is an operational record, not a tombstone. It does not keep an
object recoverable. It records only which replica nodes still need to remove a
Chunk physically.

```cpp
struct DeleteTask {
    std::string chunkHash;
    std::vector<std::string> pendingNodeIds;
    uint32_t attempts = 0;
    int64_t createdAt = 0;
    int64_t updatedAt = 0;
};
```

`FreeList` is local DataNode allocator state. Each entry is one unallocated
region of `disk0.data`:

```cpp
struct PhysicalExtent {
    uint64_t offset;
    uint64_t length;
};
```

It is unrelated to whether an object is visible in the Gateway catalog.

## Safety Rule

A Chunk may be physically deleted only when no remaining active object refers
to it. The Gateway calculates this before creating a `DeleteTask`; DataNodes
never infer global liveness themselves.

## Gateway Flow

### File deletion

```text
DELETE /api/v2/objects/{objectId}
  -> load ObjectMeta and its FileMeta
  -> reject unknown object
  -> find every active object using the same fileHash / chunk hashes
  -> LevelDB WriteBatch:
       delete obj:{objectId}
       delete path:{ownerId}:{fullPath}
       delete f:{fileHash} only when no active object still needs it
       create del:{chunkHash} for every now-unreferenced Chunk
  -> update in-memory catalog maps
  -> immediately wake DeleteScheduler
  -> return 204 No Content
```

### Directory deletion

```text
DELETE /api/v2/directories?path=/trip
  -> enumerate every descendant directory and active object
  -> calculate the union of potentially unreferenced Chunk hashes
  -> one LevelDB WriteBatch removes obj:/path:/dir: keys
  -> create one DeleteTask per unreferenced Chunk
  -> update in-memory maps only after the batch succeeds
  -> wake DeleteScheduler
```

Root directory deletion is rejected.

### Physical deletion dispatch

```text
DeleteScheduler sees del:{chunkHash}
  -> load ChunkRoute to find the original replica nodeIds
  -> pendingNodeIds contains nodes without DELETE_ACK
  -> for every ONLINE pending node:
       Gateway -> DataNode DELETE /internal/v2/chunks/{chunkHash}
  -> successful ACK:
       remove nodeId from pendingNodeIds using a LevelDB WriteBatch
  -> pendingNodeIds empty:
       delete del:{chunkHash}
       delete c:{chunkHash}
```

The scheduler runs periodically and is additionally woken by a successful node
heartbeat/registration transition to `ONLINE`. It uses existing asynchronous
HTTP so an EventLoop never blocks waiting for DataNode deletion.

## DataNode Flow

```text
DELETE /internal/v2/chunks/{chunkHash}
  -> lookup e:{chunkHash} in local physical_index LevelDB
  -> missing index: return 204 (idempotent success)
  -> delete e:{chunkHash}
  -> insert [offset, length] into FreeList and coalesce adjacent extents
  -> return 204
```

V2 only reuses the extent. It does not punch a filesystem hole, so `disk0.data`
may retain its apparent file size even though its regions are available for
future writes.

## FreeList Allocation

`FastDataStore::beginPut()` executes under its existing mutex:

1. If the hash already exists, create a discard session as today.
2. Otherwise find the smallest free extent whose length is at least the Chunk
   size (best fit).
3. Allocate from its beginning. Remove it if exactly consumed; otherwise keep
   the remaining tail in the FreeList.
4. If no fitting extent exists, allocate at `nextOffset_` and advance it.

Deleting an extent inserts it by offset and merges directly adjacent previous
and next extents. Repeated `DELETE_CHUNK` does not add it twice because the
physical index key is already absent.

## Permanently Lost Nodes

An offline machine cannot be physically erased. Its nodeId remains in
`pendingNodeIds` until it returns or an operator explicitly retires the node.
Future node-retirement work must remove that node from pending tasks and from
Chunk routes; a retired disk must be wiped before joining again with a new
nodeId.

## API Baseline

```text
DELETE /api/v2/objects/{objectId}
DELETE /api/v2/directories?path={absolutePath}

DELETE /internal/v2/chunks/{chunkHash}
```

The internal endpoint requires the existing cluster-internal authentication.

## Acceptance Criteria

1. Deleting an object removes it from its catalog immediately and future
   object/manifest requests return 404.
2. Deleting a non-root directory recursively removes its descendant catalog
   entries.
3. A shared Chunk remains readable while at least one active object references
   it.
4. A DataNode deletes an unreferenced Chunk once, returns success on a repeated
   delete, and exposes the released extent to the next fitting write.
5. If a replica is offline, its `DeleteTask` persists across Gateway restart;
   after the node returns online, it receives the pending delete.
