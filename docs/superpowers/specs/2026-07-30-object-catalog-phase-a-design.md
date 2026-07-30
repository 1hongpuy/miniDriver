# Object Catalog Phase A Design

**Goal:** Make Gateway-owned logical folders and objects the source of truth, then replace the V2 browser's local recent-file list with a navigable material library.

## Scope

Phase A implements only these workflows:

```text
create directory
  -> browse directory tree and direct children
  -> upload a file into the active directory
  -> list uploaded objects after browser refresh or on a different client
  -> inspect file metadata and download it
```

Deletion, rename, move, versions, tombstones, GC, thumbnail jobs, AI metadata and operation history are explicitly outside this phase.

## Ownership Boundaries

```text
Gateway
  owns Directory, PathEntry, ObjectMeta and the mapping to a content manifest.

DataNode
  owns Chunk bytes and chunkHash -> physical extent only.

Browser
  reads the catalog from Gateway; it never uses localStorage as the source of
  truth for the material library.
```

`fileHash` remains the existing manifest/content identifier. `objectId` is a new opaque logical identity. Two uploads of identical content create two `objectId` values and two directory entries, while both may refer to the same `fileHash` and Chunk set.

## Metadata Model

All records are owned by the single Gateway LevelDB. `ownerId` remains fixed to `admin`, but remains part of every key.

```text
dir:{ownerId}:{normalizedDirPath}
  -> DirectoryMeta { path, createdAt }

path:{ownerId}:{normalizedAbsoluteFilePath}
  -> objectId

obj:{objectId}
  -> ObjectMeta {
       objectId, ownerId, parentPath, name, fileHash,
       fileSize, contentType, state, createdAt
     }

f:{fileHash}
  -> existing FileMeta / manifest record

c:{chunkHash}
  -> existing ChunkRoute record
```

The root `/` is implicit and is never stored as `dir:`. All paths must be absolute, normalized, free of `.` and `..`, and cannot contain NUL. A directory name is one non-empty path segment. Phase A rejects duplicate names in the same directory.

`ObjectMeta.fileHash` is not exposed as a user-facing path identity. It is returned only where the current download manifest protocol requires it.

## Compatibility

Existing `f:{fileHash}` records remain readable through the legacy `/api/v2/files/{fileHash}/manifest` endpoint.

On Gateway startup, each legacy FileMeta with no matching `path:` entry is backfilled once as:

```text
objectId = "legacy-" + fileHash
parentPath = normalized(FileMeta.dirPath), default "/"
name = FileMeta.fileName
```

This preserves every legacy record that is still representable by the old model. If two historical entries already overwrote one another under the same `f:{fileHash}`, the lost path cannot be reconstructed; Phase A does not pretend otherwise.

## Gateway API

### List a directory

```text
GET /api/v2/catalog?path=/2026/Xian
```

Returns direct children only, sorted with directories before files and then by name:

```json
{
  "path": "/2026/Xian",
  "breadcrumbs": [
    {"name": "root", "path": "/"},
    {"name": "2026", "path": "/2026"},
    {"name": "Xian", "path": "/2026/Xian"}
  ],
  "directories": [{"name": "sunset", "path": "/2026/Xian/sunset"}],
  "files": [{
    "objectId": "...",
    "name": "DSC_0001.NEF",
    "size": 58412345,
    "state": "AVAILABLE",
    "createdAt": 1780000000
  }]
}
```

The response contains no Chunk route, node address or DataNode physical information.

### Create a directory

```text
POST /api/v2/directories
{"parentPath":"/2026", "name":"Xian"}
```

The parent must exist, except `/`, which is implicit. Duplicate directory or file names in the same parent return `409 Conflict`.

### Object lookup and download manifest

```text
GET /api/v2/objects/{objectId}
GET /api/v2/objects/{objectId}/manifest
```

The object endpoint returns file metadata. The manifest endpoint resolves `objectId -> fileHash`, then reuses the existing manifest snapshot logic. The old `GET /api/v2/files/{fileHash}/manifest` stays available for compatibility and benchmark tooling.

### Upload commit

`POST /api/v2/upload/sessions/{sessionId}/commit` creates the existing FileMeta and one ObjectMeta/PathEntry atomically. Its response includes both IDs:

```json
{"objectId":"...", "fileHash":"...", "state":"AVAILABLE"}
```

If another path entry already exists under `SessionState.dirPath + fileName`, commit returns `409 Conflict`; no new logical object is visible. The uploaded Chunk data remains unreferenced until a later cleanup feature handles it.

## Transaction Rules

Gateway uses one LevelDB `WriteBatch` whenever it changes a catalog record together with its index:

```text
create directory: dir:
commit file:      f: + obj: + path:
legacy backfill:  obj: + path: (+ parent dir: records as required)
```

The in-memory maps are updated only after a successful WriteBatch. Directory listing is implemented with persistent prefix scans and uses an LRU only in a later phase; Phase A does not add an unbounded catalog cache.

## Browser Design

`www-v2` becomes a small single-page application with two views, controlled by client-side state instead of a router dependency:

```text
Upload view
  active directory breadcrumb
  upload queue
  starts sessions with activeDirectory as dirPath

Library view
  left: directory tree and create-directory control
  center: breadcrumb, direct folder/file list, selected object detail
  action: download selected object
```

The old "recent files" panel and `minikv-v2:recent-files` localStorage source are removed from the primary workflow. A future history drawer may consume a Gateway event API, but Phase A does not create one.

## Error Behavior

```text
400  malformed or non-normalized path/name
404  requested directory or object does not exist
409  duplicate file/directory path on create or commit
503  unchanged: write capacity is temporarily exhausted
```

The browser renders an inline action error, preserves the current directory and never erases an upload queue solely because catalog refresh failed.

## Tests and Acceptance

Backend tests cover:

1. root and nested directory creation, duplicate collision and invalid path rejection;
2. listing direct children without recursively returning grandchildren;
3. file commit creates an ObjectMeta and path entry, and duplicate path commit is rejected;
4. object manifest is identical to the legacy fileHash manifest;
5. startup backfill exposes a legacy FileMeta in catalog listing.

Manual browser verification covers:

1. create `/2026/Xian`, navigate by tree and breadcrumb;
2. upload into `/2026/Xian`, refresh the page and still see the object;
3. download the selected object;
4. open the page from a second browser and see the same catalog.
