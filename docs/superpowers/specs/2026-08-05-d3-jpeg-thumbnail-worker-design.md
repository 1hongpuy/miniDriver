# V2 D3 JPEG Thumbnail Worker Design

## Goal

Generate `thumb-512-jpeg-v1` for committed `.jpg` and `.jpeg` source objects without blocking
Gateway or DataNode upload paths. The result is stored as a normal V2 object, but is private
derived metadata rather than a user-visible catalog entry.

## Scope

This D3 increment implements only JPEG input and one 512px JPEG profile. It does not implement
RAW previews, PNG, video frames, AI tags, vector indexing, a 2048px preview, or automatic
backfill of historical objects.

## Process Boundary

`minikv_v2_thumbnail_worker` is an independent process managed by `node_agent`:

```text
Redis consumer thread
  -> bounded job queue
  -> worker EventLoop / Async HTTP state machine
  -> one JPEG CPU executor
```

The Redis consumer blocks only in its own thread. JPEG decode/resize happens only in the CPU
executor. Gateway and DataNode retain their own EventLoop instances and do not share memory with
the Worker process.

## Redis and Lease Flow

```text
Gateway Publisher XADD media:thumbnail {jobId,type,profile}
  -> XREADGROUP thumbnail-workers <consumer-name>
  -> POST Gateway /internal/v2/media/jobs/{jobId}/claim
  -> process source and upload derived object
  -> POST complete or fail with leaseToken
  -> XACK only after Gateway accepted the outcome
```

The Worker creates the consumer group with `MKSTREAM`. It periodically calls `XAUTOCLAIM` for
pending messages idle longer than the configured reclaim time. If `claim` returns HTTP 409, the
message is stale or already owned and is acknowledged. On a retryable failure, the Worker first
records `fail` in Gateway, then acknowledges; Gateway's durable retry time is responsible for a
new `XADD`.

The initial JPEG lease is 300 seconds. A single JPEG job is limited to one local execution, so a
lease extension API is not required in this version. Long AI or video tasks will need lease renewal
later.

## Internal Derived Object Upload

The Worker must not use the browser-facing upload APIs and must not access a DataNode disk path.
Gateway adds internal endpoints that create and complete a derived upload session:

```text
POST /internal/v2/media/jobs/{jobId}/derived-uploads
  leaseToken, fileName, fileSize, chunkSize, manifestHash, chunks
  -> sessionId, missingChunks

POST /internal/v2/media/jobs/{jobId}/derived-uploads/{sessionId}/commit
  leaseToken
  -> derivedObjectId, derivedFileHash
```

The session uses existing route allocation, upload capability tokens, DataNode Chunk PUT, and
chunk commits. The internal commit writes `FileMeta` plus an `ObjectMeta`, but deliberately does
not write a user `path:` key or catalog record. The only reference is
`ThumbnailMeta.derivedObjectId` after media-job completion.

`fileHash` is the ordinary V2 manifest hash. Identical thumbnails can deduplicate Chunk storage,
but every source/profile completion receives its own private derived object ID.

## JPEG Pipeline

```text
claim result sourceFileHash
  -> Gateway manifest
  -> download source chunks from a healthy listed replica into tempDir
  -> sha256 check every downloaded chunk
  -> stb_image JPEG decode
  -> reject file > 100 MiB, dimension > 16000, or pixels > 80,000,000
  -> preserve dimensions when max edge <= 512; never upscale
  -> stb_image_resize2 downscale to max edge 512
  -> stb_image_write JPEG quality 82
  -> internal derived upload and complete
```

Temporary files are created under configured `tempDir`, include the job ID, and are removed after
success, permanent unsupported failure, or retryable failure. Files remain only if the process
crashes; D3 does not add temp-file scavenging.

## Gateway Catalog API

Each catalog file item may include:

```json
"thumbnail": {
  "profile": "thumb-512-jpeg-v1",
  "state": "PENDING|RUNNING|READY|FAILED|UNSUPPORTED",
  "objectId": "..."
}
```

Catalog lookup checks the source file hash's `ThumbnailMeta`. A non-ready entry never causes an
original image download. `READY` provides the private derived object ID; the frontend later loads
its ordinary object manifest and Chunk bytes.

## Configuration

```yaml
node:
  capabilities: [storage, thumbnail]

services:
  - id: thumbnail-worker-0
    type: thumbnail_worker
    enabled: true
    dataDir: /data/minikv/thumbnail-worker
    tempDir: /data/minikv/tmp/thumbnail
    maxConcurrentJobs: 1
```

The Worker needs Gateway address/port, Redis configuration, cluster secret, and its own logs.
Only nodes that declare and run this service consume `thumbnail-workers` messages.

## Validation

1. A JPEG commit produces one Redis message and one `PENDING` task.
2. Worker creates a 512px-or-smaller JPEG and marks task `READY`.
3. Repeated Stream messages cannot create multiple active jobs because Gateway claim is lease-bound.
4. A non-JPEG source creates no thumbnail task.
5. Oversized dimensions produce `UNSUPPORTED`; retryable transport errors produce `FAILED` then
   a Gateway-driven retry.
6. Catalog returns thumbnail state and the private derived object ID only when ready.
