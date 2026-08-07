# V2 Phase E: Browser Transfer Control Design

## 1. Scope

Phase E finishes the V2 browser upload control path. It improves how the
browser schedules already-defined V2 sessions and Chunk PUT requests. It does
not change the Chunk format, `manifestHash`, Session schema, upload token,
Gateway placement, or DataNode HTTP endpoint.

The goals are:

- keep a small amount of useful upload concurrency without overrunning a
  DataNode;
- resume a matching interrupted upload without re-sending committed Chunks;
- express capacity pressure as a visible waiting state instead of a generic
  failure;
- make browser behavior measurable before any V4 transport rewrite.

Phase E is not a Multi-Reactor, connection-pool, HTTP/2, gRPC, `sendfile`, or
adaptive-congestion-control project. Those belong to V4.

## 2. Existing V2 Contract

The browser first calculates a fixed 4 MiB Chunk manifest and its
`manifestHash`:

```text
Browser
  -> POST /api/v2/upload/preflight(manifest)
  -> Gateway returns CONTENT_EXISTS or UPLOAD_REQUIRED(sessionId, completed)
  -> POST /api/v2/upload/sessions/:id/routes for a small set of Chunks
  -> PUT http://DataNode/v2/chunks/:chunkHash
  -> Primary streams to replicas and commits the Chunk to Gateway
  -> POST /api/v2/upload/sessions/:id/commit
```

`s:{sessionId}` is the Gateway's durable upload ledger. It records the
manifest identity and completed Chunk indexes. `localStorage` is only a browser
pointer to that server-side ledger; losing browser storage does not destroy a
valid Gateway Session.

The current browser key is:

```text
minikv-v2:session:<file.name>:<file.size>:<file.lastModified>:<directory>
```

Before reusing it, the browser compares its saved `manifestHash` with the
newly calculated manifest and with the Gateway Session's `manifestHash`.
Mismatch, a missing Session, or an invalid response removes the local pointer
and starts a normal preflight request. This is the existing `resumeOrPreflight`
behavior in `www-v2/app.js`.

## 3. E1: Bounded Chunk Window

### Purpose

The old browser sent one 4 MiB Chunk, waited for the DataNode response and
Gateway commit, then requested the route for the next Chunk. Network and
control-plane latency therefore created an idle gap after every Chunk.

E1 keeps a bounded window:

```text
pending Chunk indexes
  -> at most two per-file workers
  -> request route immediately before its PUT
  -> PUT the Chunk directly to its primary DataNode
  -> on success, take the next pending Chunk
```

Initial constants are deliberately static:

```text
MAX_PARALLEL_FILES             = 1
MAX_INFLIGHT_CHUNKS_PER_FILE   = 2
MAX_ACTIVE_CHUNK_UPLOADS       = 2
```

The global scheduler is the final browser-side limit. It counts a route
request and its Chunk PUT as one active transfer slot. The current conservative
choice permits either two Chunk uploads for one file or one upload while a
second Chunk is preparing; it does not permit multiple files to multiply
DataNode write pressure.

### Progress semantics

Visible progress is:

```text
confirmed bytes from Gateway/DataNode responses
+ latest received progress of each in-flight PUT
```

A Chunk is confirmed only after its PUT receives a successful HTTP response.
Displayed bytes are not a durable acknowledgement while a PUT is still active.

## 4. E2: Resume and Re-entry

### Required behavior

Refreshing the page, closing the tab, or losing the network may abandon HTTP
requests while the Gateway has already committed some Chunks. When the same
local file is selected again in the same logical directory:

1. Rebuild the manifest. Hashing remains necessary because file name and size
   are not a content identity.
2. Read the local Session pointer.
3. Query `GET /api/v2/upload/sessions/:sessionId`.
4. Reuse it only when both browser and Gateway `manifestHash` values match.
5. Mark returned `completed` indexes as confirmed and only schedule the other
   indexes.
6. Remove the local pointer only after final file commit, or after the Session
   is conclusively invalid.

This basic behavior already exists. E2 completes its operational rules rather
than introducing a second resume protocol.

### Re-entry rules to implement

- A file identity may have only one active browser upload controller at a
  time. A repeated click or duplicate file selection joins or reports the
  existing operation; it must not start another session worker.
- An aborted browser PUT is not assumed failed or completed. On the next
  resume, Gateway Session state is authoritative.
- `404` or manifest mismatch invalidates only the local session pointer, then
  preflight creates a fresh Session.
- `409 PATH_CONFLICT` is terminal for that logical path. It is not a capacity
  retry and must not continue sending Chunks.
- A successful `CONTENT_EXISTS` result creates/reuses the logical object
  according to Gateway metadata semantics and transmits zero Chunk bytes.

## 5. E3: Capacity Waiting and Queue State

DataNode `503` means write capacity is temporarily unavailable. It is neither
a CORS failure nor proof that a Chunk is invalid. The browser must retain the
file Session and retry the same unconfirmed Chunk after a bounded delay.

### States

```text
QUEUED
  -> HASHING
  -> RESUMING | PREFLIGHTING
  -> WAITING_SLOT
  -> ROUTING
  -> UPLOADING
  -> WAITING_CAPACITY
  -> COMMITTING
  -> COMPLETED

terminal: FAILED | CANCELLED
```

`WAITING_CAPACITY` applies only to retryable capacity responses, initially
HTTP 503. It releases the global transfer slot before sleeping, so it cannot
hold the browser scheduler hostage. Retry delay is:

```text
max(Retry-After, exponential backoff), capped at 30 seconds
```

The initial cap of 120 retries is an operational guard, not an availability
guarantee. Reaching it produces a clear failed state while preserving the
Gateway Session for a later manual resume.

### Fairness policy

V2 keeps `MAX_PARALLEL_FILES = 1` until measured evidence supports raising
it. This prevents a second file from repeatedly taking a DataNode admission
slot while a first file is recovering from a capacity response.

When a later V2 deployment raises file concurrency, the scheduler must use a
round-robin ready-file queue: one Chunk from each eligible file before giving a
second Chunk to the same file. This rule is optional for V2 and required
before enabling `MAX_PARALLEL_FILES > 1`.

## 6. E4: Verification and Tuning

E4 changes no protocol. It establishes evidence for the fixed limits above.

### Browser tests

- E1 test: two 4 MiB Chunks overlap, active direct PUTs never exceed two, and
  completed entries show average throughput.
- Resume test: a Session with selected committed indexes schedules only missing
  indexes after a simulated page reload.
- Capacity test: a simulated 503 with `Retry-After` returns the Chunk to
  `WAITING_CAPACITY`, releases its slot, and later succeeds.
- Conflict test: `409 PATH_CONFLICT` stops the file before any DataNode PUT.

### Real-cluster matrix

Run each point with a new test directory and record Gateway/DataNode structured
logs:

| Case | Files | Global PUT slots | Per-file window | Expected result |
| --- | ---: | ---: | ---: | --- |
| Baseline | 1 | 1 | 1 | serial reference |
| E1 | 1 | 2 | 2 | lower idle gap, no 503 |
| Capacity | 2 | 2 | 2 | no terminal capacity failure |
| Stress | 4 | 2 | 2 | queued files finish sequentially |

For each run collect MiB/s, Chunk P50/P95/P99 completion time, `503` count,
retry count, DataNode disk queue peak, disk pause count, replica pause count,
and final file state. A larger window is rejected if it increases failures or
tail latency without a meaningful throughput gain.

## 7. Completion Criteria

Phase E is complete when:

- a selected identical file resumes against its matching Gateway Session;
- no more than two direct Chunk PUTs exist in the browser at once;
- temporary DataNode capacity produces a bounded waiting/retry flow rather
  than an immediate generic failure;
- repeated selection cannot create competing upload controllers for one file
  identity;
- the E4 matrix is recorded with reproducible commands and structured-log
  evidence.

## 8. Explicit Handoff to V4 and V5

V4 replaces internal execution mechanisms while preserving all above protocol
and Session semantics:

```text
single EventLoop -> multi Reactor
short replica HTTP -> replica connection pool
fixed client window -> adaptive, measured flow control
basic disk executor -> read/write prioritised disk scheduler
buffered download -> pread/sendfile path
single metadata writes -> batched metadata commits
```

V5 adds persistent recovery and cluster reliability:

```text
DataNode index WAL and snapshot recovery
FreeList persistence, GC, scrub and Repair
multi-Gateway metadata coordination and user authorization
```
