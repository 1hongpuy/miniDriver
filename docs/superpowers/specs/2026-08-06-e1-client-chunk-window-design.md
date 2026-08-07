# E1 Client Chunk Window Design

## Goal

Remove the idle control-plane gap between consecutive chunks of one uploaded
file without changing the V2 Session, token, hash, or DataNode upload protocol.

## Current Behavior

`www-v2/app.js` uploaded one chunk with `await` before requesting the route for
the next chunk. A 4 MiB chunk therefore had to finish local write, replica, and
Gateway commit before the browser could start the next data transfer.

## Design

Each file keeps at most two chunk uploads in flight:

```text
pending chunks
  -> two browser workers
  -> request one existing /routes lease
  -> PUT existing /v2/chunks/:hash endpoint
  -> on completion, the worker takes the next pending chunk
```

The global file worker limit remains two. The theoretical browser maximum is
therefore four active chunk PUTs. This is deliberately conservative for the
current DataNode write admission limits and bounded disk queue.

Routes are still requested when a chunk enters the small window. V2 does not
request routes for the whole file because placement leases are short-lived and
large, slow uploads could otherwise receive expired routes.

## Progress and Failure Semantics

- Progress is the sum of confirmed bytes and the latest body progress of every
  in-flight chunk.
- A chunk is counted as confirmed only after its existing HTTP PUT completes.
- A route allocation or upload failure rejects the file upload; the existing
  persisted session still records already committed chunks for a later retry.
- No DataNode, Gateway, manifest, capability token, or chunk-format change is
  required.

## Verification

`test/test_v2_upload_speed_ui.py` uses three 4 MiB test chunks and delayed
fake XHR requests. It asserts that at least two chunk uploads overlap and that
the completed queue entry still exposes average throughput.
