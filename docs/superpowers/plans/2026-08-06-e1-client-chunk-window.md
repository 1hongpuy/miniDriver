# E1 Client Chunk Window Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow one browser file upload to maintain a bounded two-chunk transfer window.

**Architecture:** Keep the existing V2 route and PUT protocol. Replace the serial per-file upload loop with two workers drawing from one pending chunk list, and render aggregate progress from confirmed and in-flight bytes.

**Tech Stack:** Browser JavaScript, existing HTTP API, Playwright Python test.

## Global Constraints

- Keep `MAX_PARALLEL_FILES = 2`.
- Set `MAX_INFLIGHT_CHUNKS_PER_FILE = 2`.
- Do not change Gateway, DataNode, Session, manifest, token, or route APIs.
- Do not preallocate routes beyond the active window.

---

### Task 1: Prove the serial uploader cannot overlap chunks

**Files:**
- Modify: `test/test_v2_upload_speed_ui.py`

- [x] Configure a three-chunk 12 MiB browser input.
- [x] Delay fake XHR completion and track `window.__maxActiveUploads`.
- [x] Assert the completed upload has average throughput and at least two
      overlapping XHR body uploads.
- [x] Run `python3 test/test_v2_upload_speed_ui.py` and verify the old serial
      implementation fails the overlap assertion.

### Task 2: Add a bounded per-file worker window

**Files:**
- Modify: `www-v2/app.js`

- [x] Add `MAX_INFLIGHT_CHUNKS_PER_FILE = 2`.
- [x] Split completed session chunks from pending chunks.
- [x] Run two asynchronous workers over the pending list.
- [x] Preserve one route request and one chunk PUT per chunk.
- [x] Aggregate progress over `confirmedBytes + inFlightBytes`.

### Task 3: Verify UI behavior and non-regression

**Files:**
- Test: `test/test_v2_upload_speed_ui.py`
- Test: `test/test_v2_thumbnail_contact_sheet_ui.py`

- [x] Run `python3 test/test_v2_upload_speed_ui.py`.
- [x] Run `python3 test/test_v2_thumbnail_contact_sheet_ui.py`.
- [x] Run `git diff --check`.
