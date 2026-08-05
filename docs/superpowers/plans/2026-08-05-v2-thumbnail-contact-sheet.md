# V2 Thumbnail Contact Sheet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render JPEG thumbnail contact sheets in the V2 catalog using D3 derived-object metadata.

**Architecture:** Keep directory rows unchanged and introduce file-card rendering in `www-v2/app.js`.
Each ready thumbnail is loaded through existing manifest/Chunk verification helpers and cached as a
revocable browser object URL for the current catalog only.

**Tech Stack:** Static HTML/CSS/JavaScript, Gateway V2 JSON APIs, browser Fetch/Blob APIs,
Playwright Python UI test.

## Global Constraints

- Use `www-v2/` as the V2 frontend source; do not edit legacy `www/`.
- Only `thumbnail.state === "READY"` and a non-empty `thumbnail.objectId` may trigger a derived object request.
- Preserve the existing original preview, download, delete, and catalog APIs.

---

### Task 1: Test D3 thumbnail consumption

**Files:**
- Create: `test/test_v2_thumbnail_contact_sheet_ui.py`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: Write the failing Playwright test**

Mock one catalog JPEG with a READY thumbnail, one pending JPEG, derived object metadata, a
derived manifest and a derived Chunk. Assert `.catalog-card__image` is rendered for READY, the
pending card shows its state, and no request targets the pending item's derived object.

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 test/test_v2_thumbnail_contact_sheet_ui.py`

Expected: FAIL because current rows do not have `.catalog-card` or derived thumbnail loading.

- [ ] **Step 3: Register the test with CTest**

Add `add_test(NAME v2_thumbnail_contact_sheet_ui COMMAND python3 ... )` beside existing V2 UI
tests.

### Task 2: Implement catalog contact-sheet rendering

**Files:**
- Modify: `www-v2/app.js`
- Modify: `www-v2/style.css`

- [ ] **Step 1: Add per-catalog thumbnail URL lifecycle**

Add a map keyed by derived object ID. Revoke all object URLs before replacing a catalog and after
an image download failure. Reuse `request`, `fetchVerifiedChunk`, and `previewMimeType`.

- [ ] **Step 2: Render file cards and state placeholders**

Replace file-row creation with cards. READY cards asynchronously load the derived object; pending,
failed, and unsupported states remain local placeholders. Card click opens the original preview,
while action buttons stop propagation.

- [ ] **Step 3: Add responsive contact-sheet CSS**

Use a stable CSS grid, fixed media aspect ratio, explicit overflow behavior, and existing muted,
safe, amber, and danger colors. Keep directory rows and mobile styles intact.

- [ ] **Step 4: Run the new UI test**

Run: `python3 test/test_v2_thumbnail_contact_sheet_ui.py`

Expected: PASS.

### Task 3: Verify and deploy frontend assets

**Files:**
- Modify: `www-v2/index.html` only if a semantic card template is necessary.

- [ ] **Step 1: Build and run relevant tests**

Run: `cmake --build build -j2 && ctest --test-dir build -R 'v2_thumbnail_contact_sheet_ui|v2_upload_speed_ui|v2_delete_ui' --output-on-failure`.

- [ ] **Step 2: Copy the validated source to the dedicated Nginx root**

Run: `sudo rsync -a --delete www-v2/ /opt/minikv-v2/www/ && sudo nginx -t && sudo systemctl reload nginx`.

- [ ] **Step 3: Commit**

Run: `git add www-v2 test/test_v2_thumbnail_contact_sheet_ui.py CMakeLists.txt docs/superpowers && git commit -m "feat: render V2 thumbnail contact sheet"`.
