# V2 Direct Delete UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task.

**Goal:** Expose direct file and directory deletion in the V2 catalog, then merge the verified backend deletion branch and publish the complete V2 change set.

**Architecture:** The browser remains a thin catalog client. A file action calls `DELETE /api/v2/objects/{objectId}`; a directory action calls `DELETE /api/v2/directories?path={path}`. The Gateway owns reference checks and async physical deletion, so the browser only confirms intent and refreshes the current catalog.

**Tech Stack:** Static HTML/CSS/JavaScript, existing V2 HTTP API, CMake/CTest, Playwright browser verification.

## Global Constraints

- Keep direct-delete semantics: no tombstone, recycle bin, or recovery UI.
- Use `window.confirm` before destructive requests.
- Preserve the existing `www-v2` preview and parallel upload work.
- Do not modify runtime `data/` or existing profiling artifacts.

### Task 1: Backend merge

**Files:**
- Merge: `feat/v2-direct-delete` into local `v2`

- [ ] Merge the three verified commits that add FreeList, DeleteTask persistence, DataNode internal delete, and Gateway retry dispatch.
- [ ] Build the repository and run the complete CTest suite.

### Task 2: Delete controls

**Files:**
- Modify: `www-v2/index.html`
- Modify: `www-v2/app.js`
- Modify: `www-v2/style.css`
- Test: `test/test_v2_delete_ui.js`

- [ ] Write a browser test that stubs `fetch` and asserts a file delete sends `DELETE /api/v2/objects/{id}` after confirmation, then reloads the current catalog.
- [ ] Run it before implementation and observe the missing delete-control failure.
- [ ] Add a compact directory-delete command in the catalog toolbar, except at `/`; add a compact per-file delete command.
- [ ] Implement `deleteObject()` and `deleteActiveDirectory()` with confirmation, disabled command states, API error reporting, and refresh to the parent after directory deletion.
- [ ] Run the browser test again and verify it passes.

### Task 3: Regression and publish

**Files:**
- Modify: `www-v2/index.html`, `www-v2/app.js`, `www-v2/style.css`

- [ ] Build with CMake and run all CTest tests.
- [ ] Exercise catalog navigation and both delete commands in Playwright using mocked API responses; capture a screenshot.
- [ ] Commit only V2 backend, frontend, test, and documentation files.
- [ ] Push local `v2` to `origin/v2`.
