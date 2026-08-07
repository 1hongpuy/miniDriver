# Object Preview Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every catalog entry independently actionable and add an image preview overlay with zoom controls.

**Architecture:** The catalog list remains the navigation surface. Selecting an object opens a fixed modal overlay instead of adding detail content below the list. The browser resolves the existing object manifest, verifies and concatenates chunks, then renders only browser-decodable image MIME types; RAW and video remain download-only.

**Tech Stack:** Existing vanilla JavaScript, CSS, object manifest API, browser Blob/Object URL APIs, Playwright.

## Global Constraints

- Do not change Gateway or DataNode APIs.
- Keep SHA-256 verification for every downloaded chunk.
- Never create DOM from a file name through `innerHTML`.
- JPEG, PNG, GIF, WebP and AVIF can preview; other files expose download only.
- Preview object URLs must be revoked when the overlay closes or is replaced.

---

### Task 1: Add a failing browser interaction check

**Files:**
- Create: `/tmp/minikv_preview_ui_check.py` during verification
- Modify: `www-v2/index.html`
- Modify: `www-v2/app.js`
- Modify: `www-v2/style.css`

- [x] **Step 1: Create an interaction check**

The check must mock `GET /api/v2/objects/object-image`, `GET /api/v2/objects/object-image/manifest`, and the direct Chunk request. It must assert that clicking either the first or second file row opens `#object-preview`, and that zoom changes the preview transform.

- [x] **Step 2: Run it before implementation**

Run it against `python3 -m http.server` serving `www-v2`. Expected: fail because `#object-preview` does not exist.

### Task 2: Replace inline object details with an overlay

**Files:**
- Modify: `www-v2/index.html`
- Modify: `www-v2/app.js`
- Modify: `www-v2/style.css`

- [x] **Step 1: Add overlay markup**

Add a hidden `#object-preview` dialog after the workbench. It includes close, zoom out, fit, zoom in and download controls, metadata, status text, and a preview stage.

- [x] **Step 2: Implement the preview state**

Replace `#object-detail` mutation with `openObjectPreview(objectId)`. It fetches object metadata, determines preview eligibility from the filename extension, fetches verified chunks only for supported images, and makes one Blob URL for the image element.

- [x] **Step 3: Implement file-row actions**

Each catalog file row receives an independent preview button and download button. Click propagation is stopped for the download command. The full row still opens the preview so no row is covered by a persistent detail block.

- [x] **Step 4: Add zoom and cleanup behavior**

Keep zoom in state, clamp it between 25% and 400%, update `transform: scale(...)`, reset on a new object, support Escape close, and revoke the object URL on close.

- [x] **Step 5: Run Playwright verification**

Expected: both catalog rows are clickable; the first image opens, image Blob renders, zoom changes, and no page errors occur.
