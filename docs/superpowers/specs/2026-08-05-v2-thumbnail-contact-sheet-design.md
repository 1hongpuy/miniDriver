# V2 Thumbnail Contact Sheet Design

## Goal

Display D3 JPEG thumbnails in the V2 catalog without changing upload, deletion, preview, or
object-download semantics.

## Interaction

- Directories remain compact rows at the top of the catalog.
- Files render as a responsive contact sheet beneath directories.
- A `READY` thumbnail loads the private derived `objectId` through the existing manifest and
  Chunk GET path, validates every Chunk, then assigns a local object URL to an image.
- `PENDING` and `RUNNING` show a neutral generation state. `FAILED` and `UNSUPPORTED` show a
  stable file-type state without requesting the original object.
- Selecting a card opens the existing original-object preview. Download and delete remain visible
  card actions and retain their existing API calls.

## Boundaries

- The browser never receives a DataNode offset or LevelDB detail.
- Thumbnail fetches are best-effort: a failed thumbnail changes only its own visual state and must
  not prevent catalog rendering or original-file actions.
- Object URLs are cached only for the currently rendered catalog and revoked before a new catalog
  replaces the grid.
- No polling is added in this increment. Refreshing the catalog observes a task transition to
  `READY`; task-state polling can be added later if needed.

## Validation

A Playwright test stubs the catalog, derived-object metadata, manifest, and Chunk endpoint. It
asserts a READY image is rendered from the derived object, a pending item never fetches a derived
object, and card actions still invoke original object APIs.
