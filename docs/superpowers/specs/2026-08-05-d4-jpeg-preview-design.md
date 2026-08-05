# V2 D4 JPEG Preview Design

## Goal

Generate a private `preview-2048-jpeg-v1` derived object for every newly committed JPEG at the
same time as `thumb-512-jpeg-v1`. The browser preview uses this derived object instead of the
full source object, while the explicit download command continues to retrieve the original file.

## Scope

This increment supports `.jpg` and `.jpeg` source objects only. It reuses the existing Gateway
media-job ledger, Redis Stream, `minikv_v2_thumbnail_worker`, derived-upload protocol, and JPEG
decode/resize implementation. It does not introduce LibRaw, full RAW decoding, video frames,
PNG input, a second Worker binary, a second Redis Stream, or a historical-object backfill scan.

## Two Independent Profiles

After a JPEG file commit, Gateway creates both durable profile records and publishes each job
only when its profile needs publishing:

```text
thumb-512-jpeg-v1       catalog contact sheet, max edge 512px, quality 82
preview-2048-jpeg-v1   interactive preview, max edge 2048px, quality 88
```

The keys remain profile-specific:

```text
d:{sourceFileHash}:thumbnail:thumb-512-jpeg-v1
d:{sourceFileHash}:thumbnail:preview-2048-jpeg-v1
```

Each profile has its own job, lease, state, retry record, derived object ID, and error. A 512px
success must remain usable when a 2048px preview fails, and vice versa. Repeated file commit or
repeated Redis delivery does not create an active duplicate because the existing profile/job
lease checks remain authoritative.

## Worker Boundary

There remains exactly one `minikv_v2_thumbnail_worker` binary and one process type. The claimed
job profile selects output dimensions and quality. The Worker still runs no more than
`maxConcurrentJobs` jobs and its conversion work remains off its EventLoop.

```text
Redis message { jobId, type=thumbnail, profile }
  -> Gateway claim validates profile and lease
  -> Worker downloads the source through manifest + verified Chunk GET
  -> JPEG decoder and resizer use the claimed profile
  -> Worker uploads a normal private derived object
  -> Gateway completes only that source/profile job
```

The source file is downloaded separately for the two jobs in D4. This deliberately avoids
introducing a shared temporary-file cache and its eviction, ownership, and crash-cleanup rules.
That optimization may be added after profiling proves the duplicate local read meaningful.

## Catalog Contract

Catalog file entries expose both records independently. `objectId` is present only for a ready
profile:

```json
{
  "thumbnail": {
    "profile": "thumb-512-jpeg-v1",
    "state": "READY",
    "objectId": "derived-thumbnail-object"
  },
  "preview": {
    "profile": "preview-2048-jpeg-v1",
    "state": "PENDING|RUNNING|READY|FAILED|UNSUPPORTED",
    "objectId": "derived-preview-object-when-ready"
  }
}
```

For files created before D4, `preview` is absent. This preserves the existing original-object
preview as a compatibility fallback. For a D4 JPEG whose preview record is `PENDING` or
`RUNNING`, the UI reports that the preview is generating and does not fetch the original file.
For `FAILED` or `UNSUPPORTED`, the UI reports the state and retains the explicit original-file
download command.

## Browser Preview Rules

```text
preview READY + objectId  -> verified download of preview object and show it
preview PENDING/RUNNING   -> show generation state; no source fetch
preview FAILED/UNSUPPORTED -> show status; offer original download only
preview absent            -> legacy fallback to the original-object preview path
```

The browser uses the same manifest and SHA-256 verified Chunk GET path as the thumbnail contact
sheet. It never sees DataNode extents or uses a direct local file path.

## RAW Follow-up: D5

`minikv_v2_thumbnail_worker` is also the future process boundary for RAW. D5 adds a format
generator selected by source extension:

```text
NEF/CR2/ARW/DNG -> LibRaw extracts embedded JPEG preview
                 -> existing JPEG output profiles (512 and 2048)
```

If no embedded preview can be extracted, D5 marks each profile `UNSUPPORTED` or `FAILED` without
changing source object availability. Full RAW demosaic is explicitly out of scope because it has
materially different CPU, memory, colour-management, and timeout requirements.

## Validation

1. A JPEG commit produces exactly two profile records and two Redis publications.
2. The Worker generates a 512px maximum-edge image at quality 82 and a 2048px maximum-edge image
   at quality 88, never enlarging either source image.
3. Catalog reports both profile states independently and only exposes ready derived IDs.
4. Clicking a D4 ready preview fetches the preview manifest, never the source manifest.
5. Clicking a D4 pending preview does not request either preview or source bytes.
6. A pre-D4 catalog item without `preview` retains the existing source-preview fallback.
