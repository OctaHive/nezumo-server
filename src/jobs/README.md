# Background jobs

This module contains the backend's in-process maintenance jobs and board export
renderer integration. The recurring jobs are started from `core/server.rs` and
share the same `Arc<AppState>` as HTTP and WebSocket handlers.

There is no separate worker process or persistent job queue. A backend restart
stops all running tasks; recurring work resumes when the next process starts.

## Module layout

| File | Responsibility |
| --- | --- |
| [`session_cleanup.rs`](./session_cleanup.rs) | Removes Redis sessions whose heartbeat expired and broadcasts updated presence. |
| [`previews.rs`](./previews.rs) | Generates and stores board thumbnails and implements PNG, JPEG, SVG, and PDF exports. |
| [`preview_service.rs`](./preview_service.rs) | Owns the long-lived native renderer process and serializes raster render requests through an actor. |
| [`yrs_compaction.rs`](./yrs_compaction.rs) | Publishes immutable canonical Yrs checkpoints from eligible journal tails. |
| [`yrs_retention.rs`](./yrs_retention.rs) | Optionally prunes canonical updates and obsolete events covered by verified immutable checkpoints. |
| [`mod.rs`](./mod.rs) | Exposes the job modules. |

## Jobs started with the server

### Board preview refresh

`start_preview_job` scans canonical boards every `PREVIEW_JOB_INTERVAL_SECS`
seconds. Missing thumbnails are generated immediately. An existing thumbnail is
regenerated only when canonical board state changed after the last successful
render and it is at least `PREVIEW_INTERVAL_SECS` old. Each scan processes at
most `PREVIEW_JOB_BATCH_LIMIT` boards, and missed ticks are skipped.

The preview job reads the current state through the canonical coordinator, so
thumbnail generation is no longer coupled to the removed legacy JSON snapshot
job. Failures are logged and retried by a later scan.

### Canonical Yrs compaction

`start_yrs_compaction_job` periodically selects a bounded set of canonical
boards whose journal tail exceeds the configured update-count or byte
threshold. It validates the active writer lineage, compacts the ordered tail,
and publishes an immutable binary checkpoint. Missed ticks are skipped.

Journal deletion is a separate opt-in job enabled by
`YRS_JOURNAL_GC_ENABLED`. It removes canonical update rows and then their
obsolete `board_events` records only when they are covered by verified immutable
checkpoints and older than the configured retention period. Both bounded deletes
commit atomically; events with a retained canonical update are never removed.

### Session cleanup job

`start_session_cleanup_job` is currently started every 30 seconds. Its API
enforces a minimum interval of five seconds and also skips missed ticks.

For every Redis key matching `board:*:sessions`, a cycle removes members whose
60-second heartbeat key has expired. It also removes stale cursor and anonymous
name data, rebuilds the board's enriched presence list, and broadcasts a
`sessions_update` WebSocket message. Invalid non-UUID board IDs are cleaned in
Redis but do not produce a broadcast.

See [`../cache/README.md`](../cache/README.md) for the presence key schema and
TTL details.

## Preview and export rendering

Raster previews and on-demand PNG/JPEG exports share a process-wide
`PreviewService`. The service starts lazily on the first request and launches:

```text
nezumo-render --serve <asset-base-url> <default-max-px>
```

Requests enter a bounded channel with capacity 64 and are rendered serially by
one warm daemon. The daemon exchanges line-delimited JSON over stdin/stdout and
uses temporary files for snapshots and encoded output. If spawning, rendering,
protocol parsing, EOF, or a timeout fails, the current request fails and the
daemon is recreated for the next request.

Important limits and timeouts:

- daemon readiness timeout: 120 seconds;
- background thumbnail timeout: 60 seconds;
- export timeout: `EXPORT_RENDER_TIMEOUT_SECS`, default 300 seconds;
- raster export longest-side ceiling: 16,384 pixels;
- preview longest side: `PREVIEW_MAX_PX`, default 512 pixels.

On Linux the child is marked as the preferred OOM victim and receives a parent
death signal. This protects the main backend from a runaway render and prevents
orphaned renderer processes after abrupt server termination.

SVG export uses a separate one-shot `nezumo-render --svg` process. PDF
export first produces SVG and converts it with `svg2pdf` in a blocking worker
thread. Stored board-media URLs are refreshed from their object keys before all
rendering paths so expired presigned URLs do not break images or attachments.

### Thumbnail lifecycle

The periodic preview job and successful board imports upload a PNG to:

```text
<STORAGE_BUCKET_BOARD_FILES>/boards/{board_id}/preview.png
```

The board's `preview_object_key` and `preview_generated_at` fields are updated
only after a successful upload. Preview failures are deliberately non-fatal to
other server work.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `PREVIEW_RENDERER_BIN` | `nezumo-render` | Renderer executable path or command available on `PATH`. |
| `PREVIEW_MAX_PX` | `512` | Longest side of generated thumbnails. |
| `PREVIEW_JOB_INTERVAL_SECS` | `60` | Delay between stale-thumbnail scans; clamped to at least five seconds. |
| `PREVIEW_JOB_BATCH_LIMIT` | `10` | Maximum thumbnails refreshed per scan; clamped to 1–100. |
| `PREVIEW_INTERVAL_SECS` | `3600` | Minimum time between successful renders of a changed board. |
| `EXPORT_RENDER_TIMEOUT_SECS` | `300` | Deadline for raster and SVG export rendering. |
| `ASSET_BASE_URL` | empty | Base URL used by the renderer to fetch fonts and media assets. |
| `RENDER_FONTS_DIR` | empty | Directory containing `fonts.json` and font files for SVG/PDF text rendering. |
| `STORAGE_BUCKET_BOARD_FILES` | `board-files` | Bucket used for generated previews and board media. |
| `YRS_COMPACTION_INTERVAL_SECS` | `300` | Delay between canonical checkpoint scans. |
| `YRS_COMPACTION_BATCH_LIMIT` | `10` | Maximum boards compacted per scan. |
| `YRS_COMPACTION_MIN_UPDATES` | `512` | Journal-update threshold for checkpoint eligibility. |
| `YRS_COMPACTION_MIN_BYTES` | `8388608` | Journal-byte threshold for checkpoint eligibility. |
| `YRS_JOURNAL_GC_ENABLED` | `false` | Enables checkpoint-fenced canonical update and board-event retention. |
| `YRS_JOURNAL_RETENTION_DAYS` | `30` | Minimum age of removable rows; values are clamped to at least seven days. |
| `YRS_JOURNAL_GC_INTERVAL_SECS` | `900` | Delay between retention batches; values are clamped to at least 60 seconds. |
| `YRS_JOURNAL_GC_BATCH_LIMIT` | `1000` | Maximum rows removed from each retained table in one transaction. |

See [`backend/.env.example`](../../.env.example) for renderer and font setup
notes. Storage and Redis configuration are documented in
[`../storage/README.md`](../storage/README.md) and
[`../cache/README.md`](../cache/README.md).

## Operational notes

- The recurring jobs have no distributed leader election. Every backend
  replica starts its own loops. The preview job rechecks eligibility before
  rendering to reduce duplicate work, but a simultaneous multi-replica render
  is still possible; deployments should account for that rendering pressure.
- Canonical retention is fail-closed by default. Enable it only after at least
  two verified immutable checkpoints are being produced per active writer
  lineage; boards without that pair of barriers are skipped automatically.
- Detached preview and garbage-collection tasks are not awaited during process
  shutdown and are not retried by an in-memory queue.
- Raster previews and exports share one serialized daemon. A long export can
  delay thumbnails and other exports; channel saturation makes callers wait
  until capacity is available.
- The renderer needs access to a GPU or a working software Vulkan/GL stack.
  Missing renderer support affects previews and exports but not board
  persistence.
- Temporary snapshot and output files use the operating system's temp
  directory. Ensure it is writable and has sufficient space for large exports.
- Canonical checkpoint correctness depends on applying Yrs updates in sequence.
  Retention changes must preserve the verified immutable barriers required for
  reconnect and restore.
- Monitor warnings and errors for Yrs compaction, journal retention,
  stale-session cleanup, renderer restarts, storage GC, and preview uploads;
  failures are generally isolated and logged rather than crashing the
  background loop.
