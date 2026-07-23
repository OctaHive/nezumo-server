# Storage

This module provides the backend's S3-compatible object-storage layer. It is
used with MinIO in local development and can work with another S3-compatible
service in deployed environments.

The application creates one shared [`StorageState`](./mod.rs) during startup
and stores it in `AppState`. Handlers pass a reference to that state to the
upload, download, delete, and URL-signing helpers.

## Module layout

| File | Responsibility |
| --- | --- |
| [`connect.rs`](./connect.rs) | Builds the S3 client, verifies connectivity, and retries startup failures. |
| [`upload.rs`](./upload.rs) | Uploads raw bytes and returns the stable storage URL. In development it creates a missing bucket automatically. |
| [`download.rs`](./download.rs) | Downloads an object's complete contents as `Vec<u8>`. |
| [`delete.rs`](./delete.rs) | Deletes objects and garbage-collects board files and converted PDF pages. |
| [`presign_url.rs`](./presign_url.rs) | Generates temporary GET URLs and refreshes expired media URLs in board-state JSON. |
| [`mod.rs`](./mod.rs) | Exposes the submodules and defines the shared `StorageState`. |

## Configuration

The main storage client uses these environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `STORAGE_HOST` | yes | — | Internal endpoint including the scheme, for example `http://minio`. |
| `STORAGE_PORT` | no | `9000` | Endpoint port. It is omitted when empty, `80`, or `443`. |
| `STORAGE_REGION` | no | `us-east-1` | S3 signing region. |
| `STORAGE_ACCESS_KEY` | yes | — | S3 access key. |
| `STORAGE_SECRET_KEY` | yes | — | S3 secret key. |
| `STORAGE_PUBLIC_ENDPOINT` | no | empty | Browser-accessible endpoint used when generating presigned URLs. Use this when the internal endpoint is not reachable by clients. |
| `STORAGE_CONNECT_RETRIES` | no | `10` | Number of startup connectivity attempts. |
| `STORAGE_CONNECT_RETRY_DELAY_MS` | no | `1000` | Delay between startup attempts in milliseconds. |
| `STORAGE_BUCKET_BOARD_FILES` | no | `board-files` | Bucket for board media, files, previews, and PDF data. |
| `STORAGE_BUCKET_PROFILE_PICTURES` | no | `profile_pictures` | Bucket for user profile pictures. |
| `STORAGE_PRESIGN_TTL_SECONDS` | no | `3600` | Default lifetime of refreshed board-media URLs. |

Support attachments may use a separate storage account. `handlers/support.rs`
reads `STORAGE_SUPPORT_HOST`, `STORAGE_SUPPORT_PORT`, and
`STORAGE_SUPPORT_REGION`, falling back to the corresponding main-storage
settings. `STORAGE_BUCKET_SUPPORT` selects a dedicated bucket; when it is
empty, support attachments use `STORAGE_BUCKET_BOARD_FILES`. The support
account credentials are configured separately with
`STORAGE_SUPPORT_ACCESS_KEY` and `STORAGE_SUPPORT_SECRET_KEY`.

See [`backend/.env.example`](../../.env.example) and the Docker Compose files in
`backend/` for complete local configuration examples.

## Usage

```rust
use crate::storage::{
    delete::delete_from_storage,
    download::download_from_storage,
    presign_url::generate_presigned_url,
    upload::upload_to_storage,
    StorageState,
};

async fn example(storage: &StorageState, data: &[u8]) -> Result<(), String> {
    let bucket = "board-files";
    let key = "boards/BOARD_ID/images/IMAGE_ID.png";

    let stored_url = upload_to_storage(storage, bucket, key, data).await?;
    let client_url = generate_presigned_url(storage, bucket, key, 3600).await?;
    let downloaded = download_from_storage(storage, bucket, key).await?;

    println!("stored at {stored_url}; signed URL: {client_url}");
    assert_eq!(downloaded, data);

    delete_from_storage(storage, bucket, key).await?;
    Ok(())
}
```

## URL and object-key rules

- Treat the bucket and object key as the stable identity of an object.
- Presigned URLs are temporary credentials. Do not rely on a saved presigned
  URL remaining usable after its TTL.
- `STORAGE_PUBLIC_ENDPOINT` affects generated client URLs only. The shared
  client continues to use the internal `STORAGE_HOST` endpoint for server-side
  uploads, downloads, and deletions.
- Persist `*_object_key` fields for board media. Before returning old board
  state to a client or server-side renderer, call
  `refresh_state_presigned_urls` to regenerate the corresponding `*url` fields.
- Server-side preview and export rendering also presigns the active
  `pdf.page` SVG from its deterministic `board_id`/`docId`/`pageIndex` key, so
  PDF elements do not depend on `ASSET_BASE_URL` to resolve a relative API URL.
- `upload_to_storage` returns an endpoint URL for compatibility with existing
  records. Private objects still need a presigned URL before a browser can read
  them.
- `presign_stored_url` accepts only URLs beginning with the configured internal
  endpoint. External URLs and malformed stored values return `None`.
- Object keys are passed to S3 as-is. Callers are responsible for generating
  safe, collision-resistant keys and keeping them scoped to the correct board
  or user.

## Bucket lifecycle and garbage collection

When `ENVIRONMENT` is not `production`, an upload retries after creating a
missing bucket. In production, all buckets must be provisioned before the
backend starts serving uploads.

Destructive board-storage garbage collection is disabled by default. When
`YRS_ASSET_GC_DELETE_ENABLED=true`, the legacy snapshot endpoint starts a
detached `gc_orphaned_board_storage_from_read_model` pass. The pass ignores the
client-supplied snapshot, acquires the board advisory fence, and proceeds only
when the canonical asset read model exactly matches the current canonical head:

- converted PDF objects are discovered by the
  `boards/{board_id}/pdf/{doc_id}/` prefix;
- other board uploads are tracked by rows in the `board_files` table;
- recently created objects are protected by a grace period to avoid deleting
  in-flight uploads;
- a failed S3 deletion keeps the database row so a later run can retry it.

This is a request-triggered, opt-in pass rather than a recurring background
scanner. Board deletion separately attempts to remove all tracked uploads,
converted PDF objects, and the generated preview.

Do not call the lower-level garbage-collection functions with reference sets
that were built from untrusted or incomplete board state.
