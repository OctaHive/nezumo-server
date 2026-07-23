//! Validated deletion of objects from S3-compatible storage.

use aws_sdk_s3::error::ProvideErrorMetadata;

use crate::storage::StorageState;

/// Deletes an object from S3/MinIO storage
///
/// # Arguments
/// - `s3_client`: Configured S3 client
/// - `bucket`: Target bucket name
/// - `object_key`: Object identifier to delete
///
/// # Returns
/// - `Ok(())` on successful deletion
/// - `Err(String)` with detailed error message on failure
#[allow(dead_code)]
pub async fn delete_from_storage(
    state: &StorageState,
    bucket: &str,
    object_key: &str,
) -> Result<(), String> {
    // Input validation
    if bucket.trim().is_empty() {
        return Err("Delete error: bucket name is empty".to_string());
    }
    if object_key.trim().is_empty() {
        return Err("Delete error: object key is empty".to_string());
    }

    let delete_result = state
        .client
        .delete_object()
        .bucket(bucket)
        .key(object_key)
        .send()
        .await;

    match delete_result {
        Ok(_) => Ok(()),
        Err(err) => {
            let code = err.code().unwrap_or("Unknown");
            let message = err.message().unwrap_or("No error message provided");
            Err(format!(
                "Failed to delete {}/{} (code: {}): {}",
                bucket, object_key, code, message
            ))
        }
    }
}

/// Garbage-collect a board's converted PDF pages from storage. Deletes every
/// `boards/{board_id}/pdf/{doc_id}/...` object whose `doc_id` is NOT in
/// `referenced`, skipping docs whose newest object is younger than `grace_secs`
/// (an in-flight upload whose element may not be in the snapshot yet).
///
/// Called on snapshot save (referenced = doc ids still on the board) and on
/// board deletion (referenced = empty, grace = 0 → delete all).
pub async fn gc_orphaned_pdf_docs(
    storage: &StorageState,
    board_id: uuid::Uuid,
    referenced: &std::collections::HashSet<String>,
    grace_secs: i64,
) {
    use crate::core::config::get_env_with_default;

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let prefix = format!("boards/{}/pdf/", board_id);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    // doc_id -> (newest object epoch secs, all object keys)
    let mut docs: std::collections::HashMap<String, (i64, Vec<String>)> =
        std::collections::HashMap::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = storage
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("pdf gc list error for board {board_id}: {e}");
                return;
            }
        };
        for obj in resp.contents() {
            let Some(key) = obj.key() else { continue };
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            let Some(doc_id) = rest.split('/').next().filter(|s| !s.is_empty()) else {
                continue;
            };
            let modified = obj.last_modified().map(|t| t.secs()).unwrap_or(0);
            let entry = docs.entry(doc_id.to_string()).or_insert((0, Vec::new()));
            entry.0 = entry.0.max(modified);
            entry.1.push(key.to_string());
        }
        if resp.is_truncated().unwrap_or(false) {
            match resp.next_continuation_token() {
                Some(t) => token = Some(t.to_string()),
                None => break,
            }
        } else {
            break;
        }
    }

    for (doc_id, (newest, keys)) in docs {
        if referenced.contains(&doc_id) {
            continue;
        }
        if now - newest < grace_secs {
            continue; // possibly an in-flight upload, keep for now
        }
        let n = keys.len();
        for key in keys {
            if let Err(e) = delete_from_storage(storage, &bucket, &key).await {
                tracing::error!("pdf gc delete {key}: {e}");
            }
        }
        tracing::info!("pdf gc: removed orphan doc {doc_id} ({n} pages) for board {board_id}");
    }
}

/// Garbage-collect a board's uploaded files (images, audio, video, generic
/// files) from storage when their element is no longer on the board. The source
/// of truth is the `board_files` table; `referenced` is the set of object keys
/// still present in the snapshot state. PDF objects (`boards/{id}/pdf/...`) are
/// skipped here — they're handled by [`gc_orphaned_pdf_docs`].
///
/// Skips rows younger than `grace_secs` (an in-flight upload whose element may
/// not be in the snapshot yet). Called on snapshot save; board deletion deletes
/// every file directly (see `delete_board`).
pub async fn gc_orphaned_board_files(
    db: &sqlx::PgPool,
    storage: &StorageState,
    board_id: uuid::Uuid,
    referenced: &std::collections::HashSet<String>,
    grace_secs: i64,
) {
    use crate::core::config::get_env_with_default;
    use crate::database::board_files::{delete_board_file, list_board_files_by_board_id};

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let pdf_prefix = format!("boards/{}/pdf/", board_id);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let files = match list_board_files_by_board_id(db, board_id).await {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("board-files gc list error for board {board_id}: {e}");
            return;
        }
    };

    for file in files {
        // PDF page/source objects are GC'd by gc_orphaned_pdf_docs.
        if file.object_key.starts_with(&pdf_prefix) {
            continue;
        }
        if referenced.contains(&file.object_key) {
            continue;
        }
        if now - file.created_at.timestamp() < grace_secs {
            continue; // possibly an in-flight upload, keep for now
        }
        if let Err(e) = delete_from_storage(storage, &bucket, &file.object_key).await {
            tracing::error!("board-files gc delete {}: {e}", file.object_key);
            continue; // leave the DB row so we retry on the next snapshot
        }
        if let Err(e) = delete_board_file(db, file.id).await {
            tracing::error!("board-files gc db delete {}: {e}", file.id);
        } else {
            tracing::info!(
                "board-files gc: removed orphan {} for board {board_id}",
                file.object_key
            );
        }
    }
}

/// Single entry point for post-snapshot storage GC: removes every orphaned
/// object a board no longer references — converted PDF pages (by doc id) and
/// uploaded media (images/audio/video/files, by object key) — in one call.
///
/// Two passes are kept because the source of truth differs: PDF pages exist
/// only in S3 (listed by the `boards/{id}/pdf/` prefix and grouped by doc id),
/// while other uploads are rows in `board_files`. Canonical callers should use
/// [`crate::state::yrs_assets::project_stable_refs`] to collect both reference
/// sets from the same durable projection.
pub async fn gc_orphaned_board_storage(
    db: &sqlx::PgPool,
    storage: &StorageState,
    board_id: uuid::Uuid,
    referenced_docs: &std::collections::HashSet<String>,
    referenced_keys: &std::collections::HashSet<String>,
    grace_secs: i64,
) {
    gc_orphaned_pdf_docs(storage, board_id, referenced_docs, grace_secs).await;
    gc_orphaned_board_files(db, storage, board_id, referenced_keys, grace_secs).await;
}

/// Destructive GC entry point. It fails closed unless the stable asset
/// read model exactly matches the canonical head. Actual deletion is a separate
/// opt-in and holds the board advisory fence for the whole storage pass so a
/// concurrent delete/re-add commit cannot race the reference set.
pub async fn gc_orphaned_board_storage_from_read_model(
    db: &sqlx::PgPool,
    storage: &StorageState,
    board_id: uuid::Uuid,
    grace_secs: i64,
) {
    if !crate::config::get_env_bool("YRS_ASSET_GC_DELETE_ENABLED", false) {
        tracing::debug!(
            "asset gc disabled for board {board_id}; YRS_ASSET_GC_DELETE_ENABLED is false"
        );
        return;
    }

    let mut tx = match db.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            tracing::warn!("asset gc begin failed for board {board_id}: {error}");
            return;
        }
    };
    if let Err(error) = crate::database::yrs_heads::lock_board_xact(&mut tx, board_id).await {
        tracing::warn!("asset gc fence failed for board {board_id}: {error}");
        return;
    }
    let references = match crate::database::yrs_assets::read_if_fresh_tx(&mut tx, board_id).await {
        Ok(Some(references)) => references,
        Ok(None) => {
            tracing::warn!(
                "asset gc skipped for board {board_id}: read model is absent, blocked, or stale"
            );
            return;
        }
        Err(error) => {
            tracing::warn!("asset gc read-model check failed for board {board_id}: {error}");
            return;
        }
    };

    let (referenced_keys, referenced_docs) = references;
    gc_orphaned_board_storage(
        db,
        storage,
        board_id,
        &referenced_docs,
        &referenced_keys,
        grace_secs,
    )
    .await;
    if let Err(error) = tx.commit().await {
        tracing::warn!("asset gc fence release failed for board {board_id}: {error}");
    }
}
