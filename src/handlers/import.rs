//! Board import: recreate a board from a `.nezumo` backup produced by
//! `export.rs` (a zip container with a custom extension).
//!
//! Multipart `POST /boards/import` with fields `project_id` (text) and `file`
//! (the backup). Unpacks `board.json` + `assets/`, creates a new board in the
//! target project, re-uploads each asset under the new board's key prefix,
//! rewrites the state's `*object_key` values to the new keys (and regenerates
//! the sibling `*url` fields via `refresh_state_presigned_urls`), then stores
//! the state as the board's first snapshot.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;

use axum::extract::{Extension, Multipart, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::core::config::get_env_with_default;
use crate::database::board_members::add_board_member;
use crate::database::boards::{
    create_board as db_create_board, delete_board_by_id, BoardCreateError,
};
use crate::database::project_members::get_project_member_role;
use crate::database::projects::get_project_by_id;
use crate::database::quotas::{
    ensure_file_size_available, ensure_storage_available, evaluate_file_size,
    record_imported_storage,
};
use crate::database::snapshots::insert_snapshot;
use crate::handlers::events::collect_object_keys;
use crate::handlers::quotas::quota_api_error;
use crate::models::boards::BoardCreateBody;
use crate::models::user::User;
use crate::routes::AppState;
use crate::storage::delete::delete_from_storage;
use crate::storage::presign_url::refresh_state_presigned_urls;
use crate::storage::upload::upload_to_storage;

const MAX_ARCHIVE_ENTRIES: usize = 10_000;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_YRS_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_ASSET_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_UNCOMPRESSED_BYTES: u64 = 4 * 1024 * 1024 * 1024;

type ApiError = (StatusCode, Json<Value>);

fn err(status: StatusCode, msg: &str) -> ApiError {
    (status, Json(json!({ "error": msg })))
}

struct ImportedArchive {
    manifest: Value,
    assets: Vec<(String, Vec<u8>)>,
    yrs_state_update: Option<Vec<u8>>,
    yrs_state_vector: Option<Vec<u8>>,
}

/// `POST /boards/import` — create a board from an uploaded `.octaboard.zip`.
pub async fn import_board(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, ApiError> {
    let mut project_id: Option<Uuid> = None;
    let mut zip_bytes: Option<Vec<u8>> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| err(StatusCode::BAD_REQUEST, "Malformed upload."))?
    {
        match field.name() {
            Some("project_id") => {
                let text = field
                    .text()
                    .await
                    .map_err(|_| err(StatusCode::BAD_REQUEST, "Invalid project_id."))?;
                project_id = Uuid::parse_str(text.trim()).ok();
            }
            Some("file") => {
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|_| err(StatusCode::BAD_REQUEST, "Could not read file."))?;
                zip_bytes = Some(bytes.to_vec());
            }
            _ => {}
        }
    }

    let project_id =
        project_id.ok_or_else(|| err(StatusCode::BAD_REQUEST, "Missing project_id."))?;
    let zip_bytes = zip_bytes.ok_or_else(|| err(StatusCode::BAD_REQUEST, "Missing file."))?;

    let tier = ensure_file_size_available(
        &state.database,
        current_user.id,
        i64::try_from(zip_bytes.len()).unwrap_or(i64::MAX),
    )
    .await
    .map_err(quota_api_error)?;

    // Authorize: admin, or a member of the target project (mirrors create_board).
    let project = get_project_by_id(&state.database, project_id)
        .await
        .map_err(|_| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not fetch project.",
            )
        })?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "Project not found."))?;
    if current_user.role_level < 2 {
        let role = get_project_member_role(&state.database, project.id, current_user.id)
            .await
            .unwrap_or(None);
        if role.is_none() {
            return Err(err(StatusCode::FORBIDDEN, "Access denied."));
        }
    }

    // Unpack the archive (zip crate is synchronous).
    let archive = tokio::task::spawn_blocking(move || read_zip(zip_bytes))
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Import task failed."))?
        .map_err(|e| {
            tracing::warn!("import: bad archive: {e}");
            err(StatusCode::BAD_REQUEST, "Invalid backup archive.")
        })?;

    // This is intentionally before `db_create_board`: a corrupt binary state,
    // invalid entity/reference graph, or changed/missing asset must never leave
    // an empty imported board behind.
    let mut state_value = validate_archive(&archive).map_err(|error| {
        tracing::warn!("import: archive validation failed: {error}");
        err(StatusCode::BAD_REQUEST, "Invalid backup archive.")
    })?;

    let imported_storage_bytes = archive
        .assets
        .iter()
        .try_fold(0_i64, |total, (_, bytes)| {
            total.checked_add(i64::try_from(bytes.len()).ok()?)
        })
        .ok_or_else(|| {
            err(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Imported assets are too large.",
            )
        })?;
    let largest_asset_bytes = archive
        .assets
        .iter()
        .map(|(_, bytes)| i64::try_from(bytes.len()).unwrap_or(i64::MAX))
        .max()
        .unwrap_or(0);
    evaluate_file_size(&tier, largest_asset_bytes).map_err(quota_api_error)?;
    ensure_storage_available(&state.database, current_user.id, imported_storage_bytes)
        .await
        .map_err(quota_api_error)?;

    let ImportedArchive {
        manifest,
        assets,
        yrs_state_update: _,
        yrs_state_vector: _,
    } = archive;

    let metadata = manifest.get("metadata").cloned().unwrap_or(json!({}));
    // Source board id — PDF page keys/URLs embed it (boards/{id}/pdf/{doc}/...).
    let old_board_id = metadata
        .get("board_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    // Existing preview thumbnail key (bundled), restored instead of re-rendering.
    let old_preview_key = metadata
        .get("preview_object_key")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let meta_str = |k: &str, default: &str| {
        metadata
            .get(k)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(default)
            .to_string()
    };

    let body = BoardCreateBody {
        project_id,
        title: meta_str("title", "Imported board"),
        visibility: "private".to_string(),
        grid_type: Some(meta_str("grid_type", "lines")),
        background_color: Some(meta_str("background_color", "#f5f5f5")),
        link_access: Some("none".to_string()),
    };

    let board = db_create_board(&state.database, current_user.id, &body)
        .await
        .map_err(|error| match error {
            BoardCreateError::Quota(error) => quota_api_error(error),
            BoardCreateError::Database(error) => {
                tracing::error!("import board create database error: {error}");
                err(StatusCode::INTERNAL_SERVER_ERROR, "Could not create board.")
            }
        })?;

    // Grant the importer ownership (the board_members row access checks rely on),
    // mirroring create_board. owner_id alone isn't enough for membership-based
    // authorization.
    let _ = add_board_member(&state.database, board.id, current_user.id, "owner").await;

    // Re-upload assets under the new board's prefix, mapping old key → new key.
    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let mut key_map: HashMap<String, String> = HashMap::new();
    let pdf_prefix = old_board_id
        .as_deref()
        .map(|old| format!("boards/{old}/pdf/"));
    let mut new_preview_key: Option<String> = None;
    for (old_key, data) in assets {
        if data.is_empty() {
            continue;
        }
        // Preserve structured keys (preview, PDF pages/original) that are
        // referenced by a fixed path under boards/{id}/... — only swap the board
        // segment. Other media assets get a fresh random key.
        let new_key = if old_preview_key.as_deref() == Some(old_key.as_str()) {
            format!("boards/{}/preview.png", board.id)
        } else if let Some(prefix) = pdf_prefix.as_deref().filter(|p| old_key.starts_with(*p)) {
            format!("boards/{}/pdf/{}", board.id, &old_key[prefix.len()..])
        } else {
            let ext = old_key
                .rsplit('.')
                .next()
                .filter(|e| !e.is_empty() && e.len() <= 8 && !e.contains('/'))
                .unwrap_or("bin");
            format!("boards/{}/{}.{}", board.id, Uuid::new_v4(), ext)
        };
        if let Err(e) = upload_to_storage(&state.storage, &bucket, &new_key, &data).await {
            tracing::error!("import: upload {new_key} failed: {e}");
            for uploaded_key in key_map.values() {
                let _ = delete_from_storage(&state.storage, &bucket, uploaded_key).await;
            }
            let _ = delete_board_by_id(&state.database, board.id).await;
            return Err(err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not store imported asset.",
            ));
        }
        if old_preview_key.as_deref() == Some(old_key.as_str()) {
            new_preview_key = Some(new_key.clone());
        }
        key_map.insert(old_key, new_key);
    }

    if let Err(error) = record_imported_storage(
        &state.database,
        current_user.id,
        board.id,
        imported_storage_bytes,
    )
    .await
    {
        for uploaded_key in key_map.values() {
            let _ = delete_from_storage(&state.storage, &bucket, uploaded_key).await;
        }
        let _ = delete_board_by_id(&state.database, board.id).await;
        return Err(quota_api_error(error));
    }

    // Point the state at the new keys, then regenerate the presigned URLs.
    rewrite_object_keys(&mut state_value, &key_map);
    refresh_state_presigned_urls(&state.storage, &mut state_value).await;

    // PDF page URLs (svgUrl/urlTemplate) embed the old board id; point them at
    // the new board so they resolve to the re-uploaded pages.
    if let Some(old) = old_board_id.as_deref() {
        rewrite_pdf_urls(&mut state_value, old, &board.id.to_string());
    }

    // If the backup carried a preview we restored it above; otherwise render one
    // now (detached) so the board still shows a thumbnail. The periodic snapshot
    // job keys off new events and would never fire for a directly-inserted
    // snapshot, so we can't rely on it.
    let preview_state = if new_preview_key.is_none() {
        Some(state_value.clone())
    } else {
        None
    };

    insert_snapshot(&state.database, board.id, 0, state_value.clone())
        .await
        .map_err(|_| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not store board state.",
            )
        })?;

    // Seed a fresh, board-local Yrs lineage from the rewritten projection. The
    // source binary was verified above, but cannot be reused byte-for-byte:
    // imported asset keys and PDF board-id URLs intentionally change. Entity
    // ids and references remain stable; canonicalization already checked their
    // numeric range and shape before the board was created.
    let candidate =
        crate::state::canonical_base::from_flat_projection(&state_value, 0).map_err(|_| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not build imported canonical state.",
            )
        })?;
    let seeded = crate::database::yrs_canonical_bases::upsert_base_cas(
        &state.database,
        board.id,
        &candidate,
        None,
    )
    .await
    .map_err(|_| {
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not store imported canonical state.",
        )
    })?;
    if !seeded {
        return Err(err(
            StatusCode::CONFLICT,
            "Imported canonical state changed concurrently.",
        ));
    }

    if let Some(preview_key) = new_preview_key {
        let _ = sqlx::query(
            "UPDATE boards SET preview_object_key = $1, preview_generated_at = NOW() WHERE id = $2",
        )
        .bind(&preview_key)
        .bind(board.id)
        .execute(&state.database)
        .await;
    } else if let Some(preview_state) = preview_state {
        let app = state.clone();
        let board_id = board.id;
        tokio::spawn(async move {
            crate::jobs::previews::generate_and_store(app, board_id, preview_state).await;
        });
    }

    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": board.id, "project_id": board.project_id })),
    ))
}

/// Read the portable archive without extracting paths to disk. Entry names,
/// duplicates and uncompressed sizes are bounded before allocating buffers.
fn read_zip(bytes: Vec<u8>) -> Result<ImportedArchive, String> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|e| e.to_string())?;
    if archive.len() > MAX_ARCHIVE_ENTRIES {
        return Err("too many archive entries".to_string());
    }

    let mut manifest: Option<Value> = None;
    let mut assets: Vec<(String, Vec<u8>)> = Vec::new();
    let mut yrs_state_update: Option<Vec<u8>> = None;
    let mut yrs_state_vector: Option<Vec<u8>> = None;
    let mut names = HashSet::new();
    let mut uncompressed_total = 0_u64;

    for i in 0..archive.len() {
        let file = archive.by_index(i).map_err(|e| e.to_string())?;
        let name = file.name().to_string();
        if file.is_dir() {
            continue;
        }
        if !names.insert(name.clone()) {
            return Err(format!("duplicate archive entry: {name}"));
        }
        uncompressed_total = uncompressed_total
            .checked_add(file.size())
            .ok_or_else(|| "archive size overflow".to_string())?;
        if uncompressed_total > MAX_ARCHIVE_UNCOMPRESSED_BYTES {
            return Err("archive is too large".to_string());
        }
        let limit = if name == "board.json" {
            MAX_MANIFEST_BYTES
        } else if name == "yrs/state.update" || name == "yrs/state.vector" {
            MAX_YRS_FILE_BYTES
        } else if name.starts_with("assets/") {
            MAX_ASSET_BYTES
        } else {
            return Err(format!("unknown archive entry: {name}"));
        };
        if file.size() > limit {
            return Err(format!("archive entry is too large: {name}"));
        }
        let mut buf = Vec::new();
        file.take(limit + 1)
            .read_to_end(&mut buf)
            .map_err(|e| e.to_string())?;
        if buf.len() as u64 > limit {
            return Err(format!("archive entry expanded past its limit: {name}"));
        }

        if name == "board.json" {
            if manifest.is_some() {
                return Err("duplicate board.json".to_string());
            }
            manifest = Some(serde_json::from_slice(&buf).map_err(|e| e.to_string())?);
        } else if name == "yrs/state.update" {
            yrs_state_update = Some(buf);
        } else if name == "yrs/state.vector" {
            yrs_state_vector = Some(buf);
        } else if let Some(key) = name.strip_prefix("assets/") {
            validate_object_key(key)?;
            assets.push((key.to_string(), buf));
        }
    }

    let manifest = manifest.ok_or_else(|| "missing board.json".to_string())?;
    Ok(ImportedArchive {
        manifest,
        assets,
        yrs_state_update,
        yrs_state_vector,
    })
}

/// Validate v1 compatibility state or the complete v2 cryptographic/binary
/// contract. Returns the deterministic flat state that import may rewrite.
fn validate_archive(archive: &ImportedArchive) -> Result<Value, String> {
    let version = archive
        .manifest
        .get("version")
        .and_then(Value::as_u64)
        .ok_or_else(|| "missing archive version".to_string())?;
    if version != 1 && version != 2 {
        return Err(format!("unsupported archive version: {version}"));
    }
    if let Some(kind) = archive.manifest.get("kind").and_then(Value::as_str) {
        if kind != "octaboard.board" {
            return Err("unexpected archive kind".to_string());
        }
    }
    let flat = archive
        .manifest
        .get("state")
        .ok_or_else(|| "missing board state".to_string())?;
    let canonical = crate::state::yrs_model::canonicalize_flat(flat)
        .map_err(|error| format!("invalid board state: {error}"))?;
    let canonical_value = canonical.as_value().clone();
    validate_import_allocator_capacity(&canonical_value)?;

    if version == 1 {
        if archive.yrs_state_update.is_some() || archive.yrs_state_vector.is_some() {
            return Err("v1 archive contains unexpected Yrs state".to_string());
        }
        return Ok(canonical_value);
    }

    let metadata = archive
        .manifest
        .get("canonical")
        .and_then(Value::as_object)
        .ok_or_else(|| "missing canonical metadata".to_string())?;
    let number = |key: &str| {
        metadata
            .get(key)
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("invalid canonical {key}"))
    };
    let protocol = number("protocol_version")?;
    let schema = number("schema_version")?;
    let minimum = number("min_writer_version")?;
    let generation = number("base_generation")?;
    let _last_event_seq = metadata
        .get("last_event_seq")
        .and_then(Value::as_i64)
        .filter(|seq| *seq >= 0)
        .ok_or_else(|| "invalid canonical last_event_seq".to_string())?;
    let client_id = number("server_client_id")?;
    if protocol != crate::state::yrs_model::PROTOCOL_VERSION
        || schema != crate::state::yrs_model::SCHEMA_VERSION
        || minimum != schema
        || generation == 0
        || metadata.get("update_encoding").and_then(Value::as_str) != Some("v1")
        || !crate::state::yrs_model::server_id_in_band(client_id)
    {
        return Err("unsupported canonical protocol or lineage".to_string());
    }
    let state_update = archive
        .yrs_state_update
        .as_deref()
        .ok_or_else(|| "missing yrs/state.update".to_string())?;
    let state_vector = archive
        .yrs_state_vector
        .as_deref()
        .ok_or_else(|| "missing yrs/state.vector".to_string())?;
    verify_digest(metadata, "state_update_sha256", state_update)?;
    verify_digest(metadata, "state_vector_sha256", state_vector)?;

    let doc = crate::state::yrs_model::doc_from_base(state_update, state_vector, client_id)
        .map_err(|error| format!("invalid canonical Yrs base: {error}"))?;
    crate::state::yrs_model::validate_schema_metadata(&doc, schema)
        .map_err(|error| format!("invalid canonical schema: {error}"))?;
    let projected = crate::state::yrs_model::doc_to_snapshot(&doc)
        .map_err(|error| format!("invalid canonical projection: {error}"))?;
    if projected != canonical_value {
        return Err("binary canonical state does not match board state".to_string());
    }

    validate_v2_assets(&archive.manifest, &archive.assets)?;
    Ok(canonical_value)
}

/// Renderer sessions allocate new synced ids in 2^16-wide blocks above the
/// highest loaded wire-id index. Refuse an archive that cannot leave the first
/// post-import block without wrapping the low 32-bit EntityId index.
fn validate_import_allocator_capacity(state: &Value) -> Result<(), String> {
    const SYNCED_ID_STRIDE: u64 = 1 << 16;
    let max_index = state
        .get("entities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entity| entity.get("id").and_then(Value::as_u64))
        .map(|id| id & u32::MAX as u64)
        .max()
        .unwrap_or(0);
    if max_index > u32::MAX as u64 - SYNCED_ID_STRIDE {
        return Err("imported entity ids exhaust the synced-id allocator".to_string());
    }
    Ok(())
}

fn validate_v2_assets(manifest: &Value, assets: &[(String, Vec<u8>)]) -> Result<(), String> {
    let declared = manifest
        .get("assets")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing v2 asset inventory".to_string())?;
    let mut inventory: HashMap<&str, (u64, &str)> = HashMap::new();
    for item in declared {
        let key = item
            .get("object_key")
            .and_then(Value::as_str)
            .ok_or_else(|| "invalid asset object_key".to_string())?;
        validate_object_key(key)?;
        let size = item
            .get("size")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("invalid asset size: {key}"))?;
        let digest = item
            .get("sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("invalid asset digest: {key}"))?;
        if inventory.insert(key, (size, digest)).is_some() {
            return Err(format!("duplicate declared asset: {key}"));
        }
    }

    let mut referenced = HashSet::new();
    collect_object_keys(
        manifest
            .get("state")
            .ok_or_else(|| "missing board state".to_string())?,
        &mut referenced,
    );
    let metadata = manifest
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or_else(|| "missing board metadata".to_string())?;
    let source_board_id = metadata
        .get("board_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing source board id".to_string())?;
    Uuid::parse_str(source_board_id).map_err(|_| "invalid source board id".to_string())?;
    collect_pdf_page_keys(
        manifest
            .get("state")
            .ok_or_else(|| "missing board state".to_string())?,
        source_board_id,
        &mut referenced,
    );
    if let Some(preview) = metadata.get("preview_object_key").and_then(Value::as_str) {
        validate_object_key(preview)?;
        referenced.insert(preview.to_string());
    }
    let declared_keys: HashSet<String> = inventory.keys().map(|key| (*key).to_string()).collect();
    if referenced != declared_keys {
        return Err("asset inventory does not cover the board references".to_string());
    }
    if inventory.len() != assets.len() {
        return Err("asset inventory does not match archive".to_string());
    }
    for (key, bytes) in assets {
        let (size, expected) = inventory
            .remove(key.as_str())
            .ok_or_else(|| format!("undeclared asset: {key}"))?;
        if size != bytes.len() as u64 || !digest_matches(expected, bytes) {
            return Err(format!("asset integrity mismatch: {key}"));
        }
    }
    if !inventory.is_empty() {
        return Err("archive is missing declared assets".to_string());
    }
    Ok(())
}

fn collect_pdf_page_keys(v: &Value, board_id: &str, keys: &mut HashSet<String>) {
    match v {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("pdf.page") {
                if let Some(doc) = map.get("docId").and_then(Value::as_str) {
                    let count = map.get("pageCount").and_then(Value::as_u64).unwrap_or(0);
                    for page in 0..count {
                        keys.insert(format!("boards/{board_id}/pdf/{doc}/page-{page}.svg"));
                    }
                }
            }
            for child in map.values() {
                collect_pdf_page_keys(child, board_id, keys);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_pdf_page_keys(child, board_id, keys);
            }
        }
        _ => {}
    }
}

fn verify_digest(
    metadata: &serde_json::Map<String, Value>,
    key: &str,
    bytes: &[u8],
) -> Result<(), String> {
    let expected = metadata
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing canonical {key}"))?;
    if !digest_matches(expected, bytes) {
        return Err(format!("canonical digest mismatch: {key}"));
    }
    Ok(())
}

fn digest_matches(expected: &str, bytes: &[u8]) -> bool {
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return false;
    }
    let actual: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    actual.eq_ignore_ascii_case(expected)
}

fn validate_object_key(key: &str) -> Result<(), String> {
    if key.is_empty()
        || key.starts_with('/')
        || key.contains('\\')
        || key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err("unsafe asset object key".to_string());
    }
    Ok(())
}

/// Replace every `*object_key` string value in the state using `map`
/// (old key → new key). Mirrors the walk in `collect_object_keys`. Sibling
/// `*url` fields are regenerated separately by `refresh_state_presigned_urls`.
fn rewrite_object_keys(v: &mut Value, map: &HashMap<String, String>) {
    match v {
        Value::Object(obj) => {
            for (k, val) in obj.iter_mut() {
                let normalized: String = k
                    .chars()
                    .filter(|c| *c != '_')
                    .flat_map(|c| c.to_lowercase())
                    .collect();
                if normalized.contains("objectkey") {
                    if let Value::String(s) = val {
                        if let Some(new_key) = map.get(s.as_str()) {
                            *s = new_key.clone();
                        }
                    }
                }
                rewrite_object_keys(val, map);
            }
        }
        Value::Array(arr) => {
            for val in arr {
                rewrite_object_keys(val, map);
            }
        }
        _ => {}
    }
}

/// Replace the old board id with the new one inside PDF page URL fields
/// (`svgUrl`, `urlTemplate`), which embed it as `boards/{id}/pdf/{doc}/...`.
fn rewrite_pdf_urls(v: &mut Value, old_board_id: &str, new_board_id: &str) {
    match v {
        Value::Object(obj) => {
            for (k, val) in obj.iter_mut() {
                if k == "svgUrl" || k == "urlTemplate" {
                    if let Value::String(s) = val {
                        *s = s.replace(old_board_id, new_board_id);
                    }
                }
                rewrite_pdf_urls(val, old_board_id, new_board_id);
            }
        }
        Value::Array(arr) => {
            for val in arr {
                rewrite_pdf_urls(val, old_board_id, new_board_id);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    #[test]
    fn rewrite_object_keys_remaps_all_key_fields() {
        let mut state = json!({
            "entities": [{
                "id": 1,
                "components": [
                    { "type": "image", "object_key": "old/a.png", "url": "stale" },
                    { "type": "video", "poster_object_key": "old/p.jpg", "label": "keep me" }
                ]
            }]
        });
        let map = HashMap::from([
            ("old/a.png".to_string(), "new/a.png".to_string()),
            ("old/p.jpg".to_string(), "new/p.jpg".to_string()),
        ]);
        rewrite_object_keys(&mut state, &map);

        let comps = &state["entities"][0]["components"];
        assert_eq!(comps[0]["object_key"], "new/a.png");
        assert_eq!(comps[1]["poster_object_key"], "new/p.jpg");
        // Non-key strings are untouched.
        assert_eq!(comps[1]["label"], "keep me");
    }

    #[test]
    fn rewrite_pdf_urls_swaps_board_id() {
        let old = "11111111-1111-1111-1111-111111111111";
        let new = "22222222-2222-2222-2222-222222222222";
        let mut state = json!({
            "entities": [{
                "components": [{
                    "type": "pdf.page",
                    "docId": "doc1",
                    "svgUrl": format!("/boards/{old}/pdf/doc1/page/0"),
                    "urlTemplate": format!("/boards/{old}/pdf/doc1/page/{{n}}"),
                    "label": format!("untouched {old}")
                }]
            }]
        });
        rewrite_pdf_urls(&mut state, old, new);
        let c = &state["entities"][0]["components"][0];
        assert_eq!(c["svgUrl"], format!("/boards/{new}/pdf/doc1/page/0"));
        assert_eq!(
            c["urlTemplate"],
            format!("/boards/{new}/pdf/doc1/page/{{n}}")
        );
        // Only the URL fields are rewritten, not arbitrary strings.
        assert_eq!(c["label"], format!("untouched {old}"));
    }

    #[test]
    fn read_zip_parses_manifest_and_assets() {
        // Build an archive the way export.rs does.
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        {
            let mut zip = zip::ZipWriter::new(&mut cursor);
            let opts = SimpleFileOptions::default();
            zip.start_file("board.json", opts).unwrap();
            zip.write_all(
                serde_json::to_vec(&json!({
                    "version": 1,
                    "state": { "entities": [] }
                }))
                .unwrap()
                .as_slice(),
            )
            .unwrap();
            zip.start_file("assets/boards/x/a.png", opts).unwrap();
            zip.write_all(b"PNGDATA").unwrap();
            zip.finish().unwrap();
        }

        let archive = read_zip(cursor.into_inner()).unwrap();
        assert_eq!(archive.manifest["version"], 1);
        assert!(archive.manifest["state"]["entities"].is_array());
        assert_eq!(archive.assets.len(), 1);
        assert_eq!(archive.assets[0].0, "boards/x/a.png");
        assert_eq!(archive.assets[0].1, b"PNGDATA");
        assert!(validate_archive(&archive).is_ok());
    }

    #[test]
    fn v2_validates_binary_projection_and_rejects_tampering() {
        let state = json!({
            "entities": [{
                "id": (1_u64 << 32) | 4_000_000,
                "components": [{ "type": "shape", "kind": "rect" }]
            }]
        });
        let canonical = crate::state::yrs_model::canonicalize_flat(&state).unwrap();
        let client_id = crate::state::yrs_model::SERVER_ID_LO;
        let doc =
            crate::state::yrs_model::snapshot_to_doc_with_client_id(&canonical, client_id).unwrap();
        let (update, vector) = crate::state::yrs_model::encode_base(&doc);
        let manifest = json!({
            "version": 2,
            "kind": "octaboard.board",
            "metadata": {
                "board_id": "11111111-1111-1111-1111-111111111111"
            },
            "assets": [],
            "canonical": {
                "protocol_version": crate::state::yrs_model::PROTOCOL_VERSION,
                "schema_version": crate::state::yrs_model::SCHEMA_VERSION,
                "min_writer_version": crate::state::yrs_model::SCHEMA_VERSION,
                "update_encoding": "v1",
                "base_generation": 3,
                "last_event_seq": 42,
                "server_client_id": client_id,
                "state_update_sha256": sha256_string(&update),
                "state_vector_sha256": sha256_string(&vector),
            },
            "state": canonical.as_value(),
        });
        let mut archive = ImportedArchive {
            manifest,
            assets: Vec::new(),
            yrs_state_update: Some(update),
            yrs_state_vector: Some(vector),
        };

        let restored = validate_archive(&archive).unwrap();
        assert_eq!(restored, canonical.as_value().clone());
        assert_eq!(restored["entities"][0]["id"], (1_u64 << 32) | 4_000_000);

        archive.yrs_state_update.as_mut().unwrap()[0] ^= 0x01;
        assert!(validate_archive(&archive).is_err());
    }

    #[test]
    fn read_zip_rejects_unsafe_or_duplicate_entries() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        {
            let mut zip = zip::ZipWriter::new(&mut cursor);
            let opts = SimpleFileOptions::default();
            zip.start_file("board.json", opts).unwrap();
            zip.write_all(b"{\"version\":1,\"state\":{\"entities\":[]}}")
                .unwrap();
            zip.start_file("assets/../escape", opts).unwrap();
            zip.write_all(b"bad").unwrap();
            zip.finish().unwrap();
        }
        assert!(read_zip(cursor.into_inner()).is_err());
    }

    #[test]
    fn import_reserves_space_above_loaded_synced_ids() {
        let safe = json!({"entities": [{"id": (1_u64 << 32) | 123, "components": []}]});
        assert!(validate_import_allocator_capacity(&safe).is_ok());
        let exhausted = json!({
            "entities": [{"id": (1_u64 << 32) | (u32::MAX as u64), "components": []}]
        });
        assert!(validate_import_allocator_capacity(&exhausted).is_err());
    }

    fn sha256_string(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}
