//! Board export: bundle a board into a portable `.nezumo` backup (a zip
//! container with a custom extension).
//!
//! The v2 archive contains a lock-consistent flat compatibility projection,
//! `yrs/state.update`, `yrs/state.vector`, their lineage/barrier metadata, and
//! every referenced binary asset under `assets/<object_key>`. Import verifies
//! the binary projection and every declared asset before creating a board.
//!
//! v1 bundles object-key assets (images / files / video / audio + posters).
//! PDF documents (stored by docId under a separate key prefix) are not bundled
//! yet — a warning is logged when present.

use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use zip::write::SimpleFileOptions;

use crate::core::config::get_env_with_default;
use crate::database::board_members::get_member_role;
use crate::database::boards::get_board_by_id;
use crate::handlers::events::collect_object_keys;
use crate::models::user::User;
use crate::routes::AppState;
use crate::storage::download::download_from_storage;

/// Current export-archive format version.
const EXPORT_VERSION: u32 = 2;

type ApiError = (StatusCode, Json<Value>);

fn err(status: StatusCode, msg: &str) -> ApiError {
    (status, Json(json!({ "error": msg })))
}

/// `GET /boards/{id}/export` — stream a `.octaboard.zip` backup of the board.
pub async fn export_board(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<impl IntoResponse, ApiError> {
    let board_id =
        Uuid::parse_str(&id).map_err(|_| err(StatusCode::BAD_REQUEST, "Invalid board id."))?;

    let board = get_board_by_id(&state.database, board_id)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not fetch board."))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "Board not found."))?;

    // Authorize: admin, owner, or member. Export reveals full content, so
    // (unlike read-only viewing) we don't grant it to anonymous/link viewers.
    let is_owner = current_user.role_level >= 2 || board.owner_id == current_user.id;
    if !is_owner {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.is_none() {
            return Err(err(StatusCode::NOT_FOUND, "Board not found."));
        }
    }

    let canonical = state
        .coordinators
        .current_canonical_export(&state.database, board_id)
        .await
        .map_err(|_| {
            err(
                StatusCode::SERVICE_UNAVAILABLE,
                "Could not materialize canonical board state.",
            )
        })?;

    // Collect referenced assets: media object_keys (image/file/video/audio +
    // posters, and the PDF original via sourceObjectKey) plus every PDF page's
    // rendered SVG (referenced by URL, not an object_key, so collected here).
    let mut object_keys: HashSet<String> = HashSet::new();
    collect_object_keys(&canonical.state, &mut object_keys);
    collect_pdf_page_keys(&canonical.state, board_id, &mut object_keys);
    // Bundle the existing preview thumbnail so import can restore it instantly
    // (no re-render needed).
    if let Some(preview_key) = &board.preview_object_key {
        object_keys.insert(preview_key.clone());
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");

    // Fetch every asset's bytes. A missing asset is skipped with a warning
    // rather than failing the whole export.
    let mut sorted_keys: Vec<String> = object_keys.into_iter().collect();
    sorted_keys.sort();
    let mut assets: Vec<(String, Vec<u8>)> = Vec::new();
    for key in &sorted_keys {
        match download_from_storage(&state.storage, &bucket, key).await {
            Ok(bytes) => assets.push((key.clone(), bytes)),
            Err(e) => {
                tracing::warn!("board {board_id} export: missing asset {key}: {e}");
                return Err(err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Could not collect every referenced board asset.",
                ));
            }
        }
    }

    let manifest = json!({
        "version": EXPORT_VERSION,
        "kind": "octaboard.board",
        "metadata": {
            "title": board.title,
            "grid_type": board.grid_type,
            "background_color": board.background_color,
            "privacy_mode": board.privacy_mode,
            "sticker_authors": board.sticker_authors,
            // Source board id — import rewrites it inside PDF page URLs/keys,
            // which embed the board id (boards/{id}/pdf/{doc}/...).
            "board_id": board.id.to_string(),
            // Existing preview thumbnail key (bundled), so import can restore it.
            "preview_object_key": board.preview_object_key,
        },
        // v2 binds every asset by key, size and digest; import rejects missing,
        // duplicate, undeclared, or changed entries before creating a board.
        "assets": assets.iter().map(|(key, bytes)| json!({
            "object_key": key,
            "size": bytes.len(),
            "sha256": sha256_hex(bytes),
        })).collect::<Vec<_>>(),
        "canonical": {
            "protocol_version": crate::state::yrs_model::PROTOCOL_VERSION,
            "schema_version": canonical.schema_version,
            "min_writer_version": canonical.schema_version,
            "update_encoding": "v1",
            "base_generation": canonical.base_generation,
            "last_event_seq": canonical.last_event_seq,
            "server_client_id": canonical.server_client_id,
            "state_update_sha256": sha256_hex(&canonical.state_update),
            "state_vector_sha256": sha256_hex(&canonical.state_vector),
        },
        "state": canonical.state,
    });

    // Build the zip on a blocking thread (the zip crate is synchronous).
    let state_update = canonical.state_update;
    let state_vector = canonical.state_vector;
    let zip_bytes = tokio::task::spawn_blocking(move || {
        build_zip(&manifest, &assets, &state_update, &state_vector)
    })
    .await
    .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Export task failed."))?
    .map_err(|e| {
        tracing::error!("zip build failed: {e}");
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not build archive.",
        )
    })?;

    let filename = format!("{}.nezumo", sanitize_filename(&board.title));
    Ok((
        [
            (header::CONTENT_TYPE, "application/zip".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        zip_bytes,
    )
        .into_response())
}

/// Query for the raster image export.
#[derive(serde::Deserialize)]
pub struct ExportImageParams {
    /// "png" (default) | "jpeg".
    pub format: Option<String>,
    /// Max longest-side pixels — the UI resolution preset (e.g. 15323, 7661,
    /// 3830). Omitted → full detail (renderer `MAX_SIDE`). Clamped server-side.
    pub max_px: Option<u32>,
}

/// `GET /boards/{id}/export/image?format=png|jpeg&max_px=15323` — render the board
/// to a raster image on the server (full detail up to `MAX_SIDE`, downscaled to
/// `max_px` longest side) and stream it as an attachment. Same authorization as
/// the zip export (owner/admin/member only — reveals full content).
pub async fn export_board_image(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<ExportImageParams>,
    Extension(current_user): Extension<User>,
) -> Result<impl IntoResponse, ApiError> {
    let board_id =
        Uuid::parse_str(&id).map_err(|_| err(StatusCode::BAD_REQUEST, "Invalid board id."))?;

    let board = get_board_by_id(&state.database, board_id)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not fetch board."))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "Board not found."))?;

    let is_owner = current_user.role_level >= 2 || board.owner_id == current_user.id;
    if !is_owner {
        let role = get_member_role(&state.database, board_id, current_user.id)
            .await
            .unwrap_or(None);
        if role.is_none() {
            return Err(err(StatusCode::NOT_FOUND, "Board not found."));
        }
    }

    // `format=svg` is a vector export: not raster, so it takes a different code
    // path (no GPU raster, no max_px) and returns image/svg+xml. Served from the
    // same `export/image` endpoint for consistency with png/jpeg.
    if params
        .format
        .as_deref()
        .map(|f| f.eq_ignore_ascii_case("svg"))
        .unwrap_or(false)
    {
        let svg = crate::jobs::previews::render_board_svg(&state, board_id)
            .await
            .map_err(|e| {
                tracing::error!("board {board_id} svg export failed: {e}");
                err(StatusCode::INTERNAL_SERVER_ERROR, "Could not render SVG.")
            })?;
        if svg.is_empty() {
            return Err(err(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Board has no content to export.",
            ));
        }
        let filename = format!("{}.svg", sanitize_filename(&board.title));
        return Ok((
            [
                (
                    header::CONTENT_TYPE,
                    "image/svg+xml; charset=utf-8".to_string(),
                ),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{filename}\""),
                ),
            ],
            svg,
        )
            .into_response());
    }

    // `format=pdf` is a vector PDF: render the same self-contained SVG (current
    // state, embedded fonts/images), then convert SVG → PDF (pure-Rust svg2pdf).
    if params
        .format
        .as_deref()
        .map(|f| f.eq_ignore_ascii_case("pdf"))
        .unwrap_or(false)
    {
        let pdf = crate::jobs::previews::render_board_pdf(&state, board_id)
            .await
            .map_err(|e| {
                tracing::error!("board {board_id} pdf export failed: {e}");
                err(StatusCode::INTERNAL_SERVER_ERROR, "Could not render PDF.")
            })?;
        if pdf.is_empty() {
            return Err(err(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Board has no content to export.",
            ));
        }
        let filename = format!("{}.pdf", sanitize_filename(&board.title));
        return Ok((
            [
                (header::CONTENT_TYPE, "application/pdf".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{filename}\""),
                ),
            ],
            pdf,
        )
            .into_response());
    }

    let format = match params.format.as_deref() {
        Some(f) if f.eq_ignore_ascii_case("jpeg") || f.eq_ignore_ascii_case("jpg") => "jpeg",
        _ => "png",
    };
    let max_px = params
        .max_px
        .unwrap_or_else(crate::jobs::previews::export_default_max_px);

    let bytes = crate::jobs::previews::render_board_image(&state, board_id, max_px, format)
        .await
        .map_err(|e| {
            tracing::error!("board {board_id} image export failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "Could not render image.")
        })?;
    if bytes.is_empty() {
        return Err(err(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Board has no content to export.",
        ));
    }

    let (content_type, ext) = if format == "jpeg" {
        ("image/jpeg", "jpg")
    } else {
        ("image/png", "png")
    };
    let filename = format!("{}.{ext}", sanitize_filename(&board.title));
    Ok((
        [
            (header::CONTENT_TYPE, content_type.to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        bytes,
    )
        .into_response())
}

/// Pack the v2 manifest, canonical Yrs barrier, and assets into an in-memory zip.
fn build_zip(
    manifest: &Value,
    assets: &[(String, Vec<u8>)],
    state_update: &[u8],
    state_vector: &[u8],
) -> Result<Vec<u8>, String> {
    let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
    {
        let mut zip = zip::ZipWriter::new(&mut cursor);
        let opts =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        zip.start_file("board.json", opts)
            .map_err(|e| e.to_string())?;
        let json = serde_json::to_vec_pretty(manifest).map_err(|e| e.to_string())?;
        zip.write_all(&json).map_err(|e| e.to_string())?;

        zip.start_file("yrs/state.update", opts)
            .map_err(|e| e.to_string())?;
        zip.write_all(state_update).map_err(|e| e.to_string())?;
        zip.start_file("yrs/state.vector", opts)
            .map_err(|e| e.to_string())?;
        zip.write_all(state_vector).map_err(|e| e.to_string())?;

        for (key, bytes) in assets {
            // object_key may contain '/', which zip treats as nested dirs — fine.
            zip.start_file(format!("assets/{key}"), opts)
                .map_err(|e| e.to_string())?;
            zip.write_all(bytes).map_err(|e| e.to_string())?;
        }

        zip.finish().map_err(|e| e.to_string())?;
    }
    Ok(cursor.into_inner())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Walk the state for `pdf.page` components and add every page's rendered-SVG
/// storage key (`boards/{board_id}/pdf/{docId}/page-{n}.svg`) to `set`. These
/// are referenced by URL (svgUrl/urlTemplate), not by an `*object_key` field,
/// so `collect_object_keys` misses them.
fn collect_pdf_page_keys(v: &Value, board_id: Uuid, set: &mut HashSet<String>) {
    match v {
        Value::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("pdf.page") {
                if let Some(doc) = map.get("docId").and_then(|d| d.as_str()) {
                    let count = map.get("pageCount").and_then(|c| c.as_u64()).unwrap_or(0);
                    for n in 0..count {
                        set.insert(format!("boards/{board_id}/pdf/{doc}/page-{n}.svg"));
                    }
                }
            }
            for val in map.values() {
                collect_pdf_page_keys(val, board_id, set);
            }
        }
        Value::Array(arr) => {
            for val in arr {
                collect_pdf_page_keys(val, board_id, set);
            }
        }
        _ => {}
    }
}

/// Make a board title safe for a download filename.
fn sanitize_filename(title: &str) -> String {
    let cleaned: String = title
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('_');
    if trimmed.is_empty() {
        "board".to_string()
    } else {
        trimmed.to_string()
    }
}
