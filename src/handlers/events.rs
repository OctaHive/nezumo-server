//! Collaborative board event and snapshot handlers.

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::{
    extract::{Extension, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    Json,
};
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use crate::database::board_members::get_member_role;
use crate::database::boards::get_board_by_id;
use crate::database::events::{list_events_by_user_session, list_events_since, min_event_seq};
use crate::database::snapshots::{fetch_latest_snapshot, insert_snapshot};
use crate::models::events::{CommitEventBody, CommitEventResponse, SnapshotCreateBody};
use crate::models::user::User;
use crate::routes::AppState;

#[derive(Deserialize, Debug)]
pub struct EventsQuery {
    pub since: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct YrsUpdatesQuery {
    pub since_seq: i64,
    pub base_generation: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Clone)]
struct CanonicalReadBase {
    state_update: Vec<u8>,
    state_vector: Vec<u8>,
    base_seq: i64,
    base_generation: i64,
    server_client_id: i64,
    protocol_version: i32,
    schema_version: i32,
    min_writer_version: i32,
    update_encoding: String,
    source: &'static str,
}

type ApiError = (StatusCode, Json<serde_json::Value>);

fn yrs_error(status: StatusCode, code: &'static str, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(json!({ "error": code, "message": message.into() })),
    )
}

async fn activate_yrs_read_path(
    state: &AppState,
    board_id: Uuid,
) -> Result<(CanonicalReadBase, crate::database::yrs_heads::YrsHead), ApiError> {
    let _resident = state
        .coordinators
        .ensure_active(&state.database, board_id)
        .await
        .map_err(|e| {
            tracing::warn!("yrs read-path activation failed for board {board_id}: {e}");
            yrs_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "YRS_BOOTSTRAP_UNAVAILABLE",
                e,
            )
        })?;

    let base = crate::database::yrs_canonical_bases::read_base(&state.database, board_id)
        .await
        .map_err(|_| {
            yrs_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "YRS_READ_FAILED",
                "Could not read Yrs base.",
            )
        })?
        .ok_or_else(|| {
            yrs_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "YRS_BOOTSTRAP_UNAVAILABLE",
                "No Yrs base is available.",
            )
        })?;
    let head = crate::database::yrs_heads::read_head(&state.database, board_id)
        .await
        .map_err(|_| {
            yrs_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "YRS_READ_FAILED",
                "Could not read Yrs head.",
            )
        })?
        .ok_or_else(|| {
            yrs_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "YRS_BOOTSTRAP_UNAVAILABLE",
                "No Yrs head is available.",
            )
        })?;
    if base.abandoned_at.is_some()
        || head.state != crate::database::yrs_heads::CanonicalState::Ready
        || head.base_generation != base.base_generation
    {
        return Err(yrs_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "YRS_BOOTSTRAP_UNAVAILABLE",
            "The canonical Yrs generation is not ready.",
        ));
    }
    let mut selected = CanonicalReadBase {
        state_update: base.state_update,
        state_vector: base.state_vector,
        base_seq: base.base_seq,
        base_generation: base.base_generation,
        server_client_id: base.server_client_id,
        protocol_version: base.protocol_version,
        schema_version: base.schema_version,
        min_writer_version: base.min_writer_version,
        update_encoding: base.update_encoding,
        source: "canonical_base",
    };
    if let Some(snapshot) = crate::database::yrs_snapshots::read_latest_at_or_before(
        &state.database,
        board_id,
        head.base_generation,
        selected.server_client_id,
        head.processed_seq,
    )
    .await
    .map_err(|_| {
        yrs_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "YRS_READ_FAILED",
            "Could not read binary Yrs snapshot.",
        )
    })? {
        if !snapshot.content_hash_matches()
            || snapshot.output_bytes != snapshot.state_update.len() as i64
            || snapshot.protocol_version != crate::state::yrs_model::PROTOCOL_VERSION as i16
        {
            return Err(yrs_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "YRS_BINARY_SNAPSHOT_INVALID",
                "The latest binary Yrs snapshot failed integrity checks.",
            ));
        }
        let doc = crate::state::yrs_model::doc_from_base(
            &snapshot.state_update,
            &snapshot.state_vector,
            snapshot.server_client_id as u64,
        )
        .map_err(|_| {
            yrs_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "YRS_BINARY_SNAPSHOT_INVALID",
                "The latest binary Yrs snapshot could not be reloaded.",
            )
        })?;
        crate::state::yrs_model::validate_schema_metadata(&doc, snapshot.schema_version as u64)
            .map_err(|_| {
                yrs_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "YRS_BINARY_SNAPSHOT_INVALID",
                    "The latest binary Yrs snapshot has invalid schema metadata.",
                )
            })?;
        selected = CanonicalReadBase {
            state_update: snapshot.state_update,
            state_vector: snapshot.state_vector,
            base_seq: snapshot.last_event_seq,
            base_generation: snapshot.base_generation,
            server_client_id: snapshot.server_client_id,
            protocol_version: snapshot.protocol_version as i32,
            schema_version: snapshot.schema_version,
            min_writer_version: snapshot.schema_version,
            update_encoding: "v1".to_string(),
            source: "binary_snapshot",
        };
    }
    Ok((selected, head))
}

fn insert_i64(headers: &mut HeaderMap, name: &'static str, value: i64) {
    headers.insert(name, HeaderValue::from_str(&value.to_string()).unwrap());
}

/// Returns an immutable canonical checkpoint. The body is the raw Yrs state
/// update; revision and decoder metadata are carried in response headers.
#[instrument(skip(state))]
pub async fn get_yrs_base(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    user_ext: Option<Extension<User>>,
    request_headers: HeaderMap,
) -> Result<Response, ApiError> {
    let board_id = Uuid::parse_str(&board_id).map_err(|_| {
        yrs_error(
            StatusCode::BAD_REQUEST,
            "INVALID_BOARD_ID",
            "Invalid board id.",
        )
    })?;
    let user = user_ext.as_ref().map(|Extension(u)| u);
    let embed_token = crate::handlers::board_embed::embed_token_from_headers(&request_headers);
    ensure_board_access(&state, board_id, user, embed_token.as_deref()).await?;
    let (base, head) = activate_yrs_read_path(&state, board_id).await?;

    let etag = format!("\"yrs-base-{}-{}\"", base.base_generation, base.base_seq);
    if request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == etag)
    {
        return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)]).into_response());
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    insert_i64(&mut headers, "x-base-seq", base.base_seq);
    insert_i64(&mut headers, "x-base-generation", base.base_generation);
    insert_i64(&mut headers, "x-server-client-id", base.server_client_id);
    insert_i64(&mut headers, "x-head-event-seq", head.processed_seq);
    insert_i64(
        &mut headers,
        "x-yrs-protocol-version",
        base.protocol_version as i64,
    );
    insert_i64(
        &mut headers,
        "x-yrs-schema-version",
        base.schema_version as i64,
    );
    insert_i64(
        &mut headers,
        "x-min-writer-version",
        base.min_writer_version as i64,
    );
    headers.insert("x-client-yupdate-v1", HeaderValue::from_static("true"));
    headers.insert("x-yrs-authority-v1", HeaderValue::from_static("true"));
    headers.insert(
        "x-update-encoding",
        HeaderValue::from_str(&base.update_encoding).map_err(|_| {
            yrs_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "YRS_METADATA_INVALID",
                "Invalid update encoding metadata.",
            )
        })?,
    );
    headers.insert("x-yrs-base-source", HeaderValue::from_static(base.source));
    let state_vector = base64::engine::general_purpose::STANDARD.encode(&base.state_vector);
    headers.insert(
        "x-yrs-state-vector",
        HeaderValue::from_str(&state_vector).map_err(|_| {
            yrs_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "YRS_METADATA_INVALID",
                "Invalid state vector metadata.",
            )
        })?,
    );
    Ok((headers, Body::from(base.state_update)).into_response())
}

/// Generation-fenced, ordered canonical update tail. `x-head-event-seq` is the
/// stable upper boundary captured before the query; a client that receives a
/// short page may advance its durable cursor to that value even when unrelated
/// reserved or deleted events created sequence gaps.
#[instrument(skip(state))]
pub async fn get_yrs_updates(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    Query(query): Query<YrsUpdatesQuery>,
    user_ext: Option<Extension<User>>,
    request_headers: HeaderMap,
) -> Result<Response, ApiError> {
    let board_id = Uuid::parse_str(&board_id).map_err(|_| {
        yrs_error(
            StatusCode::BAD_REQUEST,
            "INVALID_BOARD_ID",
            "Invalid board id.",
        )
    })?;
    let user = user_ext.as_ref().map(|Extension(u)| u);
    let embed_token = crate::handlers::board_embed::embed_token_from_headers(&request_headers);
    ensure_board_access(&state, board_id, user, embed_token.as_deref()).await?;
    let (base, head) = activate_yrs_read_path(&state, board_id).await?;

    if query.since_seq < base.base_seq
        || query.since_seq > head.processed_seq
        || query
            .base_generation
            .is_some_and(|g| g != base.base_generation)
    {
        return Err(yrs_error(
            StatusCode::GONE,
            "REBOOTSTRAP_REQUIRED",
            "The cursor is incompatible with the current canonical base generation.",
        ));
    }

    let limit = query.limit.unwrap_or(500).clamp(1, 2000);
    let mut rows = crate::database::yrs_updates::list_updates_page(
        &state.database,
        board_id,
        query.since_seq,
        head.processed_seq,
        base.base_generation,
        limit + 1,
    )
    .await
    .map_err(|_| {
        yrs_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "YRS_READ_FAILED",
            "Could not read Yrs updates.",
        )
    })?;
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.truncate(limit as usize);
    }
    let next_seq = if has_more {
        rows.last().map(|r| r.seq).unwrap_or(query.since_seq)
    } else {
        head.processed_seq
    };
    let body = crate::state::yrs_wire::encode_update_stream(&rows)
        .map_err(|e| yrs_error(StatusCode::INTERNAL_SERVER_ERROR, "YRS_ENCODE_FAILED", e))?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(crate::state::yrs_wire::YRS_UPDATES_CONTENT_TYPE),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    insert_i64(&mut headers, "x-min-event-seq", base.base_seq);
    insert_i64(&mut headers, "x-base-generation", base.base_generation);
    insert_i64(&mut headers, "x-head-event-seq", head.processed_seq);
    insert_i64(&mut headers, "x-next-event-seq", next_seq);
    headers.insert(
        "x-has-more",
        HeaderValue::from_static(if has_more { "true" } else { "false" }),
    );
    Ok((headers, Body::from(body)).into_response())
}

async fn ensure_board_access(
    state: &AppState,
    board_id: Uuid,
    user: Option<&User>,
    embed_token: Option<&str>,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    // A valid embed token grants view-only access (private-board iframe).
    if crate::handlers::board_embed::embed_grants_view(&state.database, board_id, embed_token).await
    {
        return Ok(());
    }

    let board = match get_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    if board.visibility == "public" || board.link_access != "none" {
        return Ok(());
    }

    let Some(user) = user else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Authentication required." })),
        ));
    };

    let is_admin = user.role_level >= 2;
    if is_admin {
        return Ok(());
    }

    let role = get_member_role(&state.database, board_id, user.id)
        .await
        .unwrap_or(None);
    if role.is_none() {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Access denied." })),
        ));
    }

    Ok(())
}

#[utoipa::path(
    post,
    path = "/boards/{board_id}/events",
    tag = "events",
    params(("board_id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    request_body = CommitEventBody,
    responses(
        (status = 201, description = "Event committed", body = CommitEventResponse),
        (status = 400, description = "Invalid payload"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn post_commit_event(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<CommitEventBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&board_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    if let Err(err) = ensure_board_access(&state, board_id, Some(&current_user), None).await {
        return Err(err);
    }

    // HTTP and WebSocket writes share the same canonical board coordinator.
    let resident = state
        .coordinators
        .ensure_active(&state.database, board_id)
        .await
        .map_err(|e| {
            tracing::warn!("yrs activation failed for board {board_id}: {e}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": format!("canonical state unavailable: {e}") })),
            )
        })?;
    match state
        .coordinators
        .commit(
            &state.database,
            &resident,
            board_id,
            current_user.id,
            &body.event_type,
            &body.payload,
            body.client_event_id.as_deref(),
            body.session_id.as_deref(),
            body.yrs.as_ref(),
        )
        .await
    {
        Ok(res) => {
            let commit = crate::realtime::WsMessage::Commit {
                board_id,
                seq: res.seq,
                server_event_id: res.server_event_id,
                client_event_id: body.client_event_id.clone(),
                user_id: current_user.id,
                event_type: body.event_type.clone(),
                payload: res.event_payload,
                session_id: body.session_id.clone(),
            };
            let yrs = crate::realtime::WsMessage::YrsUpdate {
                board_id,
                seq: res.seq,
                client_event_id: res.client_event_id,
                schema_version: res.schema_version,
                yupdate: res.yupdate_b64,
            };
            state.yrs_fanout.publish(commit, Some(yrs)).await;
            Ok((
                StatusCode::CREATED,
                Json(CommitEventResponse {
                    seq: res.seq,
                    server_event_id: res.server_event_id,
                    client_event_id: body.client_event_id,
                }),
            ))
        }
        Err(e) => {
            tracing::warn!("canonical HTTP commit failed for board {board_id}: {e}");
            Err((
                StatusCode::CONFLICT,
                Json(json!({ "error": format!("canonical commit rejected: {e}") })),
            ))
        }
    }
}

#[utoipa::path(
    get,
    path = "/boards/{board_id}/events",
    tag = "events",
    params(("board_id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Events list", body = [crate::models::events::EventRecord]),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_events_since(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: axum::http::HeaderMap,
    user_ext: Option<Extension<User>>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&board_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };
    let user = user_ext.as_ref().map(|Extension(u)| u);
    let embed_token = crate::handlers::board_embed::embed_token_from_headers(&headers);
    if let Err(err) = ensure_board_access(&state, board_id, user, embed_token.as_deref()).await {
        return Err(err);
    }
    let since = query.since.unwrap_or(0);
    let limit = query.limit.unwrap_or(500).clamp(1, 2000);

    match list_events_since(&state.database, board_id, since, limit).await {
        Ok(events) => {
            // Expose the lowest still-stored seq so a reconnecting client can
            // detect a gap (its last_seq < min-1 → events GC'd) and reload the
            // snapshot instead of silently missing data.
            let mut headers = axum::http::HeaderMap::new();
            if let Ok(Some(min_seq)) = min_event_seq(&state.database, board_id).await {
                if let Ok(value) = min_seq.to_string().parse() {
                    headers.insert("x-min-event-seq", value);
                }
            }
            Ok((headers, Json(events)))
        }
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch events." })),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/boards/{board_id}/snapshots/latest",
    tag = "events",
    params(("board_id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Snapshot", body = crate::models::events::SnapshotRecord),
        (status = 404, description = "Not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn get_latest_snapshot(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    user_ext: Option<Extension<User>>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    let board_id = Uuid::parse_str(&board_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid board id." })),
        )
    })?;
    let user = user_ext.as_ref().map(|Extension(u)| u);
    let embed_token = crate::handlers::board_embed::embed_token_from_headers(&headers);
    ensure_board_access(&state, board_id, user, embed_token.as_deref()).await?;

    match fetch_latest_snapshot(&state.database, board_id).await {
        Ok(Some(snapshot)) => {
            // The snapshot `seq` is a monotonic version → use it as an ETag so
            // clients can cache the body and skip re-downloading it unchanged.
            let etag = format!("\"snap-{}\"", snapshot.seq);
            let unchanged = headers
                .get(axum::http::header::IF_NONE_MATCH)
                .and_then(|v| v.to_str().ok())
                .map(|v| v == etag)
                .unwrap_or(false);
            if unchanged {
                return Ok(
                    (StatusCode::NOT_MODIFIED, [(axum::http::header::ETAG, etag)]).into_response(),
                );
            }
            Ok((
                [
                    (axum::http::header::ETAG, etag),
                    (
                        axum::http::header::CACHE_CONTROL,
                        "no-cache, private".to_string(),
                    ),
                ],
                Json(snapshot),
            )
                .into_response())
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Snapshot not found." })),
        )),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch snapshot." })),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/boards/{board_id}/snapshots",
    tag = "events",
    params(("board_id" = String, Path, description = "Board ID")),
    security(("jwt_token" = [])),
    request_body = SnapshotCreateBody,
    responses(
        (status = 201, description = "Snapshot created", body = crate::models::events::SnapshotRecord),
        (status = 400, description = "Invalid payload"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn post_snapshot(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    Extension(_current_user): Extension<User>,
    Json(body): Json<SnapshotCreateBody>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&board_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };
    if let Err(err) = ensure_board_access(&state, board_id, Some(&_current_user), None).await {
        return Err(err);
    }

    // A client-supplied flat snapshot is never authority for destructive GC.
    // The detached pass consumes only the freshness-fenced canonical asset
    // read model.
    let app = state.clone();
    tokio::spawn(async move {
        crate::storage::delete::gc_orphaned_board_storage_from_read_model(
            &app.database,
            &app.storage,
            board_id,
            300,
        )
        .await;
    });

    match insert_snapshot(&state.database, board_id, body.seq, body.state.clone()).await {
        Ok(snapshot) => Ok((StatusCode::CREATED, Json(snapshot))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not create snapshot." })),
        )),
    }
}

/// Recursively collect every uploaded-file object key referenced in a board
/// state JSON value. Matches any string field whose name normalizes to contain
/// "objectkey" (`object_key`, `poster_object_key`, `source_object_key`, …), so
/// all media element kinds (image/audio/video/file) are covered without
/// enumerating each type. Over-collecting only protects files from deletion, so
/// being generous is the safe direction.
pub(crate) fn collect_object_keys(
    v: &serde_json::Value,
    set: &mut std::collections::HashSet<String>,
) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map {
                if let serde_json::Value::String(s) = val {
                    let norm: String = k
                        .chars()
                        .filter(|c| *c != '_')
                        .flat_map(|c| c.to_lowercase())
                        .collect();
                    if norm.contains("objectkey") && !s.is_empty() {
                        set.insert(s.clone());
                    }
                }
                collect_object_keys(val, set);
            }
        }
        serde_json::Value::Array(arr) => {
            for val in arr {
                collect_object_keys(val, set);
            }
        }
        _ => {}
    }
}

#[derive(Deserialize, Debug)]
pub struct EventsBySessionQuery {
    pub user_id: String,
    pub session_id: String,
}

#[instrument(skip(state))]
pub async fn get_events_by_session(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    Query(query): Query<EventsBySessionQuery>,
    Extension(current_user): Extension<User>,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&board_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    if let Err(err) = ensure_board_access(&state, board_id, Some(&current_user), None).await {
        return Err(err);
    }

    let user_id = match Uuid::parse_str(&query.user_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid user_id." })),
            ))
        }
    };

    match list_events_by_user_session(&state.database, board_id, user_id, &query.session_id).await {
        Ok(events) => Ok(Json(events)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not fetch events." })),
        )),
    }
}
