//! WebSocket collaboration, presence, heartbeat, cursor, and commit handling.

use axum::response::IntoResponse;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Extension, Path, Query, State,
    },
    http::StatusCode,
};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use crate::cache::sessions;
use crate::core::config::get_env_bool;
use crate::database::board_members::get_member_role;
use crate::database::boards::get_board_by_id;
use crate::database::users::fetch_active_user_by_id_from_db;
use crate::models::user::User;
use crate::realtime::{
    broadcast_ws, decode_binary_client_commit, encode_ws, ClientWsMessage, WsMessage,
};
use crate::routes::AppState;

/// Per-board monotonic counter handing each connecting session a unique
/// *ordinal*. The client derives a non-overlapping entity-id range from it
/// (`loaded_max + MARGIN + ordinal*STRIDE`) so newly-created (synced) entities
/// from different clients never collide. In-memory only: on restart it resets to
/// 0, which is safe because each client offsets by its freshly-loaded `max` id
/// (which already includes everything created by earlier ordinals).
static ENTITY_ORDINALS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<Uuid, u64>>,
> = std::sync::OnceLock::new();

/// Coalesce several already-serialized durable messages into one WS frame:
/// `{"type":"batch","msgs":[<json0>,<json1>,...]}`. Each element is a complete
/// JSON object string (from [`encode_ws`]), so this is pure concatenation — no
/// re-serialization. The client unwraps and dispatches each `msg` in order.
fn encode_batch(msgs: &[axum::extract::ws::Utf8Bytes]) -> String {
    // Pre-size: header/footer + payloads + commas.
    let payload_len: usize = msgs.iter().map(|m| m.as_str().len()).sum();
    let mut s = String::with_capacity(payload_len + msgs.len() + 24);
    s.push_str(r#"{"type":"batch","msgs":["#);
    for (i, m) in msgs.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(m.as_str());
    }
    s.push_str("]}");
    s
}

/// Upper bound on messages coalesced into one batch frame. Sized to the commit
/// ring capacity so a consumer that was momentarily starved (scheduler didn't
/// wake its send task for a few seconds under a commit burst) drains its ENTIRE
/// backlog in one wake and catches up — instead of draining only a slice, still
/// trailing, and lagging again into a resync. The frame is bounded by the ring,
/// so it can't grow unbounded. Env-tunable so it can track a raised
/// `REALTIME_BROADCAST_CAPACITY` without a rebuild; default matches the commit
/// ring default (4096).
const DEFAULT_MAX_BATCH_DRAIN: u64 = 4096;

fn max_batch_drain() -> usize {
    crate::core::config::get_env_u64("REALTIME_MAX_BATCH_DRAIN", DEFAULT_MAX_BATCH_DRAIN).max(1)
        as usize
}

fn next_entity_ordinal(board_id: Uuid) -> u64 {
    let map =
        ENTITY_ORDINALS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = map.lock().unwrap_or_else(|e| e.into_inner());
    let counter = guard.entry(board_id).or_insert(0);
    let val = *counter;
    *counter += 1;
    val
}

/// Throttle for `users.last_active_at` writes: at most one DB update per user per
/// [`LAST_ACTIVE_THROTTLE`]. Heartbeats fire every 10s and a single drag can emit
/// many commits per second, so without this we'd hammer the `users` table.
static LAST_ACTIVE_WRITES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<Uuid, std::time::Instant>>,
> = std::sync::OnceLock::new();

const LAST_ACTIVE_THROTTLE: std::time::Duration = std::time::Duration::from_secs(60);

/// Stamp `users.last_active_at = NOW()` for a connected user — the "last ping /
/// last board action" signal shown in the admin users list — throttled per user.
/// Anonymous/embed sessions carry an id with no matching `users` row, so the
/// UPDATE is a harmless no-op for them.
async fn touch_user_last_active(db: &sqlx::PgPool, user_id: Uuid) {
    {
        let map = LAST_ACTIVE_WRITES
            .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut guard = map.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        if let Some(&last) = guard.get(&user_id) {
            if now.duration_since(last) < LAST_ACTIVE_THROTTLE {
                return;
            }
        }
        guard.insert(user_id, now);
    }
    if let Err(e) = sqlx::query("UPDATE users SET last_active_at = NOW() WHERE id = $1")
        .bind(user_id)
        .execute(db)
        .await
    {
        tracing::warn!("Failed to update last_active_at for {user_id}: {e}");
    }
}

#[derive(Debug, Deserialize)]
pub struct WsBoardQuery {
    pub session_id: Option<String>,
    pub display_name: Option<String>,
    /// View-only embed token (private-board iframe; no session cookie cross-site).
    pub embed_token: Option<String>,
    /// Test/automation mode: keep commits attributed to the authenticated user
    /// while exposing each socket as a separate presence/cursor identity.
    pub presence_per_session: Option<bool>,
    /// Client opts in to coalesced `{"type":"batch","msgs":[...]}` frames. Under a
    /// commit flood the send task drains the whole ring in one burst and ships it
    /// as a single frame, so a slow consumer stops lagging the broadcast (and the
    /// client applies many commits per event-loop turn). Clients that don't send
    /// this keep getting one frame per message (backward compatible).
    ///
    /// Typed as a String (not bool) on purpose: `serde_urlencoded` only parses a
    /// `bool` from the exact strings `true`/`false`, so `?batch=1` would fail
    /// deserialization and 400 the WHOLE upgrade. We accept any truthy value.
    pub batch: Option<String>,
    /// Only opted-in connections receive canonical `yrs_update` echoes.
    pub yrs_v1: Option<String>,
    /// Enables raw binary canonical frames in both WebSocket directions.
    pub yrs_binary: Option<String>,
    /// Declares that the client's content ECS is driven by canonical Yrs.
    pub yrs_authority: Option<String>,
}

/// Interpret the `?batch=` query flag leniently (`1`/`true`/`yes`/`on`).
fn batch_flag_enabled(v: Option<&str>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn is_yrs_update(bytes: &axum::extract::ws::Utf8Bytes) -> bool {
    bytes.as_str().contains(r#""type":"yrs_update""#)
}

/// Canonical connections receive the event envelope followed by its Yrs update.
/// The pair is the client's durable ordering boundary; event envelopes also
/// carry board configuration changes outside the content document.
fn should_deliver_durable(bytes: &axum::extract::ws::Utf8Bytes, yrs_supported: bool) -> bool {
    yrs_supported || !is_yrs_update(bytes)
}

#[cfg(test)]
mod durable_delivery_tests {
    use super::should_deliver_durable;
    use axum::extract::ws::Utf8Bytes;

    #[test]
    fn canonical_connections_keep_commit_and_yrs_pair() {
        let commit = Utf8Bytes::from(r#"{"type":"commit","event_type":"world_commit"}"#);
        let update = Utf8Bytes::from(r#"{"type":"yrs_update","seq":42}"#);
        assert!(should_deliver_durable(&commit, true));
        assert!(should_deliver_durable(&update, true));
    }

    #[test]
    fn non_yrs_connections_filter_yrs_updates() {
        let commit = Utf8Bytes::from(r#"{"type":"commit","event_type":"board_commit"}"#);
        let update = Utf8Bytes::from(r#"{"type":"yrs_update","seq":42}"#);
        assert!(should_deliver_durable(&commit, false));
        assert!(!should_deliver_durable(&update, false));
    }
}

fn encode_binary_yrs_update(bytes: &axum::extract::ws::Utf8Bytes) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let message: WsMessage = serde_json::from_str(bytes.as_str()).ok()?;
    let WsMessage::YrsUpdate {
        seq,
        client_event_id,
        schema_version,
        yupdate,
        ..
    } = message
    else {
        return None;
    };
    let yupdate = base64::engine::general_purpose::STANDARD
        .decode(yupdate)
        .ok()?;
    let envelope = crate::state::yrs_wire::YrsUpdateEnvelope {
        seq,
        client_event_id: client_event_id.unwrap_or_default(),
        schema_version,
        yupdate,
    };
    let mut out = Vec::new();
    crate::state::yrs_wire::encode_envelope(&envelope, &mut out).ok()?;
    Some(out)
}

#[instrument(skip(state, ws))]
pub async fn ws_board(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    Query(query): Query<WsBoardQuery>,
    user_ext: Option<Extension<User>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let board_id = match Uuid::parse_str(&board_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({ "error": "Invalid board id." })),
            )
                .into_response();
        }
    };

    let board = match get_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({ "error": "Board not found." })),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({ "error": "Could not fetch board." })),
            )
                .into_response();
        }
    };

    // A valid embed token opens a strictly view-only anonymous session, even for
    // a private board (the cross-site iframe carries no session cookie).
    let embed_ok = crate::handlers::board_embed::embed_grants_view(
        &state.database,
        board_id,
        query.embed_token.as_deref(),
    )
    .await;

    let (user_id, user_role) = if embed_ok {
        let anon_id = query
            .session_id
            .as_deref()
            .and_then(|s| Uuid::try_parse(s).ok())
            .unwrap_or_else(Uuid::new_v4);
        (anon_id, "viewer".to_string())
    } else if let Some(Extension(current_user)) = user_ext {
        // Authenticated user
        let member_role = if current_user.role_level >= 2 {
            // Administrators have unrestricted access to every board (mirrors the
            // REST board handlers), so they can always open the realtime socket.
            Some("owner".to_string())
        } else if board.owner_id == current_user.id {
            Some("owner".to_string())
        } else {
            get_member_role(&state.database, board_id, current_user.id)
                .await
                .unwrap_or(None)
        };

        if member_role.is_none() && board.link_access == "none" && board.visibility != "public" {
            return (
                StatusCode::FORBIDDEN,
                axum::Json(json!({ "error": "Access denied." })),
            )
                .into_response();
        }

        let role = member_role.unwrap_or_else(|| {
            if board.link_access != "none" {
                board.link_access.clone()
            } else {
                "viewer".to_string()
            }
        });

        (current_user.id, role)
    } else {
        // Anonymous user — allow public boards and boards with link access
        if board.visibility != "public" && board.link_access == "none" {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({ "error": "Authentication required." })),
            )
                .into_response();
        }
        // Generate a deterministic UUID from the session_id, or a random one
        let anon_id = query
            .session_id
            .as_deref()
            .and_then(|s| Uuid::try_parse(s).ok())
            .unwrap_or_else(Uuid::new_v4);
        let role = if board.link_access != "none" {
            board.link_access.clone()
        } else {
            "viewer".to_string()
        };
        (anon_id, role)
    };

    let presence_per_session_enabled = get_env_bool("REALTIME_PRESENCE_PER_SESSION_ENABLED", false);
    let presence_user_id =
        if presence_per_session_enabled && query.presence_per_session.unwrap_or(false) {
            query
                .session_id
                .as_deref()
                .and_then(|s| Uuid::try_parse(s).ok())
                .unwrap_or_else(Uuid::new_v4)
        } else {
            user_id
        };
    let session_id = query.session_id;
    let display_name = query.display_name;
    let batch_supported = batch_flag_enabled(query.batch.as_deref());
    let yrs_supported = batch_flag_enabled(query.yrs_v1.as_deref());
    let yrs_binary_supported = batch_flag_enabled(query.yrs_binary.as_deref());
    let authority_requested = batch_flag_enabled(query.yrs_authority.as_deref());
    let authority_supported = authority_requested && yrs_supported && yrs_binary_supported;
    if authority_requested && !authority_supported {
        tracing::warn!(board_id = %board_id, "client requested yrs authority without negotiated server capability");
    }

    ws.on_upgrade(move |socket| {
        handle_socket(
            state,
            board_id,
            user_id,
            presence_user_id,
            session_id,
            user_role,
            display_name,
            batch_supported,
            yrs_supported,
            yrs_binary_supported,
            socket,
        )
    })
}

/// Cached identity (username/display name/presigned avatar) for one presence
/// user. The presence list is rebuilt on every connect/disconnect; previously
/// each rebuild ran one DB query AND one S3 presign PER connected user, so a
/// reconnect storm became O(N²) DB load that drained the connection pool. These
/// fields change rarely, so we cache them for [`IDENTITY_TTL`] and only refetch
/// on miss. Cursor positions are NOT cached — they come fresh from Redis each
/// rebuild.
struct CachedIdentity {
    username: String,
    display_name: String,
    /// Presigned avatar URL (valid ~900s; we re-presign well within that).
    profile_picture_url: Option<String>,
    fetched: std::time::Instant,
}

static USER_IDENTITY_CACHE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, CachedIdentity>>,
> = std::sync::OnceLock::new();

const IDENTITY_TTL: std::time::Duration = std::time::Duration::from_secs(45);
/// Soft cap so the cache can't grow unbounded across many distinct users; when
/// exceeded we drop expired entries.
const IDENTITY_CACHE_SOFT_CAP: usize = 5000;

fn identity_cache() -> &'static std::sync::Mutex<std::collections::HashMap<String, CachedIdentity>>
{
    USER_IDENTITY_CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

async fn build_sessions_users(state: &AppState, board_id_str: &str) -> Option<Vec<Value>> {
    let all_sessions = sessions::get_session_values(&state.cache, board_id_str)
        .await
        .ok()?;
    let mut seen = std::collections::HashSet::new();
    let mut unique_user_ids = Vec::new();
    for s in &all_sessions {
        let uid = s.split(':').next().unwrap_or("").to_string();
        if !uid.is_empty() && seen.insert(uid.clone()) {
            unique_user_ids.push(uid);
        }
    }
    let cursor_positions =
        sessions::get_cursor_positions(&state.cache, board_id_str, &unique_user_ids)
            .await
            .unwrap_or_default();

    let now = std::time::Instant::now();

    // Decide which uids are missing/stale in the cache. Registered users (valid
    // UUIDs) go to a batched DB fetch; non-UUID ids are anonymous.
    let mut to_fetch_db: Vec<Uuid> = Vec::new();
    let mut to_fetch_anon: Vec<String> = Vec::new();
    {
        let cache = identity_cache().lock().unwrap_or_else(|e| e.into_inner());
        for uid_str in &unique_user_ids {
            let fresh = cache
                .get(uid_str)
                .map(|c| now.duration_since(c.fetched) < IDENTITY_TTL)
                .unwrap_or(false);
            if fresh {
                continue;
            }
            match Uuid::parse_str(uid_str) {
                Ok(id) => to_fetch_db.push(id),
                Err(_) => to_fetch_anon.push(uid_str.clone()),
            }
        }
    }

    // ONE batched query for all stale registered users (replaces the N+1).
    if !to_fetch_db.is_empty() {
        match crate::database::users::fetch_active_user_identities(&state.database, &to_fetch_db)
            .await
        {
            Ok(found) => {
                let mut found_ids = std::collections::HashSet::new();
                let mut computed: Vec<(String, CachedIdentity)> = Vec::new();
                for u in found {
                    found_ids.insert(u.id);
                    let display_name = if u.first_name.is_some() || u.last_name.is_some() {
                        [u.first_name.as_deref(), u.last_name.as_deref()]
                            .iter()
                            .filter_map(|s| *s)
                            .collect::<Vec<_>>()
                            .join(" ")
                    } else {
                        u.username.clone()
                    };
                    // Presign the stored avatar so clients can fetch it from the
                    // private bucket. Only happens on a cache miss now.
                    let avatar = match &u.profile_picture_url {
                        Some(stored) => {
                            crate::storage::presign_url::presign_stored_url(&state.storage, stored)
                                .await
                        }
                        None => None,
                    };
                    computed.push((
                        u.id.to_string(),
                        CachedIdentity {
                            username: u.username,
                            display_name,
                            profile_picture_url: avatar,
                            fetched: now,
                        },
                    ));
                }
                // Requested UUIDs not returned have no active `users` row → treat
                // as anonymous (look up their Redis display name below).
                for id in &to_fetch_db {
                    if !found_ids.contains(id) {
                        to_fetch_anon.push(id.to_string());
                    }
                }
                let mut cache = identity_cache().lock().unwrap_or_else(|e| e.into_inner());
                for (k, v) in computed {
                    cache.insert(k, v);
                }
            }
            Err(e) => {
                tracing::warn!("build_sessions_users: batch identity fetch failed: {e}");
                // Fall back to anonymous rendering for these this rebuild.
                for id in &to_fetch_db {
                    to_fetch_anon.push(id.to_string());
                }
            }
        }
    }

    // Anonymous display names (Redis) — only for misses, then cached too.
    for uid_str in &to_fetch_anon {
        let anon_name = sessions::get_anon_display_name(&state.cache, board_id_str, uid_str)
            .await
            .unwrap_or(None)
            .unwrap_or_else(|| "Аноним".to_string());
        let mut cache = identity_cache().lock().unwrap_or_else(|e| e.into_inner());
        cache.insert(
            uid_str.clone(),
            CachedIdentity {
                username: anon_name.clone(),
                display_name: anon_name,
                profile_picture_url: None,
                fetched: now,
            },
        );
    }

    // Assemble the list from cached identities + fresh cursor positions.
    let mut users = Vec::new();
    {
        let mut cache = identity_cache().lock().unwrap_or_else(|e| e.into_inner());
        for uid_str in &unique_user_ids {
            let (cursor_x, cursor_y) = match cursor_positions.get(uid_str) {
                Some((x, y)) => (json!(*x), json!(*y)),
                None => (json!(null), json!(null)),
            };
            let (username, display_name, avatar) = match cache.get(uid_str) {
                Some(c) => (
                    c.username.clone(),
                    c.display_name.clone(),
                    c.profile_picture_url.clone(),
                ),
                // Shouldn't happen (just populated), but stay graceful.
                None => ("Аноним".to_string(), "Аноним".to_string(), None),
            };
            users.push(json!({
                "user_id": uid_str,
                "username": username,
                "display_name": display_name,
                "profile_picture_url": avatar,
                "cursor_x": cursor_x,
                "cursor_y": cursor_y,
            }));
        }
        // Opportunistic eviction of expired entries when the cache grows large.
        if cache.len() > IDENTITY_CACHE_SOFT_CAP {
            cache.retain(|_, c| now.duration_since(c.fetched) < IDENTITY_TTL);
        }
    }
    Some(users)
}

async fn handle_socket(
    state: Arc<AppState>,
    board_id: Uuid,
    actor_user_id: Uuid,
    presence_user_id: Uuid,
    session_id: Option<String>,
    user_role: String,
    display_name: Option<String>,
    batch_supported: bool,
    yrs_supported: bool,
    yrs_binary_supported: bool,
    socket: WebSocket,
) {
    let board_id_str = board_id.to_string();
    let presence_user_id_str = presence_user_id.to_string();

    // Store anonymous display name in Redis if provided
    if let Some(ref name) = display_name {
        if !name.is_empty() {
            let _ = sessions::set_anon_display_name(
                &state.cache,
                &board_id_str,
                &presence_user_id_str,
                name,
            )
            .await;
        }
    }

    // Subscribe first so the connecting client receives its own sessions_update.
    // Durable state (commits/acks/sessions) and transient previews ride separate
    // channels: a preview flood can no longer evict committed events or force a
    // spurious resync.
    state.yrs_fanout.register_board(board_id).await;
    let channels = state.realtime.channels_for(board_id).await;
    let preview_sender = channels.previews.clone();
    // Per-board latest-cursor map fed by cursor-only previews (see
    // `broadcast_cursor!`); a single aggregator broadcasts it, so cursors don't
    // fan out one-per-move. `None` when aggregation is disabled (legacy fan-out).
    let cursor_map =
        crate::realtime::cursor_aggregation_enabled().then(|| channels.cursors.clone());
    let mut commit_rx = channels.commits.subscribe();
    let mut preview_rx = channels.previews.subscribe();

    // Register session in Redis on connect
    if let Some(ref sid) = session_id {
        if let Err(e) =
            sessions::add_session(&state.cache, &board_id_str, &presence_user_id_str, sid).await
        {
            tracing::warn!("Failed to register session in Redis: {}", e);
        }

        // Touch heartbeat so the cleanup job knows this session is alive
        let _ = sessions::touch_session_heartbeat(
            &state.cache,
            &board_id_str,
            &presence_user_id_str,
            sid,
        )
        .await;

        // Record activity immediately on connect (before the first heartbeat tick).
        touch_user_last_active(&state.database, actor_user_id).await;

        // Broadcast sessions update with enriched user data. Presence is
        // droppable and self-healed by periodic `/sessions` reconciliation, so it
        // must not occupy the durable commit ring or trigger board resyncs.
        if let Some(users) = build_sessions_users(&state, &board_id_str).await {
            broadcast_ws(
                &preview_sender,
                WsMessage::SessionsUpdate { board_id, users },
            );
        }
    }

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Hand this session its unique entity-id ordinal (first, before any edits).
    // Clients that don't understand `id_ordinal` ignore it (graceful rollout).
    let ordinal = next_entity_ordinal(board_id);
    let _ = ws_tx
        .send(Message::Text(
            json!({ "type": "id_ordinal", "ordinal": ordinal })
                .to_string()
                .into(),
        ))
        .await;

    // Per-connection channel for THIS session's own commit acks. Acks are only
    // meaningful to the committing client (web ignores them entirely; the desktop
    // Rust client no-ops on them), so instead of broadcasting each ack to ALL
    // subscribers — half the durable ring traffic, pure waste for everyone else —
    // the recv task routes it here and the send task delivers it to just us.
    // Bounded + drop-on-full: a client that floods commits without reading its
    // socket must not grow an unbounded ack backlog; dropping an ignored ack is
    // harmless (that client already resyncs on the commit-ring lag). Raised
    // 256→1024 for 8 GB headroom (a burst-committing client keeps more of its
    // own acks in flight before any are dropped); env-tunable via
    // `REALTIME_CONN_SEND_BUFFER`. Cost is per live connection, so keep it
    // modest — 1024 `Utf8Bytes` handles ≈ tens of KB per conn, ~tens of MB at
    // 500 users.
    let conn_send_buffer =
        crate::core::config::get_env_u64("REALTIME_CONN_SEND_BUFFER", 1024).max(1) as usize;
    let (ack_tx, mut ack_rx) =
        tokio::sync::mpsc::channel::<axum::extract::ws::Utf8Bytes>(conn_send_buffer);

    // Cooperative shutdown signal for recv_task. recv_task runs the commit
    // transaction (begin/insert/commit) inline, so it must NEVER be `.abort()`ed:
    // aborting a task that holds a live sqlx `Transaction` drops the query future
    // mid-wire-exchange, leaving the pooled connection with unread protocol state
    // plus a queued-but-unsent ROLLBACK. That connection returns to the pool
    // poisoned — every later query on it fails with "current transaction is
    // aborted, commands ignored until end of transaction block", and it holds its
    // row locks until the process restarts (the 30s–500s lock waits, deadlocks and
    // the snapshot job dying every minute all trace back to one such connection).
    // Instead we flip this watch; recv_task observes it only at its safe await
    // point (between messages) and breaks after finishing any in-flight commit.
    let (recv_shutdown_tx, recv_shutdown_rx) = tokio::sync::watch::channel(false);

    let hb_cache = state.cache.clone();
    let hb_db = state.database.clone();
    let hb_board_id = board_id_str.clone();
    let hb_user_id = presence_user_id_str.clone();
    let hb_session_id = session_id.clone();
    let mut send_task = tokio::spawn(async move {
        let max_batch_drain = max_batch_drain();
        // A binary Yrs frame cannot be embedded in the legacy JSON batch.
        // Preserve strict Commit→Yrs ordering by sending canonical binary
        // connections one ring item at a time.
        let batch_supported = batch_supported && !yrs_binary_supported;
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
        heartbeat.tick().await; // consume the immediate first tick
                                // Debounce window for resync requests on this connection (see the
                                // commit-lag arm). NOT a "don't resync for X seconds" ban: each resync
                                // fetches CURRENT state, so at most one is in flight per window and the
                                // client is never more than ~this stale.
        let mut last_resync_at: Option<std::time::Instant> = None;
        const RESYNC_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(1000);
        loop {
            tokio::select! {
                biased;

                ack = ack_rx.recv() => {
                    // Our own commit ack (targeted, off the broadcast ring).
                    let Some(bytes) = ack else { break };
                    if ws_tx.send(Message::Text(bytes)).await.is_err() {
                        break;
                    }
                }
                msg = commit_rx.recv() => {
                    match msg {
                        // Already serialized once at broadcast time. If the client
                        // opted into batching, drain everything ELSE currently
                        // buffered and ship it as ONE frame: a slow consumer then
                        // empties the ring in bursts (so it stops lagging) and
                        // applies many commits per event-loop turn. Otherwise send
                        // the single frame as before.
                        Ok(bytes) => {
                            if !should_deliver_durable(&bytes, yrs_supported) {
                                continue;
                            }
                            if !batch_supported {
                                let frame = if yrs_binary_supported && is_yrs_update(&bytes) {
                                    match encode_binary_yrs_update(&bytes) {
                                        Some(binary) => Message::Binary(binary.into()),
                                        None => continue,
                                    }
                                } else {
                                    Message::Text(bytes)
                                };
                                if ws_tx.send(frame).await.is_err() {
                                    break;
                                }
                            } else {
                                let mut batch: Vec<axum::extract::ws::Utf8Bytes> = vec![bytes];
                                let mut lagged_during_drain = false;
                                while batch.len() < max_batch_drain {
                                    match commit_rx.try_recv() {
                                        Ok(more) => {
                                            if should_deliver_durable(&more, yrs_supported) {
                                                batch.push(more);
                                            }
                                        }
                                        Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                                        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                                            // Dropped mid-drain: send what we have,
                                            // then fall into the resync path below.
                                            lagged_during_drain = true;
                                            break;
                                        }
                                        Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                                    }
                                }
                                let frame = if batch.len() == 1 {
                                    Message::Text(batch.pop().unwrap())
                                } else {
                                    Message::Text(encode_batch(&batch).into())
                                };
                                if ws_tx.send(frame).await.is_err() {
                                    break;
                                }
                                if lagged_during_drain {
                                    let now = std::time::Instant::now();
                                    let due = last_resync_at
                                        .map_or(true, |t| now.duration_since(t) >= RESYNC_DEBOUNCE);
                                    if due {
                                        last_resync_at = Some(now);
                                        tracing::warn!(
                                            "realtime: client lagged mid-batch-drain — requesting resync"
                                        );
                                        if ws_tx
                                            .send(Message::Text(r#"{"type":"resync"}"#.into()))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        // This receiver fell behind and the channel dropped `n`
                        // DURABLE commit broadcasts for it. The
                        // stream resumes from the current tail, so tell the client
                        // to resync — but DEBOUNCE: while a recent resync is still
                        // effectively in flight, coalesce further requests. A burst
                        // of lag events then triggers ONE resync (which fetches the
                        // latest snapshot+events = current state), not a storm of
                        // snapshot fetches that hammer the DB.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            let now = std::time::Instant::now();
                            let due = last_resync_at
                                .map_or(true, |t| now.duration_since(t) >= RESYNC_DEBOUNCE);
                            if due {
                                last_resync_at = Some(now);
                                tracing::warn!(
                                    "realtime: client lagged, dropped {n} commit broadcasts — requesting resync"
                                );
                                if ws_tx
                                    .send(Message::Text(r#"{"type":"resync"}"#.into()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            // else: coalesced — the in-flight/just-sent resync will
                            // bring the client up to current state anyway.
                            continue;
                        }
                        // Channel closed (sender gone): the board has no more
                        // producers — end this send task.
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                pmsg = preview_rx.recv() => {
                    match pmsg {
                        Ok(bytes) => {
                            // COALESCE previews under backpressure. Previews are
                            // transient (cursor/drag/presence frames) — only the
                            // newest is worth showing. The select is biased above,
                            // so if commits are also ready they drain first; this
                            // branch only spends a send slot on preview traffic
                            // when the durable stream is currently empty.
                            let mut latest = bytes;
                            loop {
                                match preview_rx.try_recv() {
                                    Ok(newer) => latest = newer,
                                    Err(_) => break, // Empty / Lagged / Closed
                                }
                            }
                            if ws_tx.send(Message::Text(latest)).await.is_err() {
                                break;
                            }
                        }
                        // Preview frames are TRANSIENT: a lagged receiver only
                        // missed some ephemeral drag/cursor frames. Silently
                        // resume from the tail — no resync, no client-visible
                        // effect (the next preview/commit corrects the view).
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_n)) => {
                            continue;
                        }
                        // Previews share the board's lifetime with commits; if
                        // this closes, the commit channel will too and end us.
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = heartbeat.tick() => {
                    // Protocol-level Ping: browser auto-responds with Pong,
                    // server detects dead clients when send fails.
                    if ws_tx.send(Message::Ping(vec![].into())).await.is_err() {
                        break;
                    }
                    // Application-level ping: browser WebSocket API can't see
                    // protocol-level Ping/Pong, so we also send a text message
                    // the client uses to track connection liveness.
                    if ws_tx.send(Message::Text(r#"{"type":"ping"}"#.into())).await.is_err() {
                        break;
                    }
                    // Renew session heartbeat in Redis so cleanup job doesn't remove us
                    if let Some(ref sid) = hb_session_id {
                        let _ = sessions::touch_session_heartbeat(
                            &hb_cache, &hb_board_id, &hb_user_id, sid,
                        ).await;
                    }
                    // Persist the ping as last activity (throttled) for the users list.
                    touch_user_last_active(&hb_db, actor_user_id).await;
                }
            }
        }
    });

    let state_clone = state.clone();
    let session_id_clone = session_id.clone();
    let board_id_str_clone = board_id_str.clone();
    let presence_user_id_str_clone = presence_user_id_str.clone();
    let user_role_clone = user_role;
    let mut recv_task = tokio::spawn(async move {
        let mut recv_shutdown_rx = recv_shutdown_rx;
        // Mark the initial `false` as seen so `changed()` only fires on the real
        // shutdown `send(true)`, never on the first poll.
        let _ = recv_shutdown_rx.borrow_and_update();
        // Cursor-preview throttle. A session's cursor-only previews are coalesced
        // to at most one broadcast per CURSOR_MIN_INTERVAL, with the latest flushed
        // on a timer so the final resting position always lands (no cursor stuck
        // behind where the user stopped). The interval matches the client's own
        // remote-cursor render cadence (REMOTE_CURSOR_RENDER_INTERVAL_MS ≈ 33ms /
        // 30fps): the client already re-rendered remote cursors no faster than
        // this, so frames above this rate were parsed on its main thread then
        // dropped — throttling them here removes pure waste WITHOUT making cursors
        // jerk. ONLY cursor-only previews are throttled; drag/transform/laser
        // previews pass through immediately so remote manipulation stays smooth.
        // The LOCAL user's own cursor is native (not networked) — their latency is
        // unaffected.
        const CURSOR_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(33);
        let mut pending_cursor: Option<serde_json::Value> = None;
        let mut last_cursor_at: Option<std::time::Instant> = None;
        let mut cursor_flush = tokio::time::interval(CURSOR_MIN_INTERVAL);
        cursor_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        macro_rules! broadcast_cursor {
            // `$is_cursor_only` decides the fan-out path: a bare cursor folds into
            // the per-board aggregation map (when enabled); a drag/transform/laser
            // preview still broadcasts immediately for low latency.
            ($payload:expr, $is_cursor_only:expr) => {{
                let payload = $payload;
                let is_cursor_only = $is_cursor_only;
                // Persist the cursor position to Redis (presence list / reconnect)
                // whenever the payload carries coordinates — same as before.
                if let (Some(x), Some(y)) = (
                    payload
                        .get("cursor")
                        .and_then(|c| c.get("x"))
                        .and_then(|v| v.as_f64()),
                    payload
                        .get("cursor")
                        .and_then(|c| c.get("y"))
                        .and_then(|v| v.as_f64()),
                ) {
                    let cache = state_clone.cache.clone();
                    let bid = board_id_str_clone.clone();
                    let uid = presence_user_id_str_clone.clone();
                    tokio::spawn(async move {
                        let _ = sessions::set_cursor_position(&cache, &bid, &uid, x, y).await;
                    });
                }
                match (is_cursor_only, cursor_map.as_ref()) {
                    // Cursor-only + aggregation ON: fold into the per-board map.
                    // The board's aggregator broadcasts every cursor in ONE frame
                    // at a fixed rate, so there is no per-move fan-out — this is
                    // what removes the O(N²) cursor cost at 500+ users.
                    (true, Some(map)) => {
                        let mut guard = map.lock().unwrap_or_else(|p| p.into_inner());
                        guard.insert(
                            presence_user_id,
                            crate::realtime::CursorEntry {
                                user_id: presence_user_id,
                                payload,
                                updated_at: std::time::Instant::now(),
                            },
                        );
                    }
                    // Drag/transform/laser previews, or aggregation disabled:
                    // broadcast individually and immediately (legacy path).
                    _ => {
                        broadcast_ws(
                            &preview_sender,
                            WsMessage::Preview {
                                board_id,
                                user_id: presence_user_id,
                                payload,
                            },
                        );
                    }
                }
            }};
        }
        loop {
            tokio::select! {
                // Observed ONLY here, between messages — never while a commit tx is
                // in flight — so shutdown can't poison the connection.
                _ = recv_shutdown_rx.changed() => break,
                maybe_msg = ws_rx.next() => {
                    let Some(Ok(msg)) = maybe_msg else { break };
                    let client_msg = match msg {
                        Message::Text(text) => serde_json::from_str(&text).ok(),
                        Message::Binary(bytes) if yrs_binary_supported => {
                            decode_binary_client_commit(&bytes).ok()
                        }
                        Message::Close(_) => break,
                        _ => None,
                    };
                    let Some(client_msg) = client_msg else { continue };
                    match client_msg {
                        ClientWsMessage::Preview { payload } => {
                            // Cursor-only previews (no transform/handle/overlay/
                            // laser) are throttled + coalesced; everything else
                            // (drag/transform/laser) passes through immediately.
                            let is_cursor_only = payload.get("cursor").is_some()
                                && payload.get("transforms").is_none()
                                && payload.get("handle").is_none()
                                && payload.get("overlays").is_none()
                                && payload.get("laser").is_none();
                            let due = last_cursor_at.map_or(true, |t| {
                                std::time::Instant::now().duration_since(t) >= CURSOR_MIN_INTERVAL
                            });
                            if is_cursor_only && !due {
                                // Too soon — keep only the latest; the flush timer
                                // sends it so the resting position always lands.
                                pending_cursor = Some(payload);
                            } else {
                                if is_cursor_only {
                                    last_cursor_at = Some(std::time::Instant::now());
                                    pending_cursor = None;
                                }
                                broadcast_cursor!(payload, is_cursor_only);
                            }
                        }
                        ClientWsMessage::Commit {
                            event_type,
                            payload,
                            client_event_id,
                            session_id: msg_session_id,
                            yrs,
                        } => {
                            if user_role_clone == "viewer" {
                                tracing::warn!(
                                    "Viewer {} attempted commit on board {}, ignoring",
                                    actor_user_id,
                                    board_id
                                );
                                continue;
                            }
                            // Use session_id from the message if provided, otherwise fall back to connection-level session_id
                            let effective_session_id =
                                msg_session_id.or_else(|| session_id_clone.clone());

                            // The canonical coordinator owns sequence reservation,
                            // event insertion, and the Yrs update in one transaction.
                            match state_clone
                                .coordinators
                                .ensure_active(&state_clone.database, board_id)
                                .await
                            {
                                Ok(resident) => {
                                    match state_clone
                                        .coordinators
                                        .commit(
                                            &state_clone.database,
                                            &resident,
                                            board_id,
                                            actor_user_id,
                                            &event_type,
                                            &payload,
                                            client_event_id.as_deref(),
                                            effective_session_id.as_deref(),
                                            yrs.as_ref(),
                                        )
                                        .await
                                    {
                                        Ok(res) => {
                                            touch_user_last_active(
                                                &state_clone.database,
                                                actor_user_id,
                                            )
                                            .await;
                                            // Ack ONLY to the committer (off the ring).
                                            if let Some(ack_bytes) = encode_ws(&WsMessage::Ack {
                                                board_id,
                                                seq: res.seq,
                                                server_event_id: res.server_event_id,
                                                client_event_id: client_event_id.clone(),
                                            }) {
                                                let _ = ack_tx.try_send(ack_bytes);
                                            }
                                            // Local delivery plus a remote wake-up.
                                            // Other instances replay the
                                            // exact durable pair from PostgreSQL.
                                            let commit = WsMessage::Commit {
                                                board_id,
                                                seq: res.seq,
                                                server_event_id: res.server_event_id,
                                                client_event_id: client_event_id.clone(),
                                                user_id: actor_user_id,
                                                event_type: event_type.clone(),
                                                payload: res.event_payload,
                                                session_id: effective_session_id.clone(),
                                            };
                                            let yrs = WsMessage::YrsUpdate {
                                                board_id,
                                                seq: res.seq,
                                                client_event_id: res.client_event_id,
                                                schema_version: res.schema_version,
                                                yupdate: res.yupdate_b64,
                                            };
                                            state_clone.yrs_fanout.publish(commit, Some(yrs)).await;
                                        }
                                        Err(e) => {
                                            // Neither the event nor its Yrs update was
                                            // committed. Do not retry through another
                                            // path that could persist an unpaired event.
                                            tracing::warn!(
                                                "canonical commit failed for board {board_id}, user {actor_user_id}: {e}"
                                            );
                                            if let Some(nack_bytes) = encode_ws(&WsMessage::Nack {
                                                board_id,
                                                client_event_id: client_event_id.clone(),
                                                code: e.code().to_string(),
                                                reason: e.to_string(),
                                                retryable: e.retryable(),
                                                rebootstrap_required: !e.retryable(),
                                            }) {
                                                let _ = ack_tx.try_send(nack_bytes);
                                            }
                                        }
                                    }
                                    continue;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "yrs activation failed for board {board_id}: {e}"
                                    );
                                    if let Some(nack_bytes) = encode_ws(&WsMessage::Nack {
                                        board_id,
                                        client_event_id: client_event_id.clone(),
                                        code: "CANONICAL_STATE_UNAVAILABLE".to_string(),
                                        reason: e,
                                        retryable: true,
                                        rebootstrap_required: true,
                                    }) {
                                        let _ = ack_tx.try_send(nack_bytes);
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }
                _ = cursor_flush.tick() => {
                    // Flush the coalesced latest cursor (if any) on the interval.
                    // `pending_cursor` is only ever set for cursor-only previews.
                    if let Some(payload) = pending_cursor.take() {
                        last_cursor_at = Some(std::time::Instant::now());
                        broadcast_cursor!(payload, true);
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = &mut send_task => {
            // Sender half ended (client disconnected / send error). Signal
            // recv_task to stop cooperatively and await it — do NOT abort, it may
            // be mid-commit and aborting would poison its pooled connection. It
            // finishes any in-flight transaction, then breaks at its select!,
            // releasing its broadcast receivers before we evict idle channels.
            let _ = recv_shutdown_tx.send(true);
            let _ = recv_task.await;
        }
        _ = &mut recv_task => {
            // recv_task ended on its own (Close/EOF) — its tx is already
            // committed/rolled back. send_task holds no DB tx, so aborting is safe.
            send_task.abort();
            let _ = send_task.await;
        }
    }

    // Cleanup session and cursor from Redis on disconnect
    if let Some(ref sid) = session_id {
        if let Err(e) =
            sessions::remove_session(&state.cache, &board_id_str, &presence_user_id_str, sid).await
        {
            tracing::warn!("Failed to remove session from Redis: {}", e);
        }

        let _ = sessions::remove_cursor(&state.cache, &board_id_str, &presence_user_id_str).await;
        let _ =
            sessions::remove_anon_display_name(&state.cache, &board_id_str, &presence_user_id_str)
                .await;

        // Broadcast sessions update after disconnect with enriched user data.
        // Presence is droppable; clients periodically reconcile it via REST.
        if let Some(users) = build_sessions_users(&state, &board_id_str).await {
            let sender = state.realtime.preview_sender_for(board_id).await;
            broadcast_ws(&sender, WsMessage::SessionsUpdate { board_id, users });
        }
    }

    // Last client gone → free this board's broadcast ring buffers now instead of
    // letting them linger until some other new board triggers a sweep. Must run
    // AFTER the SessionsUpdate above (which re-opens the channel to notify any
    // remaining peers); if peers remain, `evict_if_idle` is a no-op.
    state.realtime.evict_if_idle(board_id).await;
}

/// Lists the active realtime sessions currently registered for a board.
pub async fn get_board_sessions(
    State(state): State<Arc<AppState>>,
    Path(board_id): Path<String>,
    user_ext: Option<Extension<User>>,
) -> impl IntoResponse {
    let board_id_uuid = match Uuid::parse_str(&board_id) {
        Ok(id) => id,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                axum::Json(json!({ "error": "Invalid board id." })),
            ))
        }
    };

    let board = match get_board_by_id(&state.database, board_id_uuid).await {
        Ok(Some(board)) => board,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                axum::Json(json!({ "error": "Board not found." })),
            ))
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({ "error": "Could not fetch board." })),
            ))
        }
    };

    if board.link_access == "none" {
        let current_user = match user_ext.as_ref().map(|Extension(u)| u) {
            Some(u) => u,
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    axum::Json(json!({ "error": "Authentication required." })),
                ))
            }
        };
        // Administrators bypass membership checks on every board.
        if current_user.role_level < 2 {
            let role = get_member_role(&state.database, board_id_uuid, current_user.id)
                .await
                .unwrap_or(None);
            if role.is_none() {
                return Err((
                    StatusCode::FORBIDDEN,
                    axum::Json(json!({ "error": "Access denied." })),
                ));
            }
        }
    }

    let session_list = match sessions::get_sessions(&state.cache, &board_id).await {
        Ok(s) => s,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({ "error": format!("Could not fetch sessions: {}", e) })),
            ))
        }
    };

    // Collect unique user_ids
    let mut seen = std::collections::HashSet::new();
    let mut unique_user_ids = Vec::new();
    for s in &session_list {
        if seen.insert(s.user_id.clone()) {
            unique_user_ids.push(s.user_id.clone());
        }
    }

    // Fetch cursor positions for all active users
    let cursor_positions =
        sessions::get_cursor_positions(&state.cache, &board_id, &unique_user_ids)
            .await
            .unwrap_or_default();

    // Fetch user info for each unique user_id
    let mut active_users = Vec::new();
    for uid_str in &unique_user_ids {
        let uid = match Uuid::parse_str(uid_str) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let (cursor_x, cursor_y) = match cursor_positions.get(uid_str) {
            Some((x, y)) => (json!(*x), json!(*y)),
            None => (json!(null), json!(null)),
        };
        if let Ok(Some(user)) = fetch_active_user_by_id_from_db(&state.database, uid).await {
            let display_name = if user.first_name.is_some() || user.last_name.is_some() {
                [user.first_name.as_deref(), user.last_name.as_deref()]
                    .iter()
                    .filter_map(|s| *s)
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                user.username.clone()
            };
            active_users.push(json!({
                "user_id": user.id,
                "username": user.username,
                "display_name": display_name,
                "profile_picture_url": user.profile_picture_url,
                "cursor_x": cursor_x,
                "cursor_y": cursor_y,
            }));
        } else {
            let anon_name = sessions::get_anon_display_name(&state.cache, &board_id, uid_str)
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| "Аноним".to_string());
            active_users.push(json!({
                "user_id": uid_str,
                "username": &anon_name,
                "display_name": &anon_name,
                "profile_picture_url": null,
                "cursor_x": cursor_x,
                "cursor_y": cursor_y,
            }));
        }
    }

    Ok(axum::Json(json!({
        "sessions": session_list,
        "users": active_users,
    })))
}
