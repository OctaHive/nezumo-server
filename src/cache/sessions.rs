//! Ephemeral board-presence state stored in Redis.
//!
//! A board owns a set of active `{user_id}:{session_id}` members. Separate
//! expiring keys hold heartbeats, cursor coordinates, and anonymous display
//! names. The realtime handler refreshes these values while a client is
//! connected, and the session-cleanup job removes members whose heartbeat has
//! expired.
//!
//! Redis is not the durable source of truth for users or boards: every value in
//! this module may disappear through expiry or cache loss and must be treated as
//! reconstructible presence data.

use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;
use serde::Serialize;
use std::collections::HashSet;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Serialize, ToSchema)]
/// Parsed representation of a member in a board's Redis session set.
pub struct SessionInfo {
    /// User or anonymous presence identifier.
    pub user_id: String,
    /// Identifier of one client connection belonging to the user.
    pub session_id: String,
}

/// Redis set key containing every active session for a board.
fn session_key(board_id: &str) -> String {
    format!("board:{}:sessions", board_id)
}

/// Encode a session-set member.
///
/// Callers must keep `user_id` free of `:` because readers split the value on
/// the first colon. The session id may contain colons because it is the final
/// component.
fn session_value(user_id: &str, session_id: &str) -> String {
    format!("{}:{}", user_id, session_id)
}

/// Safety-net lifetime for a board's complete session set.
///
/// Adding any session refreshes this TTL. Per-session liveness is determined by
/// the much shorter heartbeat TTL below.
const SESSION_TTL_SECS: i64 = 86400; // 24 hours

/// Add one client session to a board and refresh the board session-set TTL.
///
/// Redis sets make repeated calls for the same `(user_id, session_id)`
/// idempotent. This function does not create the heartbeat; callers should also
/// call [`touch_session_heartbeat`] when a connection is established.
pub async fn add_session(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
    session_id: &str,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = session_key(board_id);
    let value = session_value(user_id, session_id);

    let _: () = conn
        .sadd(&key, &value)
        .await
        .map_err(|e| format!("Failed to SADD session: {e}"))?;

    let _: () = conn
        .expire(&key, SESSION_TTL_SECS)
        .await
        .map_err(|e| format!("Failed to set TTL on sessions key: {e}"))?;

    Ok(())
}

/// Remove one client session from a board's session set.
///
/// Removing a missing member succeeds. Associated heartbeat, cursor, and
/// anonymous-name keys are managed separately by the disconnect flow or stale
/// session cleanup.
pub async fn remove_session(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
    session_id: &str,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = session_key(board_id);
    let value = session_value(user_id, session_id);

    let _: () = conn
        .srem(&key, &value)
        .await
        .map_err(|e| format!("Failed to SREM session: {e}"))?;

    Ok(())
}

/// Return all parseable sessions currently recorded for a board.
///
/// Malformed set members that do not contain the `user_id:session_id`
/// delimiter are ignored.
pub async fn get_sessions(pool: &Pool, board_id: &str) -> Result<Vec<SessionInfo>, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = session_key(board_id);
    let members: Vec<String> = conn
        .smembers(&key)
        .await
        .map_err(|e| format!("Failed to SMEMBERS sessions: {e}"))?;

    let sessions = members
        .into_iter()
        .filter_map(|m| {
            let mut parts = m.splitn(2, ':');
            let user_id = parts.next()?.to_string();
            let session_id = parts.next()?.to_string();
            Some(SessionInfo {
                user_id,
                session_id,
            })
        })
        .collect();

    Ok(sessions)
}

/// Redis key for a user's latest cursor position on a board.
fn cursor_key(board_id: &str, user_id: &str) -> String {
    format!("board:{}:cursor:{}", board_id, user_id)
}

/// Cursor positions expire automatically when updates stop.
const CURSOR_TTL_SECS: i64 = 300;

/// Store a user's latest board-space cursor coordinates for five minutes.
///
/// Each update replaces the previous value and refreshes its TTL.
pub async fn set_cursor_position(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
    x: f64,
    y: f64,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = cursor_key(board_id, user_id);
    let value = format!("{},{}", x, y);

    let _: () = conn
        .set_ex(&key, &value, CURSOR_TTL_SECS as u64)
        .await
        .map_err(|e| format!("Failed to SET cursor: {e}"))?;

    Ok(())
}

/// Fetch the latest cursor coordinates for the requested users.
///
/// Missing, expired, or malformed values are omitted from the returned map.
/// This currently performs one Redis `GET` per user on a shared connection.
pub async fn get_cursor_positions(
    pool: &Pool,
    board_id: &str,
    user_ids: &[String],
) -> Result<std::collections::HashMap<String, (f64, f64)>, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let mut result = std::collections::HashMap::new();
    for uid in user_ids {
        let key = cursor_key(board_id, uid);
        let val: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| format!("Failed to GET cursor: {e}"))?;
        if let Some(val) = val {
            let mut parts = val.splitn(2, ',');
            if let (Some(x_str), Some(y_str)) = (parts.next(), parts.next()) {
                if let (Ok(x), Ok(y)) = (x_str.parse::<f64>(), y_str.parse::<f64>()) {
                    result.insert(uid.clone(), (x, y));
                }
            }
        }
    }

    Ok(result)
}

/// Delete a user's cached cursor position for a board.
///
/// Deleting a missing key succeeds.
pub async fn remove_cursor(pool: &Pool, board_id: &str, user_id: &str) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = cursor_key(board_id, user_id);
    let _: () = conn
        .del(&key)
        .await
        .map_err(|e| format!("Failed to DEL cursor: {e}"))?;

    Ok(())
}

/// Redis key for an anonymous user's board-scoped display name.
fn anon_name_key(board_id: &str, user_id: &str) -> String {
    format!("board:{}:anon_name:{}", board_id, user_id)
}

/// Store an anonymous user's display name with the session-set lifetime.
///
/// The name is board-scoped so the same anonymous identifier can be represented
/// independently on different boards.
pub async fn set_anon_display_name(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
    display_name: &str,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = anon_name_key(board_id, user_id);
    let _: () = conn
        .set_ex(&key, display_name, SESSION_TTL_SECS as u64)
        .await
        .map_err(|e| format!("Failed to SET anon name: {e}"))?;

    Ok(())
}

/// Return an anonymous user's display name, or `None` when it is absent or has
/// expired.
pub async fn get_anon_display_name(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
) -> Result<Option<String>, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = anon_name_key(board_id, user_id);
    let val: Option<String> = conn
        .get(&key)
        .await
        .map_err(|e| format!("Failed to GET anon name: {e}"))?;

    Ok(val)
}

/// Delete an anonymous user's board-scoped display name.
///
/// Deleting a missing key succeeds.
pub async fn remove_anon_display_name(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = anon_name_key(board_id, user_id);
    let _: () = conn
        .del(&key)
        .await
        .map_err(|e| format!("Failed to DEL anon name: {e}"))?;

    Ok(())
}

/// Return the raw `{user_id}:{session_id}` members of a board's session set.
///
/// Prefer [`get_sessions`] when parsed values are needed. The raw form is used
/// by snapshot retention and presence aggregation paths.
pub async fn get_session_values(pool: &Pool, board_id: &str) -> Result<Vec<String>, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = session_key(board_id);
    let members: Vec<String> = conn
        .smembers(&key)
        .await
        .map_err(|e| format!("Failed to SMEMBERS sessions: {e}"))?;

    Ok(members)
}

// --- Session heartbeat for stale session cleanup ---

/// Maximum time a session remains live without a heartbeat refresh.
const HEARTBEAT_TTL_SECS: u64 = 60;

/// Redis liveness-marker key for one concrete client session.
fn heartbeat_key(board_id: &str, user_id: &str, session_id: &str) -> String {
    format!("board:{}:heartbeat:{}:{}", board_id, user_id, session_id)
}

/// Create or refresh a session heartbeat with a 60-second TTL.
///
/// Called when a client connects and on each realtime heartbeat tick. The
/// marker value itself is irrelevant; key existence represents liveness.
pub async fn touch_session_heartbeat(
    pool: &Pool,
    board_id: &str,
    user_id: &str,
    session_id: &str,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = heartbeat_key(board_id, user_id, session_id);
    let _: () = conn
        .set_ex(&key, "1", HEARTBEAT_TTL_SECS)
        .await
        .map_err(|e| format!("Failed to SET heartbeat: {e}"))?;

    Ok(())
}

/// Remove board sessions whose heartbeat key no longer exists.
///
/// Cursor and anonymous-name keys for each stale user are removed on a
/// best-effort basis. The returned strings retain the raw
/// `{user_id}:{session_id}` representation so callers can log or reconcile the
/// removed connections. Malformed session members are left untouched.
pub async fn cleanup_stale_sessions(pool: &Pool, board_id: &str) -> Result<Vec<String>, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let key = session_key(board_id);
    let members: Vec<String> = conn
        .smembers(&key)
        .await
        .map_err(|e| format!("Failed to SMEMBERS sessions: {e}"))?;

    let mut removed = Vec::new();
    for member in &members {
        let mut parts = member.splitn(2, ':');
        let (Some(uid), Some(sid)) = (parts.next(), parts.next()) else {
            continue;
        };
        let hb_key = heartbeat_key(board_id, uid, sid);
        let exists: bool = conn
            .exists(&hb_key)
            .await
            .map_err(|e| format!("Failed to EXISTS heartbeat: {e}"))?;
        if !exists {
            let _: () = conn
                .srem(&key, member)
                .await
                .map_err(|e| format!("Failed to SREM stale session: {e}"))?;
            // Also clean up cursor and anon name for this user
            let _ = conn.del::<_, ()>(&cursor_key(board_id, uid)).await;
            let _ = conn.del::<_, ()>(&anon_name_key(board_id, uid)).await;
            removed.push(member.clone());
        }
    }

    Ok(removed)
}

/// Return board IDs for which Redis currently has a session-set key.
///
/// Uses incremental `SCAN` rather than blocking Redis with `KEYS`. The result is
/// a point-in-time view: keys may be added or expire while scanning, and Redis
/// does not guarantee ordering.
pub async fn get_boards_with_sessions(pool: &Pool) -> Result<Vec<String>, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let pattern = "board:*:sessions";
    let mut board_ids = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = deadpool_redis::redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut *conn)
            .await
            .map_err(|e| format!("Failed to SCAN sessions keys: {e}"))?;

        for key in keys {
            // key format: "board:{board_id}:sessions"
            if let Some(bid) = key
                .strip_prefix("board:")
                .and_then(|s| s.strip_suffix(":sessions"))
            {
                board_ids.push(bid.to_string());
            }
        }

        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(board_ids)
}

/// Return registered user IDs that currently have a recorded board session.
///
/// Session members are unioned across all discovered boards and deduplicated.
/// The stale-session cleanup job keeps the sets close to live presence by
/// removing members after their 60-second heartbeat expires. Anonymous and
/// embed identifiers that are not valid UUIDs are intentionally ignored.
pub async fn get_all_online_user_ids(pool: &Pool) -> Result<HashSet<Uuid>, String> {
    let board_ids = get_boards_with_sessions(pool).await?;
    let mut online: HashSet<Uuid> = HashSet::new();
    for board_id in board_ids {
        let members = get_session_values(pool, &board_id).await?;
        for member in members {
            // member format: "{user_id}:{session_id}"
            if let Some(uid) = member.split(':').next() {
                if let Ok(id) = Uuid::parse_str(uid) {
                    online.insert(id);
                }
            }
        }
    }
    Ok(online)
}
