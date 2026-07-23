//! Periodic Redis session cleanup and websocket departure notifications.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::cache::sessions;
use crate::realtime::{broadcast_ws, WsMessage};
use crate::routes::AppState;

/// Start a background task that periodically scans Redis for stale sessions
/// (heartbeat key expired, i.e. no update in 60s) and removes them.
pub fn start_session_cleanup_job(state: Arc<AppState>, interval_secs: u64) {
    let tick_interval = Duration::from_secs(interval_secs.max(5));
    tokio::spawn(async move {
        let mut ticker = interval(tick_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(err) = run_cleanup_cycle(&state).await {
                warn!("session cleanup job failed: {}", err);
            }
        }
    });
}

async fn run_cleanup_cycle(state: &Arc<AppState>) -> Result<(), String> {
    let board_ids = sessions::get_boards_with_sessions(&state.cache).await?;

    for board_id_str in board_ids {
        let removed = sessions::cleanup_stale_sessions(&state.cache, &board_id_str).await?;
        if removed.is_empty() {
            continue;
        }

        info!(
            "cleaned up {} stale session(s) from board {}: {:?}",
            removed.len(),
            board_id_str,
            removed
        );

        // Broadcast updated sessions list so other clients see the change.
        // Presence is droppable and periodically reconciled by clients, so keep
        // it off the durable commit ring.
        let board_id = match Uuid::parse_str(&board_id_str) {
            Ok(id) => id,
            Err(_) => continue,
        };

        if let Some(users) = build_sessions_users(state, &board_id_str).await {
            let sender = state.realtime.preview_sender_for(board_id).await;
            broadcast_ws(&sender, WsMessage::SessionsUpdate { board_id, users });
        }
    }

    Ok(())
}

/// Duplicate of the helper from realtime handler — builds enriched user list for sessions_update.
async fn build_sessions_users(
    state: &AppState,
    board_id_str: &str,
) -> Option<Vec<serde_json::Value>> {
    use crate::database::users::fetch_active_user_by_id_from_db;
    use serde_json::json;

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
    let mut users = Vec::new();
    for uid_str in &unique_user_ids {
        let uid = Uuid::parse_str(uid_str).ok()?;
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
            let avatar = match &user.profile_picture_url {
                Some(stored) => {
                    crate::storage::presign_url::presign_stored_url(&state.storage, stored).await
                }
                None => None,
            };
            users.push(json!({
                "user_id": user.id,
                "username": user.username,
                "display_name": display_name,
                "profile_picture_url": avatar,
                "cursor_x": cursor_x,
                "cursor_y": cursor_y,
            }));
        } else {
            let anon_name = sessions::get_anon_display_name(&state.cache, board_id_str, uid_str)
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| "Аноним".to_string());
            users.push(json!({
                "user_id": uid_str,
                "username": &anon_name,
                "display_name": &anon_name,
                "profile_picture_url": null,
                "cursor_x": cursor_x,
                "cursor_y": cursor_y,
            }));
        }
    }
    Some(users)
}
