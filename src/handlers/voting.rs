//! Miro-style board voting endpoints. Server-authoritative: sessions, the tally,
//! and participation are persisted (see [`crate::database::voting`]); the server
//! enforces the per-participant vote budget and the owner/admin gate on
//! start/end. Live updates ride the realtime **preview** channel as a compact
//! `{voting:{…}}` payload (LWW by `rev`, exactly like the board timer); the full
//! participant list + the caller's own state come from `GET` / POST responses.

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::database::board_members::get_member_role;
use crate::database::boards::get_board_by_id;
use crate::database::users::fetch_active_user_identities;
use crate::database::voting;
use crate::models::user::User;
use crate::realtime::{broadcast_ws, WsMessage};
use crate::routes::AppState;

fn err(code: StatusCode, msg: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "error": msg })))
}

fn parse_board_id(id: &str) -> Result<Uuid, (StatusCode, Json<Value>)> {
    Uuid::parse_str(id).map_err(|_| err(StatusCode::BAD_REQUEST, "Invalid board id."))
}

/// Owner/admin gate: site admins, the board creator, or a board member with the
/// `owner` role may start/end a voting session.
async fn is_organizer(state: &AppState, board_id: Uuid, user: &User) -> bool {
    if user.role_level >= 2 {
        return true;
    }
    match get_board_by_id(&state.database, board_id).await {
        Ok(Some(board)) if board.owner_id == user.id => return true,
        Ok(_) => {}
        Err(_) => return false,
    }
    matches!(
        get_member_role(&state.database, board_id, user.id).await,
        Ok(Some(role)) if role == "owner"
    )
}

/// Compact live payload broadcast on the preview channel — counts + aggregate
/// participation only (no per-user identities). This is the wire frame every
/// client applies under LWW.
async fn compact_state(
    pool: &sqlx::PgPool,
    s: &voting::VotingSessionRow,
) -> Result<Value, sqlx::Error> {
    let tally = voting::tally(pool, s.id).await?;
    let mut counts = Map::new();
    for (cid, n) in tally {
        counts.insert(cid, json!(n));
    }
    let parts = voting::participants(pool, s.id).await?;
    let joined: Vec<&voting::ParticipantRow> =
        parts.iter().filter(|p| p.status == "joined").collect();
    let joined_count = joined.len() as i64;
    let voted_count = joined
        .iter()
        .filter(|p| p.finished || p.votes_used >= s.votes_per_participant as i64)
        .count() as i64;
    let candidate_ids: Value = serde_json::from_str(&s.candidate_ids).unwrap_or_else(|_| json!([]));

    Ok(json!({
        "sessionId": s.id,
        "boardId": s.board_id,
        "startedBy": s.created_by,
        "status": s.status,
        "endsAt": s.ends_at.timestamp_millis(),
        "rev": s.rev,
        "anonymous": s.anonymous,
        "votesPerParticipant": s.votes_per_participant,
        "candidateIds": candidate_ids,
        "counts": counts,
        "joinedCount": joined_count,
        "votedCount": voted_count,
    }))
}

/// Full payload for `GET` / POST responses: the compact frame plus the resolved
/// participant list (names + avatars + completion), the caller's own status, and
/// — only when the session is NOT anonymous — per-candidate voter attribution.
async fn full_state(
    state: &AppState,
    s: &voting::VotingSessionRow,
    me: Option<Uuid>,
) -> Result<Value, sqlx::Error> {
    let mut base = compact_state(&state.database, s).await?;
    let parts = voting::participants(&state.database, s.id).await?;

    // Resolve identities in one batched query, then presign avatars.
    let ids: Vec<Uuid> = parts.iter().map(|p| p.user_id).collect();
    let identities = fetch_active_user_identities(&state.database, &ids)
        .await
        .unwrap_or_default();
    let mut id_map: std::collections::HashMap<Uuid, (String, Option<String>)> =
        std::collections::HashMap::new();
    for u in identities {
        let display_name = if u.first_name.is_some() || u.last_name.is_some() {
            [u.first_name.as_deref(), u.last_name.as_deref()]
                .iter()
                .filter_map(|s| *s)
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            u.username.clone()
        };
        let avatar = match &u.profile_picture_url {
            Some(stored) => {
                crate::storage::presign_url::presign_stored_url(&state.storage, stored).await
            }
            None => None,
        };
        id_map.insert(u.id, (display_name, avatar));
    }

    let participants_json: Vec<Value> = parts
        .iter()
        .map(|p| {
            let (name, avatar) = id_map
                .get(&p.user_id)
                .cloned()
                .unwrap_or_else(|| ("Аноним".to_string(), None));
            json!({
                "userId": p.user_id,
                "name": name,
                "avatar": avatar,
                "status": p.status,
                "votesUsed": p.votes_used,
                "done": p.status == "joined"
                    && (p.finished || p.votes_used >= s.votes_per_participant as i64),
            })
        })
        .collect();

    let obj = base.as_object_mut().unwrap();
    obj.insert("participants".to_string(), json!(participants_json));

    if let Some(uid) = me {
        let mine = parts.iter().find(|p| p.user_id == uid);
        obj.insert(
            "myStatus".to_string(),
            json!(mine.map(|p| p.status.clone())),
        );
        obj.insert(
            "myVotesUsed".to_string(),
            json!(mine.map(|p| p.votes_used).unwrap_or(0)),
        );
        obj.insert(
            "myFinished".to_string(),
            json!(mine.map(|p| p.finished).unwrap_or(false)),
        );
    }

    // Per-candidate attribution only when not anonymous.
    if !s.anonymous {
        let rows = voting::voters(&state.database, s.id)
            .await
            .unwrap_or_default();
        let mut voters: Map<String, Value> = Map::new();
        for (cid, voter) in rows {
            voters
                .entry(cid)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .push(json!(voter));
        }
        obj.insert("voters".to_string(), json!(voters));
    }

    Ok(base)
}

/// Broadcast the compact live state on the board's preview channel.
async fn broadcast(state: &AppState, s: &voting::VotingSessionRow, actor: Uuid) {
    match compact_state(&state.database, s).await {
        Ok(voting_payload) => {
            let sender = state.realtime.preview_sender_for(s.board_id).await;
            broadcast_ws(
                &sender,
                WsMessage::Preview {
                    board_id: s.board_id,
                    user_id: actor,
                    payload: json!({ "voting": voting_payload }),
                },
            );
        }
        Err(e) => tracing::warn!("voting broadcast build failed: {e}"),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartBody {
    #[serde(default)]
    pub candidate_ids: Vec<String>,
    #[serde(default = "default_votes")]
    pub votes_per_participant: i32,
    #[serde(default = "default_minutes")]
    pub minutes: i32,
    #[serde(default = "default_anon")]
    pub anonymous: bool,
}
fn default_votes() -> i32 {
    3
}
fn default_minutes() -> i32 {
    2
}
fn default_anon() -> bool {
    true
}

/// `POST /boards/{id}/voting` — start a session (owner/admin only).
pub async fn start_voting_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<StartBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    if !is_organizer(&state, board_id, &current_user).await {
        return Err(err(
            StatusCode::FORBIDDEN,
            "Only the board owner can start a voting session.",
        ));
    }
    // A ranking vote needs at least two candidates to compare.
    if body.candidate_ids.len() < 2 {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "Select at least two objects to start voting.",
        ));
    }
    let votes = body.votes_per_participant.clamp(1, 50);
    let minutes = body.minutes.clamp(1, 120);
    let candidate_ids = serde_json::to_string(&body.candidate_ids).unwrap_or_else(|_| "[]".into());

    let session = voting::create_session(
        &state.database,
        board_id,
        current_user.id,
        &candidate_ids,
        votes,
        body.anonymous,
        minutes,
    )
    .await
    .map_err(|e| {
        tracing::error!("create_session failed: {e}");
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not start session.",
        )
    })?;

    broadcast(&state, &session, current_user.id).await;
    let state_json = full_state(&state, &session, Some(current_user.id))
        .await
        .map_err(|e| {
            tracing::error!("full_state failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "Could not build state.")
        })?;
    Ok(Json(state_json))
}

#[derive(Deserialize)]
pub struct ParticipationBody {
    pub status: String,
}

/// `POST /boards/{id}/voting/participation` — join or decline the active session.
pub async fn set_participation_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<ParticipationBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    if body.status != "joined" && body.status != "declined" {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "Invalid participation status.",
        ));
    }
    let session = voting::active_session(&state.database, board_id)
        .await
        .map_err(|e| {
            tracing::error!("active_session failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "Could not load session.")
        })?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "No active voting session."))?;

    voting::set_participation(&state.database, session.id, current_user.id, &body.status)
        .await
        .map_err(|e| {
            tracing::error!("set_participation failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "Could not join.")
        })?;
    let rev = voting::bump_rev(&state.database, session.id)
        .await
        .unwrap_or(session.rev);
    let session = voting::VotingSessionRow { rev, ..session };

    broadcast(&state, &session, current_user.id).await;
    let state_json = full_state(&state, &session, Some(current_user.id))
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not build state."))?;
    Ok(Json(state_json))
}

#[derive(Deserialize)]
pub struct VoteBody {
    pub candidate: String,
}

/// `POST /boards/{id}/voting/vote` — cast one vote on a candidate.
pub async fn cast_vote_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
    Json(body): Json<VoteBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    let session = voting::active_session(&state.database, board_id)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not load session."))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "No active voting session."))?;

    if session.ends_at <= chrono::Utc::now() {
        return Err(err(StatusCode::CONFLICT, "Voting has ended."));
    }
    // Must be a joined participant.
    if !voting::is_joined(&state.database, session.id, current_user.id)
        .await
        .unwrap_or(false)
    {
        return Err(err(
            StatusCode::FORBIDDEN,
            "Join the session before voting.",
        ));
    }
    // Candidate must belong to the session.
    let candidates: Vec<String> = serde_json::from_str(&session.candidate_ids).unwrap_or_default();
    if !candidates.contains(&body.candidate) {
        return Err(err(StatusCode::BAD_REQUEST, "Not a voting candidate."));
    }
    // Enforce the per-participant budget.
    let used = voting::voter_used(&state.database, session.id, current_user.id)
        .await
        .unwrap_or(0);
    if used >= session.votes_per_participant as i64 {
        return Err(err(StatusCode::CONFLICT, "No votes left."));
    }

    voting::insert_vote(
        &state.database,
        session.id,
        current_user.id,
        &body.candidate,
    )
    .await
    .map_err(|e| {
        tracing::error!("insert_vote failed: {e}");
        err(StatusCode::INTERNAL_SERVER_ERROR, "Could not record vote.")
    })?;
    let rev = voting::bump_rev(&state.database, session.id)
        .await
        .unwrap_or(session.rev);
    let session = voting::VotingSessionRow { rev, ..session };

    broadcast(&state, &session, current_user.id).await;
    let state_json = full_state(&state, &session, Some(current_user.id))
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not build state."))?;
    Ok(Json(state_json))
}

/// `POST /boards/{id}/voting/finish` — the caller marks themselves as done
/// voting without spending every vote. Must be a joined participant.
pub async fn finish_voting_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    let session = voting::active_session(&state.database, board_id)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not load session."))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "No active voting session."))?;

    if !voting::is_joined(&state.database, session.id, current_user.id)
        .await
        .unwrap_or(false)
    {
        return Err(err(
            StatusCode::FORBIDDEN,
            "Join the session before finishing.",
        ));
    }
    voting::set_finished(&state.database, session.id, current_user.id)
        .await
        .map_err(|e| {
            tracing::error!("set_finished failed: {e}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not finish voting.",
            )
        })?;
    let rev = voting::bump_rev(&state.database, session.id)
        .await
        .unwrap_or(session.rev);
    let session = voting::VotingSessionRow { rev, ..session };

    broadcast(&state, &session, current_user.id).await;
    let state_json = full_state(&state, &session, Some(current_user.id))
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not build state."))?;
    Ok(Json(state_json))
}

/// `POST /boards/{id}/voting/end` — end the session (owner/admin only).
pub async fn end_voting_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    if !is_organizer(&state, board_id, &current_user).await {
        return Err(err(
            StatusCode::FORBIDDEN,
            "Only the board owner can end a voting session.",
        ));
    }
    let session = voting::active_session(&state.database, board_id)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not load session."))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "No active voting session."))?;

    let ended = voting::end_session(&state.database, session.id)
        .await
        .map_err(|e| {
            tracing::error!("end_session failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "Could not end session.")
        })?
        .unwrap_or(session);

    broadcast(&state, &ended, current_user.id).await;
    let state_json = full_state(&state, &ended, Some(current_user.id))
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not build state."))?;
    Ok(Json(state_json))
}

/// `GET /boards/{id}/voting` — the board's latest session (active or ended
/// results), or `null`. Anonymous callers get the state without `my*` fields.
pub async fn get_voting_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    user_ext: Option<Extension<User>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    let session = voting::latest_session(&state.database, board_id)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not load session."))?;
    let Some(session) = session else {
        return Ok(Json(Value::Null));
    };
    let me = user_ext.as_ref().map(|Extension(u)| u.id);
    let state_json = full_state(&state, &session, me)
        .await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "Could not build state."))?;
    Ok(Json(state_json))
}
