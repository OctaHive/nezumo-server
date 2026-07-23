//! Per-board embed-token endpoints + shared access helpers. An embed token is an
//! unguessable secret that grants anonymous, VIEW-ONLY access to a board so it
//! can be dropped into a third-party `<iframe>` (including private boards, where
//! the user's session cookie is never sent cross-site).
//!
//! Management endpoints (`/boards/{id}/embed`, owner-only):
//!   - `POST`   → return the board's current token, creating one if absent.
//!   - `GET`    → the current token (or `null`).
//!   - `DELETE` → revoke all tokens (disables / rotates embedding).
//!
//! Read access: REST handlers pass the secret via the `X-Embed-Token` header,
//! the realtime socket via the `embed_token` query param. [`embed_grants_view`]
//! validates it and, when valid, callers treat the request as an anon viewer.

use axum::{
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::database::board_embed_tokens::{
    create_embed_token, delete_embed_tokens, get_valid_embed_token, latest_embed_token,
};
use crate::database::board_members::get_member_role;
use crate::database::boards::get_board_by_id;
use crate::models::user::User;
use crate::routes::AppState;

/// Pull an embed token out of the `X-Embed-Token` request header (REST calls).
pub fn embed_token_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-embed-token")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Whether `token` is a valid, non-expired embed token for `board_id`. A valid
/// token grants view-only access regardless of the board's visibility/membership.
pub async fn embed_grants_view(db: &sqlx::PgPool, board_id: Uuid, token: Option<&str>) -> bool {
    let Some(token) = token else { return false };
    matches!(
        get_valid_embed_token(db, token).await,
        Ok(Some(token_board_id)) if token_board_id == board_id
    )
}

fn parse_board_id(id: &str) -> Result<Uuid, (StatusCode, Json<Value>)> {
    Uuid::parse_str(id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid board id." })),
        )
    })
}

/// Only the board owner (or an admin) may manage embed tokens.
async fn require_owner(
    state: &AppState,
    board_id: Uuid,
    user: &User,
) -> Result<(), (StatusCode, Json<Value>)> {
    if user.role_level >= 2 {
        return Ok(());
    }
    let board = get_board_by_id(&state.database, board_id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch board." })),
            )
        })?;
    let is_owner = match board {
        Some(b) => {
            b.owner_id == user.id
                || get_member_role(&state.database, board_id, user.id)
                    .await
                    .unwrap_or(None)
                    .as_deref()
                    == Some("owner")
        }
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Board not found." })),
            ))
        }
    };
    if is_owner {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Only the owner can manage embedding." })),
        ))
    }
}

fn gen_token() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(48)
        .map(char::from)
        .collect()
}

/// `POST /boards/{id}/embed` — return the board's embed token, creating one if it
/// doesn't have a (non-expired) one yet. Idempotent: repeated calls reuse it.
pub async fn create_embed_token_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    require_owner(&state, board_id, &current_user).await?;

    if let Ok(Some(existing)) = latest_embed_token(&state.database, board_id).await {
        return Ok(Json(json!({ "token": existing })));
    }

    let token = gen_token();
    create_embed_token(&state.database, board_id, &token, current_user.id, None)
        .await
        .map_err(|e| {
            tracing::error!("create_embed_token failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not create embed token." })),
            )
        })?;
    Ok(Json(json!({ "token": token })))
}

/// `GET /boards/{id}/embed` — the board's current embed token, or `null`.
pub async fn get_embed_token_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    require_owner(&state, board_id, &current_user).await?;
    match latest_embed_token(&state.database, board_id).await {
        Ok(Some(token)) => Ok(Json(json!({ "token": token }))),
        Ok(None) => Ok(Json(json!({ "token": Value::Null }))),
        Err(e) => {
            tracing::error!("get_embed_token failed: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch embed token." })),
            ))
        }
    }
}

/// `DELETE /boards/{id}/embed` — revoke all embed tokens (disable embedding).
pub async fn delete_embed_token_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    require_owner(&state, board_id, &current_user).await?;
    delete_embed_tokens(&state.database, board_id)
        .await
        .map_err(|e| {
            tracing::error!("delete_embed_tokens failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not revoke embed tokens." })),
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}
