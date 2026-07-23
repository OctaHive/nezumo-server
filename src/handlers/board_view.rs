//! Per-user, per-board camera (pan + zoom) endpoints. `GET` returns the caller's
//! saved view for a board (or `null`); `POST` upserts it. Used by clients to
//! restore the user's last view when reopening a board.

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::database::board_view::{get_board_view, upsert_board_view};
use crate::models::user::User;
use crate::routes::AppState;

fn parse_board_id(id: &str) -> Result<Uuid, (StatusCode, Json<Value>)> {
    Uuid::parse_str(id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid board id." })),
        )
    })
}

/// `GET /boards/{id}/view` — the caller's saved `{x, y, zoom}` for this board,
/// or `null` if none. The view is per-user, so anonymous callers (e.g. an embed
/// iframe viewer) simply have no saved view → `null` instead of 401.
pub async fn get_board_view_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    user_ext: Option<Extension<User>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    let Some(Extension(current_user)) = user_ext else {
        return Ok(Json(Value::Null));
    };
    match get_board_view(&state.database, current_user.id, board_id).await {
        Ok(Some((x, y, zoom))) => Ok(Json(json!({ "x": x, "y": y, "zoom": zoom }))),
        Ok(None) => Ok(Json(Value::Null)),
        Err(e) => {
            tracing::error!("get_board_view failed: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not fetch view." })),
            ))
        }
    }
}

/// `POST /boards/{id}/view` with `{x, y, zoom}` — upsert the caller's view.
/// Per-user, so anonymous callers (e.g. an embed iframe viewer) have nothing to
/// save → silent no-op (204) instead of 401.
pub async fn put_board_view_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    user_ext: Option<Extension<User>>,
    Json(body): Json<Value>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    let board_id = parse_board_id(&id)?;
    let Some(Extension(current_user)) = user_ext else {
        return Ok(StatusCode::NO_CONTENT);
    };
    let x = body.get("x").and_then(|v| v.as_f64());
    let y = body.get("y").and_then(|v| v.as_f64());
    let zoom = body.get("zoom").and_then(|v| v.as_f64());
    let (Some(x), Some(y), Some(zoom)) = (x, y, zoom) else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Expected { x, y, zoom }." })),
        ));
    };
    match upsert_board_view(&state.database, current_user.id, board_id, x, y, zoom).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) => {
            tracing::error!("upsert_board_view failed: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not save view." })),
            ))
        }
    }
}
