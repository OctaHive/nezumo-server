//! Authenticated user preference query and update handlers.

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;

use crate::database::users::{fetch_color_preferences, update_color_preferences};
use crate::models::error::ErrorResponse;
use crate::models::user::{ColorPreferences, User};
use crate::routes::AppState;

// Hard limits — the client caps at 5 recent / 18 custom, but never trust it.
const MAX_RECENT: usize = 5;
const MAX_CUSTOM: usize = 12;
const REJECT_ARRAY_LEN: usize = 64;
const MAX_SERIALIZED_BYTES: usize = 2048;

fn is_hex_color(s: &str) -> bool {
    s.len() == 7 && s.as_bytes()[0] == b'#' && s[1..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// GET /users/current/preferences — the caller's saved color preferences.
/// Returns a default (empty) shape when never set or the stored blob is
/// unparseable, so the client never has to handle an error for missing data.
#[utoipa::path(
    get,
    path = "/users/current/preferences",
    tag = "user",
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Color preferences", body = ColorPreferences),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    ),
)]
#[instrument(skip(state, current_user))]
pub async fn get_color_preferences(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> Result<Json<ColorPreferences>, (StatusCode, Json<serde_json::Value>)> {
    match fetch_color_preferences(&state.database, current_user.id).await {
        Ok(Some(raw)) => {
            let prefs = serde_json::from_str::<ColorPreferences>(&raw).unwrap_or_default();
            Ok(Json(prefs))
        }
        Ok(None) => Ok(Json(ColorPreferences::default())),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Database error: {}", e) })),
        )),
    }
}

/// PATCH /users/current/preferences — replace the caller's color preferences.
/// Sanitizes server-side: rejects absurdly large arrays, drops non-`#RRGGBB`
/// entries, and caps recent/custom counts.
#[utoipa::path(
    patch,
    path = "/users/current/preferences",
    tag = "user",
    security(("jwt_token" = [])),
    request_body = ColorPreferences,
    responses(
        (status = 200, description = "Preferences saved"),
        (status = 400, description = "Payload too large", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    ),
)]
#[instrument(skip(state, current_user, body))]
pub async fn update_color_preferences_handler(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(mut body): Json<ColorPreferences>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    if body.recent.len() > REJECT_ARRAY_LEN || body.custom.len() > REJECT_ARRAY_LEN {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Too many colors" })),
        ));
    }

    let sanitize = |colors: &mut Vec<String>, max: usize| {
        colors.retain(|c| is_hex_color(c));
        for c in colors.iter_mut() {
            c.make_ascii_uppercase();
        }
        colors.truncate(max);
    };
    sanitize(&mut body.recent, MAX_RECENT);
    sanitize(&mut body.custom, MAX_CUSTOM);

    let serialized = serde_json::to_string(&body).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Serialization error: {}", e) })),
        )
    })?;
    if serialized.len() > MAX_SERIALIZED_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Payload too large" })),
        ));
    }

    match update_color_preferences(&state.database, current_user.id, &serialized).await {
        Ok(()) => Ok(Json(json!({ "success": true }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Database error: {}", e) })),
        )),
    }
}
