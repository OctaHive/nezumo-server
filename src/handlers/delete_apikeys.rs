//! API-key revocation handlers for authenticated key owners.

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument; // For logging
use uuid::Uuid;

use crate::database::apikeys::delete_apikey_from_db;
use crate::models::user::User;
use crate::routes::AppState;

// --- Route Handler ---

// Delete a API key by id
#[utoipa::path(
    delete,
    path = "/apikeys/{id}",
    tag = "apikey",
    security(
        ("jwt_token" = [])
    ),
    params(
        ("id" = String, Path, description = "API key ID")
    ),
    responses(
        (status = 200, description = "API key deleted successfully", body = String),
        (status = 400, description = "Invalid UUID format", body = String),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 404, description = "API key not found", body = String),
        (status = 500, description = "Internal server error", body = String)
    )
)]
#[instrument(skip(state))]
pub async fn delete_apikey_by_id(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Path(id): Path<String>, // Use Path extractor here
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    // Parse the id string to UUID
    let uuid = match Uuid::parse_str(&id) {
        Ok(uuid) => uuid,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid UUID format.") })),
            ));
        }
    };

    match delete_apikey_from_db(&state.database, uuid, user.id).await {
        Ok(rows_affected) => {
            if rows_affected == 0 {
                Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": format!("API key with ID '{}' not found.", id) })),
                ))
            } else {
                Ok((
                    StatusCode::OK,
                    Json(json!({ "success": format!("API key with ID '{}' deleted.", id) })),
                ))
            }
        }
        Err(_err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Could not delete API key '{}'.", id) })),
        )),
    }
}
