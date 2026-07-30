//! User-account deletion and associated resource cleanup handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, instrument};
use uuid::Uuid;

use crate::core::config::get_env_with_default;
use crate::database::users::delete_user_from_db;
use crate::models::documentation::{ErrorResponse, SuccessResponse};
use crate::routes::AppState;
use crate::storage::delete::{delete_from_storage, object_key_from_stored_url};

// --- Route Handler ---

// Delete a user by id
#[utoipa::path(
    delete,
    path = "/users/{id}",
    tag = "user",
    security(
        ("jwt_token" = [])
    ),
    responses(
        (status = 200, description = "User deleted successfully", body = SuccessResponse),
        (status = 400, description = "Invalid UUID format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 404, description = "User not found", body = ErrorResponse),
        (status = 500, description = "Internal Server Error", body = ErrorResponse)
    ),
    params(
        ("id" = Uuid, Path, description = "User ID")
    )
)]
#[instrument(skip(state))]
pub async fn delete_user_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>, // Use Path extractor here
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let uuid = match Uuid::parse_str(&id) {
        Ok(uuid) => uuid,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid UUID format." })),
            ));
        }
    };

    let mut transaction = state.database.begin().await.map_err(|err| {
        error!(user_id = %uuid, error = %err, "Could not start user deletion transaction");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete the user." })),
        )
    })?;

    let profile_picture_url = delete_user_from_db(&mut transaction, uuid)
        .await
        .map_err(|err| {
            error!(user_id = %uuid, error = %err, "Could not delete user from database");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not delete the user." })),
            )
        })?;

    let Some(profile_picture_url) = profile_picture_url else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("User with ID '{}' not found.", id) })),
        ));
    };

    if let Some(stored_url) = profile_picture_url {
        let bucket = get_env_with_default("STORAGE_BUCKET_PROFILE_PICTURES", "profile_pictures");
        if let Some(object_key) =
            object_key_from_stored_url(&state.storage.endpoint_url, &bucket, &stored_url)
        {
            delete_from_storage(&state.storage, &bucket, &object_key)
                .await
                .map_err(|err| {
                    error!(
                        user_id = %uuid,
                        object_key = %object_key,
                        error = %err,
                        "Could not delete user avatar from storage"
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "Could not delete the user's profile picture." })),
                    )
                })?;
        }
    }

    transaction.commit().await.map_err(|err| {
        error!(user_id = %uuid, error = %err, "Could not commit user deletion");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not delete the user." })),
        )
    })?;

    Ok((
        StatusCode::OK,
        Json(json!({ "success": format!("User with ID '{}' deleted.", id) })),
    ))
}
