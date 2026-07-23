//! Administrative tier catalogue and HTTP mapping for quota failures.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::Serialize;
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::database::quotas::{
    list_tiers, tier_for_user, usage_for_owner, QuotaError, QuotaUsage, TierQuota,
};
use crate::models::user::User;
use crate::routes::AppState;

pub type QuotaApiError = (StatusCode, Json<Value>);

pub fn quota_api_error(error: QuotaError) -> QuotaApiError {
    match error {
        QuotaError::BoardLimit { used, limit } => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "Board limit reached for this tier.",
                "code": "tier_board_limit",
                "used": used,
                "limit": limit,
            })),
        ),
        QuotaError::UploadTooLarge { size, limit } => (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({
                "error": "File exceeds the maximum upload size for this tier.",
                "code": "tier_upload_limit",
                "size": size,
                "limit": limit,
            })),
        ),
        QuotaError::StorageLimit { used, size, limit } => (
            StatusCode::INSUFFICIENT_STORAGE,
            Json(json!({
                "error": "Storage quota exceeded for this tier.",
                "code": "tier_storage_limit",
                "used": used,
                "size": size,
                "limit": limit,
            })),
        ),
        QuotaError::TierNotFound => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "The user's tier is not configured.",
                "code": "tier_not_configured",
            })),
        ),
        QuotaError::Database(error) => {
            tracing::error!("quota database error: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Could not validate resource quota." })),
            )
        }
    }
}

#[utoipa::path(
    get,
    path = "/users/tiers",
    tag = "user",
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Configured resource tiers", body = [TierQuota]),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn get_tiers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<TierQuota>>, QuotaApiError> {
    list_tiers(&state.database)
        .await
        .map(Json)
        .map_err(|error| quota_api_error(QuotaError::Database(error)))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CurrentQuotaResponse {
    pub tier: TierQuota,
    pub usage: QuotaUsage,
}

#[utoipa::path(
    get,
    path = "/users/current/quota",
    tag = "user",
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Current user's resource tier and usage", body = CurrentQuotaResponse),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn get_current_quota(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> Result<Json<CurrentQuotaResponse>, QuotaApiError> {
    let tier = tier_for_user(&state.database, current_user.id)
        .await
        .map_err(quota_api_error)?;
    let usage = usage_for_owner(&state.database, current_user.id)
        .await
        .map_err(|error| quota_api_error(QuotaError::Database(error)))?;

    Ok(Json(CurrentQuotaResponse { tier, usage }))
}
