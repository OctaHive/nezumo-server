//! Administrator-managed, non-secret runtime server configuration.

use std::sync::Arc;

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

use crate::core::config::get_env_with_default;
use crate::database::{
    quotas::{list_tiers, TierQuota},
    server_settings::{self, ServerSettings, ServerSettingsPatch, TierSettingsUpdate},
};
use crate::models::user::User;
use crate::routes::AppState;

type ApiError = (StatusCode, Json<serde_json::Value>);

#[derive(Debug, Serialize, ToSchema)]
pub struct PublicServerSettings {
    pub public_registration_enabled: bool,
    pub google_login_enabled: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct IntegrationStatus {
    pub google: bool,
    pub mail: bool,
    pub github_issues: bool,
    pub storage: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminServerSettingsResponse {
    pub settings: ServerSettings,
    pub tiers: Vec<TierQuota>,
    pub integrations: IntegrationStatus,
}

fn configured(keys: &[&str]) -> bool {
    keys.iter()
        .all(|key| !get_env_with_default(key, "").trim().is_empty())
}

fn integration_status() -> IntegrationStatus {
    IntegrationStatus {
        google: configured(&[
            "GOOGLE_CLIENT_ID",
            "GOOGLE_CLIENT_SECRET",
            "GOOGLE_REDIRECT_URL",
        ]),
        mail: configured(&["MAIL_SERVER", "MAIL_USER", "MAIL_PASS", "MAIL_FROM"]),
        github_issues: configured(&["GITHUB_ISSUES_REPO", "GITHUB_ISSUES_TOKEN"]),
        storage: configured(&["STORAGE_HOST", "STORAGE_ACCESS_KEY", "STORAGE_SECRET_KEY"]),
    }
}

fn internal_error() -> ApiError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "Could not read or update server settings." })),
    )
}

pub async fn get_public_server_settings(
    State(state): State<Arc<AppState>>,
) -> Result<Json<PublicServerSettings>, ApiError> {
    let settings = server_settings::load(&state.database)
        .await
        .map_err(|_| internal_error())?;
    Ok(Json(PublicServerSettings {
        public_registration_enabled: settings.public_registration_enabled,
        google_login_enabled: settings.google_login_enabled && integration_status().google,
    }))
}

pub async fn get_admin_server_settings(
    State(state): State<Arc<AppState>>,
) -> Result<Json<AdminServerSettingsResponse>, ApiError> {
    let settings = server_settings::load(&state.database)
        .await
        .map_err(|_| internal_error())?;
    let tiers = list_tiers(&state.database)
        .await
        .map_err(|_| internal_error())?;
    Ok(Json(AdminServerSettingsResponse {
        settings,
        tiers,
        integrations: integration_status(),
    }))
}

pub async fn patch_admin_server_settings(
    State(state): State<Arc<AppState>>,
    Extension(administrator): Extension<User>,
    Json(patch): Json<ServerSettingsPatch>,
) -> Result<Json<ServerSettings>, ApiError> {
    for value in [
        patch.support_max_reports_per_day,
        patch.feature_request_max_per_day,
    ]
    .into_iter()
    .flatten()
    {
        if !(0..=100_000).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Daily limits must be between 0 and 100000." })),
            ));
        }
    }

    server_settings::update(&state.database, &patch, administrator.id)
        .await
        .map(Json)
        .map_err(|_| internal_error())
}

pub async fn patch_tier_settings(
    State(state): State<Arc<AppState>>,
    Path(level): Path<i32>,
    Extension(administrator): Extension<User>,
    Json(body): Json<TierSettingsUpdate>,
) -> Result<Json<TierQuota>, ApiError> {
    const MAX_UPLOAD_BYTES: i64 = 500 * 1024 * 1024;
    if body.max_owned_boards.is_some_and(|value| value <= 0)
        || body.max_upload_bytes <= 0
        || body.max_upload_bytes > MAX_UPLOAD_BYTES
        || body.max_storage_bytes <= 0
        || body.max_upload_bytes > body.max_storage_bytes
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Tier limits must be positive, files cannot exceed 500 MiB, and one file must fit in total storage."
            })),
        ));
    }

    server_settings::update_tier(&state.database, level, &body, administrator.id)
        .await
        .map_err(|_| internal_error())?
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Tier not found." })),
            )
        })
}
