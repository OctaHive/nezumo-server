//! Logout and authentication-cookie invalidation handler.

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::core::config::{get_env_bool, get_env_u64, get_env_with_default};

/// User sign-out endpoint.
///
/// Clears auth cookie for HttpOnly sessions.
#[utoipa::path(
    post,
    path = "/logout",
    tag = "auth",
    responses(
        (status = 200, description = "Successful sign-out", body = serde_json::Value)
    )
)]
#[instrument]
pub async fn logout() -> impl IntoResponse {
    let mut headers = HeaderMap::new();

    let use_https = get_env_bool("SERVER_HTTPS_ENABLED", false);
    let cookie_name = get_env_with_default("JWT_COOKIE_NAME", "auth_token");
    let samesite_value = get_env_with_default("JWT_COOKIE_SAMESITE", "Lax");
    let (samesite_flag, secure_flag) = match samesite_value.to_lowercase().as_str() {
        "none" if use_https => ("SameSite=None;", "Secure;"),
        "none" => {
            warn!("SameSite=None requires HTTPS. Falling back to Lax.");
            ("SameSite=Lax;", "")
        }
        "lax" => ("SameSite=Lax;", ""),
        "strict" => ("SameSite=Strict;", ""),
        _ => {
            warn!(
                "Invalid SameSite value '{}'. Allowed: None/Lax/Strict. Using Lax.",
                samesite_value
            );
            ("SameSite=Lax;", "")
        }
    };

    // Expire cookie immediately
    let cookie = format!(
        "{name}=; HttpOnly; Path=/; Max-Age=0; {secure_flag}{samesite_flag}",
        name = cookie_name,
        secure_flag = secure_flag,
        samesite_flag = samesite_flag,
    );

    headers.insert(
        axum::http::header::SET_COOKIE,
        HeaderValue::from_str(&cookie).unwrap(),
    );
    debug!("Clearing cookie: {}", cookie);

    // Optionally include JWT cookie max age for reference (kept for compatibility)
    let _ = get_env_u64("JWT_COOKIE_MAX_AGE", 604800);

    (
        StatusCode::OK,
        headers,
        axum::Json(json!({ "success": true })),
    )
}
