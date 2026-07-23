//! Password and TOTP login, JWT issuance, and challenge handlers.

use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::{Duration, Utc};
use serde_json::json;
use std::sync::Arc;
use tracing::{debug, error, instrument, warn};

use crate::core::config::{get_env_bool, get_env_u64, get_env_with_default};
use crate::database::{
    apikeys::fetch_active_apikeys_by_user_id_from_db,
    login_challenges::{
        create_login_challenge, delete_login_challenge, fetch_login_challenge,
        record_failed_attempt,
    },
    users::{fetch_active_user_by_email_from_db, fetch_active_user_by_id_from_db},
};
use crate::models::auth::{LoginChallengeResponse, LoginData, LoginTotpData};
use crate::routes::AppState;
use crate::utils::auth::{encode_jwt, verify_hash, verify_totp_code};

const MAX_TOTP_ATTEMPTS: i16 = 5;

/// User sign-in endpoint.
///
/// This endpoint allows users to sign in using their email, password, and optionally a TOTP code.
///
/// # Parameters
/// - `State(pool)`: The shared database connection pool.
/// - `Json(user_data)`: The user sign-in data (email, password, and optional TOTP code).
///
/// # Returns
/// - `Ok(Json(serde_json::Value))`: A JSON response containing the JWT token if sign-in is successful.
/// - `Err((StatusCode, Json(serde_json::Value)))`: An error response if sign-in fails.
#[utoipa::path(
    post,
    path = "/login",
    tag = "auth",
    request_body = LoginData,
    responses(
        (status = 200, description = "Successful sign-in", body = serde_json::Value),
        (status = 202, description = "TOTP required", body = LoginChallengeResponse),
        (status = 400, description = "Bad request", body = serde_json::Value),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    )
)]
#[instrument(skip(state, user_data))]
pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(user_data): Json<LoginData>,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    // Fetch the user from the database based on their email.
    let user = match fetch_active_user_by_email_from_db(&state.database, &user_data.email).await {
        Ok(Some(user)) => user,
        Ok(None) | Err(_) => {
            // Log the error for failed login attempt
            error!("Failed to find user with email: {}", user_data.email);
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Incorrect credentials." })),
            ));
        }
    };

    // Fetch active API keys for the user.
    let api_key_hashes =
        match fetch_active_apikeys_by_user_id_from_db(&state.database, user.id).await {
            Ok(hashes) => hashes,
            Err(_) => {
                // Log the error fetching API keys
                error!("Error fetching API keys for user: {}", user.id);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Internal server error." })),
                ));
            }
        };

    // Check if any of the API keys match the provided password.
    let api_key_futures = api_key_hashes.iter().map(|api_key| {
        let password = user_data.password.clone();
        let hash = api_key.key_hash.clone();
        async move {
            // Verify the password against each API key hash.
            verify_hash(&password, &hash).await.unwrap_or(false)
        }
    });

    // Wait for all API key verification futures to complete.
    let any_api_key_valid = futures::future::join_all(api_key_futures)
        .await
        .into_iter()
        .any(|result| result);

    // Verify the user's password against their stored password hash.
    let password_valid = match verify_hash(&user_data.password, &user.password_hash).await {
        Ok(valid) => valid,
        Err(_) => {
            // Log the error and return unauthorized response if password verification fails
            error!(
                "Password verification failed for email: {}",
                user_data.email
            );
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Incorrect credentials." })),
            ));
        }
    };

    // Determine if the credentials are valid based on API keys or password.
    let credentials_valid = any_api_key_valid || password_valid;

    if !credentials_valid {
        // Log invalid credentials attempt
        error!("Invalid credentials for user: {}", user_data.email);
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Incorrect credentials." })),
        ));
    }

    if user.totp_secret.is_some() {
        let expires_at = Utc::now() + Duration::minutes(5);
        let challenge = create_login_challenge(&state.database, user.id, expires_at)
            .await
            .map_err(|_| {
                error!("Error creating login challenge for user: {}", user.id);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Internal server error." })),
                )
            })?;

        return Ok((
            StatusCode::ACCEPTED,
            Json(LoginChallengeResponse {
                challenge_id: challenge.id,
                expires_at: challenge.expires_at,
            }),
        )
            .into_response());
    }

    issue_login_response(&user.email)
}

/// Complete login with TOTP.
#[utoipa::path(
    post,
    path = "/login/totp",
    tag = "auth",
    request_body = LoginTotpData,
    responses(
        (status = 200, description = "Successful sign-in", body = serde_json::Value),
        (status = 400, description = "Bad request", body = serde_json::Value),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 429, description = "Too many invalid TOTP codes", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    )
)]
#[instrument(skip(state, data))]
pub async fn login_totp(
    State(state): State<Arc<AppState>>,
    Json(data): Json<LoginTotpData>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let challenge = match fetch_login_challenge(&state.database, data.challenge_id).await {
        Ok(Some(challenge)) => challenge,
        Ok(None) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid login challenge." })),
            ));
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Internal server error." })),
            ));
        }
    };

    if challenge.expires_at < Utc::now() {
        let _ = delete_login_challenge(&state.database, challenge.id).await;
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Login challenge expired." })),
        ));
    }

    let user = match fetch_active_user_by_id_from_db(&state.database, challenge.user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Incorrect credentials." })),
            ));
        }
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Internal server error." })),
            ));
        }
    };

    let totp_secret = match user.totp_secret.clone() {
        Some(secret) => secret,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "2FA is not enabled for this account." })),
            ));
        }
    };

    let totp_valid = verify_totp_code(&totp_secret, &data.totp).map_err(|error| {
        error!("Invalid TOTP configuration for user {}: {}", user.id, error);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Internal server error." })),
        )
    })?;
    if !totp_valid {
        let attempts = record_failed_attempt(&state.database, challenge.id, MAX_TOTP_ATTEMPTS)
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Internal server error." })),
                )
            })?
            .unwrap_or(MAX_TOTP_ATTEMPTS);
        if attempts >= MAX_TOTP_ATTEMPTS {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "Too many invalid codes. Sign in with your password again."
                })),
            ));
        }
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid 2FA code." })),
        ));
    }

    let _ = delete_login_challenge(&state.database, challenge.id).await;
    issue_login_response(&user.email)
}

pub(crate) fn issue_login_response(
    email: &str,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    // Generate a JWT token for the user.
    let token = encode_jwt(email.to_string()).map_err(|_| {
        error!("Error generating JWT for user: {}", email);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Internal server error." })),
        )
    })?;

    // Log the successful sign-in.
    debug!("User signed in: {}", email);

    // Prepare response headers
    let mut headers = HeaderMap::new();

    // Prevent caching of the response
    headers.insert("Cache-Control", HeaderValue::from_static("no-store"));

    let allow_cookie_auth = get_env_bool("JWT_ALLOW_COOKIE_AUTH", false);
    let force_cookie_auth = get_env_bool("JWT_FORCE_COOKIE_AUTH", false);
    let cookie_max_age = get_env_u64("JWT_COOKIE_MAX_AGE", 604800); // default: 7 days
    let use_https = get_env_bool("SERVER_HTTPS_ENABLED", false);
    let cookie_name = get_env_with_default("JWT_COOKIE_NAME", "auth_token");
    let samesite_value = get_env_with_default("JWT_COOKIE_SAMESITE", "Lax");
    let (samesite_flag, secure_flag) = match samesite_value.to_lowercase().as_str() {
        "none" if use_https => ("SameSite=None;", "Secure;"), // Enforce HTTPS requirement
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

    let cookie = format!(
        "{name}={value}; HttpOnly; Path=/; Max-Age={cookie_max_age}; {secure_flag}{samesite_flag}",
        name = cookie_name,
        value = token,
        secure_flag = secure_flag,
        samesite_flag = samesite_flag,
        cookie_max_age = cookie_max_age
    );

    if force_cookie_auth {
        headers.insert(
            axum::http::header::SET_COOKIE,
            HeaderValue::from_str(&cookie).unwrap(),
        );
        return Ok((StatusCode::OK, headers, Json(json!({ "success": true }))).into_response());
    }

    if allow_cookie_auth {
        headers.insert(
            axum::http::header::SET_COOKIE,
            HeaderValue::from_str(&cookie).unwrap(),
        );
    }

    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );

    Ok((
        StatusCode::OK,
        headers,
        Json(json!({
            "access_token": token,
            "token_type": "Bearer"
        })),
    )
        .into_response())
}
