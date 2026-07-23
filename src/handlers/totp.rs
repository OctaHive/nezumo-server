//! Authenticated TOTP enrollment, status, confirmation, and disabling.

use std::sync::Arc;

use axum::{
    extract::{Extension, Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{Duration, Utc};
use deadpool_redis::redis::AsyncCommands;
use serde_json::{json, Value};
use tracing::{instrument, warn};

use crate::database::totp_enrollments;
use crate::models::auth::{
    TotpConfirmData, TotpDisableData, TotpSetupResponse, TotpStatusResponse,
};
use crate::models::user::User;
use crate::routes::AppState;
use crate::utils::auth::{generate_totp_secret, totp_provisioning_uri, verify_totp_code};

type ApiError = (StatusCode, Json<Value>);

const SETUP_TTL_MINUTES: i64 = 10;
const MAX_CONFIRM_ATTEMPTS: i16 = 5;
const DISABLE_ATTEMPT_WINDOW_SECS: i64 = 300;

fn api_error(status: StatusCode, message: &str) -> ApiError {
    (status, Json(json!({ "error": message })))
}

fn no_store_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers
}

fn disable_attempt_key(user_id: uuid::Uuid) -> String {
    format!("auth:totp:disable-attempts:{user_id}")
}

async fn disable_attempts(state: &AppState, user_id: uuid::Uuid) -> Result<i64, ApiError> {
    let mut connection = state.cache.get().await.map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TOTP verification is temporarily unavailable.",
        )
    })?;
    let attempts: Option<i64> =
        connection
            .get(disable_attempt_key(user_id))
            .await
            .map_err(|_| {
                api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "TOTP verification is temporarily unavailable.",
                )
            })?;
    Ok(attempts.unwrap_or(0))
}

async fn record_disable_failure(state: &AppState, user_id: uuid::Uuid) -> Result<i64, ApiError> {
    let mut connection = state.cache.get().await.map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TOTP verification is temporarily unavailable.",
        )
    })?;
    let key = disable_attempt_key(user_id);
    let attempts: i64 = connection.incr(&key, 1).await.map_err(|_| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TOTP verification is temporarily unavailable.",
        )
    })?;
    if attempts == 1 {
        let _: bool = connection
            .expire(&key, DISABLE_ATTEMPT_WINDOW_SECS)
            .await
            .map_err(|_| {
                api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "TOTP verification is temporarily unavailable.",
                )
            })?;
    }
    Ok(attempts)
}

async fn clear_disable_failures(state: &AppState, user_id: uuid::Uuid) {
    if let Ok(mut connection) = state.cache.get().await {
        let _: Result<usize, _> = connection.del(disable_attempt_key(user_id)).await;
    }
}

#[utoipa::path(
    get,
    path = "/users/current/totp",
    tag = "user",
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "Current TOTP status", body = TotpStatusResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, current_user))]
pub async fn get_totp_status(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> Result<(HeaderMap, Json<TotpStatusResponse>), ApiError> {
    let pending = totp_enrollments::fetch(&state.database, current_user.id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not read TOTP status.",
            )
        })?;
    let setup_pending = pending
        .is_some_and(|row| row.expires_at > Utc::now() && row.attempts < MAX_CONFIRM_ATTEMPTS);
    Ok((
        no_store_headers(),
        Json(TotpStatusResponse {
            enabled: current_user.totp_secret.is_some(),
            setup_pending,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users/current/totp/setup",
    tag = "user",
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "TOTP enrollment started", body = TotpSetupResponse),
        (status = 409, description = "TOTP is already enabled"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, current_user))]
pub async fn setup_totp(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> Result<(HeaderMap, Json<TotpSetupResponse>), ApiError> {
    if current_user.totp_secret.is_some() {
        return Err(api_error(
            StatusCode::CONFLICT,
            "TOTP is already enabled for this account.",
        ));
    }

    let secret = generate_totp_secret();
    let provisioning_uri = totp_provisioning_uri(&current_user.email, &secret).map_err(|_| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not start TOTP setup.",
        )
    })?;
    let expires_at = Utc::now() + Duration::minutes(SETUP_TTL_MINUTES);
    totp_enrollments::upsert(&state.database, current_user.id, &secret, expires_at)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not start TOTP setup.",
            )
        })?;

    Ok((
        no_store_headers(),
        Json(TotpSetupResponse {
            secret,
            provisioning_uri,
            expires_at,
            algorithm: "SHA1".to_string(),
            digits: 6,
            period: 30,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users/current/totp/confirm",
    tag = "user",
    security(("jwt_token" = [])),
    request_body = TotpConfirmData,
    responses(
        (status = 200, description = "TOTP enabled", body = TotpStatusResponse),
        (status = 401, description = "Invalid TOTP code"),
        (status = 404, description = "No pending setup"),
        (status = 410, description = "Setup expired"),
        (status = 429, description = "Too many attempts"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, current_user, data))]
pub async fn confirm_totp(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(data): Json<TotpConfirmData>,
) -> Result<(HeaderMap, Json<TotpStatusResponse>), ApiError> {
    if current_user.totp_secret.is_some() {
        return Err(api_error(
            StatusCode::CONFLICT,
            "TOTP is already enabled for this account.",
        ));
    }

    let pending = totp_enrollments::fetch(&state.database, current_user.id)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not confirm TOTP setup.",
            )
        })?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "No pending TOTP setup was found."))?;
    if pending.expires_at <= Utc::now() {
        let _ = totp_enrollments::delete(&state.database, current_user.id).await;
        return Err(api_error(
            StatusCode::GONE,
            "TOTP setup expired. Start it again.",
        ));
    }

    let valid = verify_totp_code(&pending.secret, data.totp.trim()).map_err(|_| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not confirm TOTP setup.",
        )
    })?;
    if !valid {
        let attempts = totp_enrollments::record_failed_attempt(
            &state.database,
            current_user.id,
            MAX_CONFIRM_ATTEMPTS,
        )
        .await
        .map_err(|_| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not confirm TOTP setup.",
            )
        })?
        .unwrap_or(MAX_CONFIRM_ATTEMPTS);
        if attempts >= MAX_CONFIRM_ATTEMPTS {
            return Err(api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "Too many invalid codes. Start TOTP setup again.",
            ));
        }
        return Err(api_error(StatusCode::UNAUTHORIZED, "Invalid TOTP code."));
    }

    let activated = totp_enrollments::activate(&state.database, current_user.id, &pending.secret)
        .await
        .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "Could not enable TOTP."))?;
    if !activated {
        return Err(api_error(
            StatusCode::CONFLICT,
            "TOTP setup changed. Start it again.",
        ));
    }

    Ok((
        no_store_headers(),
        Json(TotpStatusResponse {
            enabled: true,
            setup_pending: false,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users/current/totp/disable",
    tag = "user",
    security(("jwt_token" = [])),
    request_body = TotpDisableData,
    responses(
        (status = 200, description = "TOTP disabled", body = TotpStatusResponse),
        (status = 400, description = "TOTP is not enabled"),
        (status = 401, description = "Invalid TOTP code"),
        (status = 429, description = "Too many invalid TOTP codes"),
        (status = 503, description = "TOTP verification temporarily unavailable"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, current_user, data))]
pub async fn disable_totp(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(data): Json<TotpDisableData>,
) -> Result<(HeaderMap, Json<TotpStatusResponse>), ApiError> {
    let secret = current_user.totp_secret.as_deref().ok_or_else(|| {
        api_error(
            StatusCode::BAD_REQUEST,
            "TOTP is not enabled for this account.",
        )
    })?;
    if disable_attempts(&state, current_user.id).await? >= i64::from(MAX_CONFIRM_ATTEMPTS) {
        return Err(api_error(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many invalid codes. Try again later.",
        ));
    }
    let valid = verify_totp_code(secret, data.totp.trim())
        .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "Could not disable TOTP."))?;
    if !valid {
        let attempts = record_disable_failure(&state, current_user.id).await?;
        if attempts >= i64::from(MAX_CONFIRM_ATTEMPTS) {
            return Err(api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "Too many invalid codes. Try again later.",
            ));
        }
        return Err(api_error(StatusCode::UNAUTHORIZED, "Invalid TOTP code."));
    }

    totp_enrollments::disable(&state.database, current_user.id)
        .await
        .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "Could not disable TOTP."))?;
    clear_disable_failures(&state, current_user.id).await;
    Ok((
        no_store_headers(),
        Json(TotpStatusResponse {
            enabled: false,
            setup_pending: false,
        }),
    ))
}

/// Administrator recovery action for an account whose authenticator is no
/// longer available. No TOTP secret or recovery credential is disclosed.
#[utoipa::path(
    post,
    path = "/users/{id}/totp/reset",
    tag = "user",
    params(("id" = String, Path, description = "User UUID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "TOTP reset; the user may enroll again", body = TotpStatusResponse),
        (status = 400, description = "Invalid user ID"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, current_user))]
pub async fn reset_user_totp(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Extension(current_user): Extension<User>,
) -> Result<(HeaderMap, Json<TotpStatusResponse>), ApiError> {
    let user_id = id
        .parse::<uuid::Uuid>()
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "Invalid user ID."))?;

    let reset = totp_enrollments::disable(&state.database, user_id)
        .await
        .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "Could not reset TOTP."))?;
    if !reset {
        return Err(api_error(StatusCode::NOT_FOUND, "User not found."));
    }

    clear_disable_failures(&state, user_id).await;
    warn!(
        administrator_id = %current_user.id,
        target_user_id = %user_id,
        "Administrator reset TOTP for user"
    );

    Ok((
        no_store_headers(),
        Json(TotpStatusResponse {
            enabled: false,
            setup_pending: false,
        }),
    ))
}
