//! OAuth 2.0/OpenID Connect authorization and callback handlers.

use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use chrono::{Duration as ChronoDuration, Utc};
use deadpool_redis::redis::AsyncCommands;
use oauth2::{PkceCodeChallenge, PkceCodeVerifier};
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata};
use openidconnect::reqwest::async_http_client;
use openidconnect::OAuth2TokenResponse;
use openidconnect::{
    AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce, RedirectUrl, Scope,
};
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::{instrument, warn};

use crate::core::config::{get_env, get_env_bool, get_env_u64, get_env_with_default};
use crate::database::{
    login_challenges::create_login_challenge,
    oauth_accounts::upsert_oauth_account,
    server_settings,
    users::{
        check_user_exists_by_username, fetch_user_by_email_from_db, insert_user_into_db,
        update_user_profile_picture_in_db,
    },
};
use crate::models::auth::LoginChallengeResponse;
use crate::routes::AppState;
use crate::utils::auth::hash_password;

static GOOGLE_CLIENT: OnceCell<CoreClient> = OnceCell::const_new();

#[derive(Debug, Error)]
pub enum OAuthError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("OAuth error: {0}")]
    OAuth(String),
}

#[derive(Debug, Deserialize)]
pub struct OAuthStartQuery {
    pub redirect: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OAuthCallbackQuery {
    pub code: String,
    pub state: String,
}

#[derive(Deserialize, serde::Serialize)]
struct OAuthStateData {
    nonce: String,
    pkce_verifier: String,
    redirect: String,
}

fn safe_redirect(input: Option<String>) -> String {
    let app_origin = get_env_with_default("APP_ORIGIN", "http://localhost:5173");
    let redirect = input.unwrap_or_else(|| "/".to_string());

    if redirect.starts_with('/') && !redirect.starts_with("//") {
        return format!("{}{}", app_origin.trim_end_matches('/'), redirect);
    }

    if (redirect.starts_with("http://") || redirect.starts_with("https://"))
        && redirect.starts_with(&app_origin)
    {
        return redirect;
    }

    format!("{}/", app_origin.trim_end_matches('/'))
}

fn append_totp_challenge(redirect: &str, challenge_id: uuid::Uuid) -> String {
    let separator = if redirect.contains('?') { '&' } else { '?' };
    format!("{redirect}{separator}totpChallenge={challenge_id}")
}

async fn google_client() -> Result<CoreClient, OAuthError> {
    GOOGLE_CLIENT
        .get_or_try_init(|| async {
            let issuer = get_env_with_default("GOOGLE_ISSUER_URL", "https://accounts.google.com");
            let issuer_url =
                IssuerUrl::new(issuer).map_err(|e| OAuthError::Config(e.to_string()))?;

            let provider_metadata =
                CoreProviderMetadata::discover_async(issuer_url, async_http_client)
                    .await
                    .map_err(|e| OAuthError::OAuth(e.to_string()))?;

            let client_id = ClientId::new(get_env("GOOGLE_CLIENT_ID"));
            let client_secret = ClientSecret::new(get_env("GOOGLE_CLIENT_SECRET"));
            let redirect_url = RedirectUrl::new(get_env("GOOGLE_REDIRECT_URL"))
                .map_err(|e| OAuthError::Config(e.to_string()))?;

            Ok::<CoreClient, OAuthError>(
                CoreClient::from_provider_metadata(
                    provider_metadata,
                    client_id,
                    Some(client_secret),
                )
                .set_redirect_uri(redirect_url),
            )
        })
        .await
        .cloned()
}

#[utoipa::path(
    get,
    path = "/oauth/google",
    tag = "auth",
    responses(
        (status = 302, description = "Redirects to Google OAuth"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn oauth_google(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<OAuthStartQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let settings = server_settings::load(&state.database).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not read server settings." })),
        )
    })?;
    if !settings.google_login_enabled {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Google login is disabled." })),
        ));
    }
    let client = google_client().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let redirect = safe_redirect(query.redirect);

    let (auth_url, csrf_state, nonce) = client
        .authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        .add_scope(Scope::new("openid".to_string()))
        .add_scope(Scope::new("email".to_string()))
        .add_scope(Scope::new("profile".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    let state_data = OAuthStateData {
        nonce: nonce.secret().clone(),
        pkce_verifier: pkce_verifier.secret().clone(),
        redirect,
    };

    let mut conn = state.cache.get().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    let key = format!("oauth_state:{}", csrf_state.secret());
    let value = serde_json::to_string(&state_data).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    let _: () = conn.set_ex(key, value, 600).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    Ok(Redirect::to(auth_url.as_str()))
}

#[utoipa::path(
    get,
    path = "/oauth/google/callback",
    tag = "auth",
    responses(
        (status = 302, description = "OAuth callback")
    )
)]
#[instrument(skip(state))]
pub async fn oauth_google_callback(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<OAuthCallbackQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let runtime_settings = server_settings::load(&state.database).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not read server settings." })),
        )
    })?;
    if !runtime_settings.google_login_enabled {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Google login is disabled." })),
        ));
    }
    let client = google_client().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    let mut conn = state.cache.get().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    let key = format!("oauth_state:{}", query.state);
    let value: Option<String> = conn.get(&key).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;
    let _: () = conn.del(&key).await.unwrap_or(());

    let state_data: OAuthStateData = match value {
        Some(v) => serde_json::from_str(&v).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
        })?,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid OAuth state." })),
            ));
        }
    };

    let token_response = client
        .exchange_code(AuthorizationCode::new(query.code))
        .set_pkce_verifier(PkceCodeVerifier::new(state_data.pkce_verifier))
        .request_async(async_http_client)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
        })?;

    let id_token = token_response.extra_fields().id_token().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing id_token" })),
        )
    })?;

    let claims = id_token
        .claims(&client.id_token_verifier(), &Nonce::new(state_data.nonce))
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
        })?;

    let email = claims.email().map(|e| e.to_string()).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Email not available" })),
        )
    })?;

    if let Some(false) = claims.email_verified() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Email not verified" })),
        ));
    }

    let sub = claims.subject().as_str().to_string();
    let picture_url = claims
        .picture()
        .and_then(|p| p.get(None))
        .map(|p| p.as_str().to_string());

    let user = fetch_user_by_email_from_db(&state.database, &email)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;

    if user
        .as_ref()
        .is_some_and(|existing| existing.status != "active")
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Account is disabled" })),
        ));
    }

    if user.is_none() {
        if !runtime_settings.public_registration_enabled {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Public registration is disabled." })),
            ));
        }
        let mut base = email
            .split('@')
            .next()
            .unwrap_or("user")
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>()
            .to_lowercase();

        if base.len() < 3 {
            base = format!("user{}", base);
        }
        if base.len() > 30 {
            base.truncate(30);
        }

        let mut username = base.clone();
        let mut attempts = 0;
        while check_user_exists_by_username(&state.database, &username)
            .await
            .unwrap_or(true)
        {
            attempts += 1;
            let suffix: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(4)
                .map(char::from)
                .collect();
            let trimmed = base.chars().take(26).collect::<String>();
            username = format!("{}_{}", trimmed, suffix.to_lowercase());
            if attempts > 10 {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Failed to generate username" })),
                ));
            }
        }

        let random_password: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let hashed_password = hash_password(&random_password).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;

        insert_user_into_db(
            &state.database,
            &username,
            &email,
            &hashed_password,
            None,
            1,
            1,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
    }

    let access_token = token_response.access_token().secret().to_string();
    let refresh_token = token_response
        .refresh_token()
        .map(|t| t.secret().to_string());
    let expires_at = token_response
        .expires_in()
        .map(|d| Utc::now() + ChronoDuration::from_std(d).unwrap_or(ChronoDuration::minutes(60)));

    let user = fetch_user_by_email_from_db(&state.database, &email)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "User not found after creation" })),
            )
        })?;

    if user.status != "active" {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Account is disabled" })),
        ));
    }

    upsert_oauth_account(
        &state.database,
        user.id,
        "google",
        &sub,
        &access_token,
        refresh_token.as_deref(),
        expires_at,
    )
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    if let Some(url) = picture_url {
        if user.profile_picture_url.is_none() {
            let _ = update_user_profile_picture_in_db(&state.database, user.id, &url).await;
        }
    }

    // Google verifies possession of the external account, but it must not
    // bypass a second factor configured in Nezumo. Defer issuing our session
    // until the regular TOTP challenge has been completed.
    if user.totp_secret.is_some() {
        let challenge = create_login_challenge(
            &state.database,
            user.id,
            Utc::now() + ChronoDuration::minutes(5),
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
        let redirect = append_totp_challenge(&state_data.redirect, challenge.id);
        let mut headers = HeaderMap::new();
        headers.insert("Cache-Control", HeaderValue::from_static("no-store"));
        return Ok((headers, Redirect::to(&redirect)));
    }

    let token = crate::utils::auth::encode_jwt(email.clone()).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    let mut headers = HeaderMap::new();
    let allow_cookie_auth = get_env_bool("JWT_ALLOW_COOKIE_AUTH", false);
    let force_cookie_auth = get_env_bool("JWT_FORCE_COOKIE_AUTH", false);
    if !allow_cookie_auth && !force_cookie_auth {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Cookie auth disabled" })),
        ));
    }

    let cookie_max_age = get_env_u64("JWT_COOKIE_MAX_AGE", 604800);
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

    let cookie = format!(
        "{name}={value}; HttpOnly; Path=/; Max-Age={cookie_max_age}; {secure_flag}{samesite_flag}",
        name = cookie_name,
        value = token,
        secure_flag = secure_flag,
        samesite_flag = samesite_flag,
        cookie_max_age = cookie_max_age
    );

    headers.insert(
        axum::http::header::SET_COOKIE,
        HeaderValue::from_str(&cookie).unwrap(),
    );
    let redirect = state_data.redirect;
    Ok((headers, Redirect::to(&redirect)))
}

#[derive(Debug, Deserialize)]
pub struct OAuthExchangeBody {
    pub id_token: String,
}

/// Native/desktop Google sign-in: the desktop app completes the PKCE loopback
/// flow with Google itself, then POSTs the resulting `id_token` here. We verify
/// it via Google's `tokeninfo` endpoint (checks signature + expiry), confirm the
/// audience is one of our Google client ids, then mint our own JWT — reusing the
/// same find/create-user logic as the browser callback.
#[instrument(skip(state, body))]
pub async fn oauth_google_exchange(
    State(state): State<std::sync::Arc<AppState>>,
    Json(body): Json<OAuthExchangeBody>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let runtime_settings = server_settings::load(&state.database).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not read server settings." })),
        )
    })?;
    if !runtime_settings.google_login_enabled {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Google login is disabled." })),
        ));
    }
    // The id_token is a JWT (chars [A-Za-z0-9-_.]) — safe to put in the query.
    let url = format!(
        "https://oauth2.googleapis.com/tokeninfo?id_token={}",
        body.id_token
    );
    let resp = reqwest::get(&url).await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": format!("tokeninfo: {e}") })),
        )
    })?;
    if !resp.status().is_success() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid id_token" })),
        ));
    }
    let claims: serde_json::Value = resp.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": format!("tokeninfo json: {e}") })),
        )
    })?;

    // Audience must be one of our Google client ids (desktop or web).
    let aud = claims.get("aud").and_then(|v| v.as_str()).unwrap_or("");
    let desktop_id = get_env_with_default("GOOGLE_DESKTOP_CLIENT_ID", "");
    let web_id = get_env_with_default("GOOGLE_CLIENT_ID", "");
    let aud_ok =
        (!desktop_id.is_empty() && aud == desktop_id) || (!web_id.is_empty() && aud == web_id);
    if !aud_ok {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Audience mismatch" })),
        ));
    }

    // tokeninfo returns email_verified as the string "true".
    let email_verified = match claims.get("email_verified") {
        Some(serde_json::Value::String(s)) => s == "true",
        Some(serde_json::Value::Bool(b)) => *b,
        _ => false,
    };
    if !email_verified {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Email not verified" })),
        ));
    }
    let email = claims
        .get("email")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Email not available" })),
            )
        })?;
    let picture_url = claims
        .get("picture")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Find or create the user (same as the browser callback).
    let existing = fetch_user_by_email_from_db(&state.database, &email)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
    if existing
        .as_ref()
        .is_some_and(|user| user.status != "active")
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Account is disabled" })),
        ));
    }
    if existing.is_none() {
        if !runtime_settings.public_registration_enabled {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Public registration is disabled." })),
            ));
        }
        let mut base = email
            .split('@')
            .next()
            .unwrap_or("user")
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>()
            .to_lowercase();
        if base.len() < 3 {
            base = format!("user{}", base);
        }
        if base.len() > 30 {
            base.truncate(30);
        }
        let mut username = base.clone();
        let mut attempts = 0;
        while check_user_exists_by_username(&state.database, &username)
            .await
            .unwrap_or(true)
        {
            attempts += 1;
            let suffix: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(4)
                .map(char::from)
                .collect();
            let trimmed = base.chars().take(26).collect::<String>();
            username = format!("{}_{}", trimmed, suffix.to_lowercase());
            if attempts > 10 {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Failed to generate username" })),
                ));
            }
        }
        let random_password: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let hashed_password = hash_password(&random_password).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
        insert_user_into_db(
            &state.database,
            &username,
            &email,
            &hashed_password,
            None,
            1,
            1,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
    }

    if let Some(url) = picture_url {
        if let Ok(Some(user)) = fetch_user_by_email_from_db(&state.database, &email).await {
            if user.profile_picture_url.is_none() {
                let _ = update_user_profile_picture_in_db(&state.database, user.id, &url).await;
            }
        }
    }

    let user = fetch_user_by_email_from_db(&state.database, &email)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "User not found after creation" })),
            )
        })?;

    // The native exchange endpoint follows the same rule as browser OAuth:
    // Google identity is not enough to bypass a configured Nezumo TOTP factor.
    if user.totp_secret.is_some() {
        let challenge = create_login_challenge(
            &state.database,
            user.id,
            Utc::now() + ChronoDuration::minutes(5),
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
        return Ok((
            StatusCode::ACCEPTED,
            [("Cache-Control", "no-store")],
            Json(LoginChallengeResponse {
                challenge_id: challenge.id,
                expires_at: challenge.expires_at,
            }),
        )
            .into_response());
    }

    let token = crate::utils::auth::encode_jwt(email).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;
    Ok(Json(json!({ "token": token })).into_response())
}

#[cfg(test)]
mod tests {
    use super::append_totp_challenge;

    #[test]
    fn totp_challenge_is_added_to_oauth_redirect() {
        let id = uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
        assert_eq!(
            append_totp_challenge("https://app.example/login", id),
            "https://app.example/login?totpChallenge=00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(
            append_totp_challenge("https://app.example/login?returnTo=%2Fboards", id),
            "https://app.example/login?returnTo=%2Fboards&totpChallenge=00000000-0000-4000-8000-000000000001"
        );
    }
}
