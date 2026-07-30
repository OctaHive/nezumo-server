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
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::{instrument, warn};

use crate::core::config::{get_env, get_env_bool, get_env_u64, get_env_with_default};
use crate::database::{
    login_challenges::create_login_challenge,
    oauth_accounts::{fetch_user_by_oauth_account, upsert_oauth_account},
    server_settings,
    users::{
        check_user_exists_by_username, fetch_user_by_email_from_db,
        fill_missing_oauth_profile_names, insert_oauth_user_into_db, insert_user_into_db,
        update_user_profile_picture_in_db,
    },
};
use crate::models::auth::LoginChallengeResponse;
use crate::models::user::User;
use crate::routes::AppState;
use crate::utils::auth::hash_password;

static GOOGLE_CLIENT: OnceCell<CoreClient> = OnceCell::const_new();
type ApiError = (StatusCode, Json<serde_json::Value>);

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

#[derive(Debug, Deserialize)]
pub struct VkOAuthCallbackQuery {
    pub code: String,
    pub state: String,
    pub device_id: String,
}

#[derive(Deserialize, Serialize)]
struct OAuthStateData {
    provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nonce: Option<String>,
    pkce_verifier: String,
    redirect: String,
}

struct OAuthLogin {
    provider: &'static str,
    provider_user_id: String,
    email: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    picture_url: Option<String>,
    access_token: String,
    refresh_token: Option<String>,
    expires_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct YandexTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct YandexUserInfo {
    id: String,
    client_id: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    display_name: Option<String>,
    real_name: Option<String>,
    default_email: Option<String>,
    emails: Option<Vec<String>>,
    is_avatar_empty: Option<bool>,
    default_avatar_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VkTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
    state: String,
    user_id: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct VkUserInfoResponse {
    user: VkUserInfo,
}

#[derive(Debug, Deserialize)]
struct VkUserInfo {
    user_id: Option<serde_json::Value>,
    email: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    avatar: Option<String>,
}

fn safe_redirect(input: Option<String>) -> String {
    let app_origin = get_env_with_default("APP_ORIGIN", "http://localhost:5173");
    safe_redirect_for_origin(input.as_deref(), &app_origin)
}

fn safe_redirect_for_origin(input: Option<&str>, app_origin: &str) -> String {
    let fallback = format!("{}/", app_origin.trim_end_matches('/'));
    let Ok(origin) = url::Url::parse(&fallback) else {
        return "http://localhost:5173/".to_string();
    };
    let redirect = input.unwrap_or("/");
    let candidate = if redirect.starts_with('/') && !redirect.starts_with("//") {
        origin.join(redirect)
    } else {
        url::Url::parse(redirect)
    };
    let Ok(candidate) = candidate else {
        return fallback;
    };
    let same_origin = matches!(candidate.scheme(), "http" | "https")
        && candidate.scheme() == origin.scheme()
        && candidate.host_str() == origin.host_str()
        && candidate.port_or_known_default() == origin.port_or_known_default()
        && candidate.username().is_empty()
        && candidate.password().is_none();
    if same_origin {
        candidate.to_string()
    } else {
        fallback
    }
}

fn append_totp_challenge(redirect: &str, challenge_id: uuid::Uuid) -> String {
    let separator = if redirect.contains('?') { '&' } else { '?' };
    format!("{redirect}{separator}totpChallenge={challenge_id}")
}

fn oauth_error(status: StatusCode, message: impl Into<String>) -> ApiError {
    (status, Json(json!({ "error": message.into() })))
}

fn required_env(key: &str) -> Result<String, ApiError> {
    let value = get_env_with_default(key, "");
    if value.trim().is_empty() {
        return Err(oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Missing required environment variable: {key}"),
        ));
    }
    Ok(value)
}

fn token_expires_at(expires_in: Option<u64>) -> Option<chrono::DateTime<Utc>> {
    expires_in.and_then(|seconds| {
        let seconds = i64::try_from(seconds).ok()?;
        Utc::now().checked_add_signed(ChronoDuration::seconds(seconds))
    })
}

fn normalize_profile_name(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_string();
    if value.is_empty() {
        return None;
    }
    Some(value.chars().take(50).collect())
}

fn json_id(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) if !value.is_empty() => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

async fn store_oauth_state(
    state: &AppState,
    csrf_state: &str,
    state_data: &OAuthStateData,
) -> Result<(), ApiError> {
    let mut conn = state
        .cache
        .get()
        .await
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let value = serde_json::to_string(state_data)
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let key = format!("oauth_state:{csrf_state}");
    conn.set_ex::<_, _, ()>(key, value, 600)
        .await
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn take_oauth_state(
    state: &AppState,
    csrf_state: &str,
    expected_provider: &str,
) -> Result<OAuthStateData, ApiError> {
    let mut conn = state
        .cache
        .get()
        .await
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let key = format!("oauth_state:{csrf_state}");
    let value: Option<String> = conn
        .get(&key)
        .await
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let _: () = conn.del(&key).await.unwrap_or(());
    let value =
        value.ok_or_else(|| oauth_error(StatusCode::BAD_REQUEST, "Invalid OAuth state."))?;
    let state_data: OAuthStateData = serde_json::from_str(&value)
        .map_err(|e| oauth_error(StatusCode::BAD_REQUEST, e.to_string()))?;
    if state_data.provider != expected_provider {
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            "OAuth provider does not match the saved state.",
        ));
    }
    Ok(state_data)
}

async fn find_or_create_oauth_user(
    state: &AppState,
    provider: &str,
    provider_user_id: &str,
    email: Option<&str>,
    public_registration_enabled: bool,
) -> Result<User, ApiError> {
    if let Some(user) = fetch_user_by_oauth_account(&state.database, provider, provider_user_id)
        .await
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        if user.status != "active" {
            return Err(oauth_error(StatusCode::FORBIDDEN, "Account is disabled"));
        }
        return Ok(user);
    }

    let existing = match email {
        Some(email) => fetch_user_by_email_from_db(&state.database, email)
            .await
            .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
        None => None,
    };

    if existing
        .as_ref()
        .is_some_and(|user| user.status != "active")
    {
        return Err(oauth_error(StatusCode::FORBIDDEN, "Account is disabled"));
    }

    if existing.is_none() {
        if !public_registration_enabled {
            return Err(oauth_error(
                StatusCode::FORBIDDEN,
                "Public registration is disabled.",
            ));
        }

        let username_source = email
            .and_then(|value| value.split('@').next())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{provider}_{provider_user_id}"));
        let mut base = username_source
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>()
            .to_lowercase();
        if base.len() < 3 {
            base = format!("user{base}");
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
            if attempts > 10 {
                return Err(oauth_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to generate username",
                ));
            }
            let suffix: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(4)
                .map(char::from)
                .collect();
            let trimmed = base.chars().take(25).collect::<String>();
            username = format!("{}_{suffix}", trimmed).to_lowercase();
        }

        let random_password: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let hashed_password = hash_password(&random_password)
            .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let user = insert_oauth_user_into_db(&state.database, &username, email, &hashed_password)
            .await
            .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        return Ok(user);
    }

    existing.ok_or_else(|| {
        oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "User not found after OAuth lookup",
        )
    })
}

async fn complete_browser_oauth(
    state: &AppState,
    public_registration_enabled: bool,
    state_data: OAuthStateData,
    login: OAuthLogin,
) -> Result<(HeaderMap, Redirect), ApiError> {
    let email = login
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
        .map(str::to_lowercase);

    let mut user = find_or_create_oauth_user(
        state,
        login.provider,
        &login.provider_user_id,
        email.as_deref(),
        public_registration_enabled,
    )
    .await?;
    fill_missing_oauth_profile_names(
        &state.database,
        user.id,
        login.first_name.as_deref(),
        login.last_name.as_deref(),
    )
    .await
    .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if user.first_name.is_none() {
        user.first_name = login.first_name.clone();
    }
    if user.last_name.is_none() {
        user.last_name = login.last_name.clone();
    }
    upsert_oauth_account(
        &state.database,
        user.id,
        login.provider,
        &login.provider_user_id,
        &login.access_token,
        login.refresh_token.as_deref(),
        login.expires_at,
    )
    .await
    .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if user.profile_picture_url.is_none() {
        if let Some(picture_url) = login.picture_url.filter(|url| !url.trim().is_empty()) {
            let _ = update_user_profile_picture_in_db(&state.database, user.id, &picture_url).await;
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert("Cache-Control", HeaderValue::from_static("no-store"));
    if user.totp_secret.is_some() {
        let challenge = create_login_challenge(
            &state.database,
            user.id,
            Utc::now() + ChronoDuration::minutes(5),
        )
        .await
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let redirect = append_totp_challenge(&state_data.redirect, challenge.id);
        return Ok((headers, Redirect::to(&redirect)));
    }

    let token = crate::utils::auth::encode_jwt(user.id.to_string())
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let allow_cookie_auth = get_env_bool("JWT_ALLOW_COOKIE_AUTH", false);
    let force_cookie_auth = get_env_bool("JWT_FORCE_COOKIE_AUTH", false);
    if !allow_cookie_auth && !force_cookie_auth {
        return Err(oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Cookie auth disabled",
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
        "{cookie_name}={token}; HttpOnly; Path=/; Max-Age={cookie_max_age}; \
         {secure_flag}{samesite_flag}"
    );
    let cookie = HeaderValue::from_str(&cookie).map_err(|e| {
        oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Invalid authentication cookie: {e}"),
        )
    })?;
    headers.insert(axum::http::header::SET_COOKIE, cookie);
    Ok((headers, Redirect::to(&state_data.redirect)))
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
        provider: "google".to_string(),
        nonce: Some(nonce.secret().clone()),
        pkce_verifier: pkce_verifier.secret().clone(),
        redirect,
    };

    store_oauth_state(&state, csrf_state.secret(), &state_data).await?;
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

    let state_data = take_oauth_state(&state, &query.state, "google").await?;

    let token_response = client
        .exchange_code(AuthorizationCode::new(query.code))
        .set_pkce_verifier(PkceCodeVerifier::new(state_data.pkce_verifier.clone()))
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

    let nonce = state_data
        .nonce
        .as_ref()
        .ok_or_else(|| oauth_error(StatusCode::BAD_REQUEST, "Missing OAuth nonce."))?;
    let claims = id_token
        .claims(&client.id_token_verifier(), &Nonce::new(nonce.clone()))
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
    let login = OAuthLogin {
        provider: "google",
        provider_user_id: sub,
        email: Some(email),
        first_name: None,
        last_name: None,
        picture_url,
        access_token: token_response.access_token().secret().to_string(),
        refresh_token: token_response
            .refresh_token()
            .map(|token| token.secret().to_string()),
        expires_at: token_expires_at(
            token_response
                .expires_in()
                .map(|duration| duration.as_secs()),
        ),
    };
    complete_browser_oauth(
        &state,
        runtime_settings.public_registration_enabled,
        state_data,
        login,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/oauth/yandex",
    tag = "auth",
    responses(
        (status = 302, description = "Redirects to Yandex ID"),
        (status = 403, description = "Yandex login is disabled"),
        (status = 500, description = "OAuth configuration error")
    )
)]
#[instrument(skip(state))]
pub async fn oauth_yandex(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<OAuthStartQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let settings = server_settings::load(&state.database).await.map_err(|_| {
        oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read server settings.",
        )
    })?;
    if !settings.yandex_login_enabled {
        return Err(oauth_error(
            StatusCode::FORBIDDEN,
            "Yandex login is disabled.",
        ));
    }

    let client_id = required_env("YANDEX_CLIENT_ID")?;
    required_env("YANDEX_CLIENT_SECRET")?;
    let redirect_url = required_env("YANDEX_REDIRECT_URL")?;
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let csrf_state = CsrfToken::new_random();
    let mut auth_url = url::Url::parse("https://oauth.yandex.ru/authorize")
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    auth_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &client_id)
        .append_pair("redirect_uri", &redirect_url)
        .append_pair("scope", "login:info login:email login:avatar")
        .append_pair("state", csrf_state.secret())
        .append_pair("code_challenge", pkce_challenge.as_str())
        .append_pair("code_challenge_method", "S256");

    let state_data = OAuthStateData {
        provider: "yandex".to_string(),
        nonce: None,
        pkce_verifier: pkce_verifier.secret().clone(),
        redirect: safe_redirect(query.redirect),
    };
    store_oauth_state(&state, csrf_state.secret(), &state_data).await?;
    Ok(Redirect::to(auth_url.as_str()))
}

#[utoipa::path(
    get,
    path = "/oauth/yandex/callback",
    tag = "auth",
    responses(
        (status = 302, description = "Completes Yandex ID login"),
        (status = 400, description = "Invalid callback or provider response"),
        (status = 403, description = "Login or public registration is disabled")
    )
)]
#[instrument(skip(state))]
pub async fn oauth_yandex_callback(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<OAuthCallbackQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let settings = server_settings::load(&state.database).await.map_err(|_| {
        oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read server settings.",
        )
    })?;
    if !settings.yandex_login_enabled {
        return Err(oauth_error(
            StatusCode::FORBIDDEN,
            "Yandex login is disabled.",
        ));
    }

    let state_data = take_oauth_state(&state, &query.state, "yandex").await?;
    let client_id = required_env("YANDEX_CLIENT_ID")?;
    let client_secret = required_env("YANDEX_CLIENT_SECRET")?;
    let http = reqwest::Client::new();
    let token_response = http
        .post("https://oauth.yandex.ru/token")
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", query.code.as_str()),
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("code_verifier", state_data.pkce_verifier.as_str()),
        ])
        .send()
        .await
        .map_err(|e| {
            oauth_error(
                StatusCode::BAD_GATEWAY,
                format!("Yandex token request: {e}"),
            )
        })?;
    let token_status = token_response.status();
    if !token_status.is_success() {
        let details = token_response.text().await.unwrap_or_default();
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            format!("Yandex token exchange failed ({token_status}): {details}"),
        ));
    }
    let token: YandexTokenResponse = token_response.json().await.map_err(|e| {
        oauth_error(
            StatusCode::BAD_GATEWAY,
            format!("Invalid Yandex token response: {e}"),
        )
    })?;

    let user_response = http
        .get("https://login.yandex.ru/info")
        .query(&[("format", "json")])
        .header("Authorization", format!("OAuth {}", token.access_token))
        .send()
        .await
        .map_err(|e| oauth_error(StatusCode::BAD_GATEWAY, format!("Yandex user info: {e}")))?;
    let user_status = user_response.status();
    if !user_status.is_success() {
        let details = user_response.text().await.unwrap_or_default();
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            format!("Yandex user info failed ({user_status}): {details}"),
        ));
    }
    let yandex_user: YandexUserInfo = user_response.json().await.map_err(|e| {
        oauth_error(
            StatusCode::BAD_GATEWAY,
            format!("Invalid Yandex user response: {e}"),
        )
    })?;
    let has_default_email = yandex_user
        .default_email
        .as_deref()
        .is_some_and(|email| !email.trim().is_empty());
    let email_count = yandex_user.emails.as_ref().map_or(0, Vec::len);
    if !has_default_email && email_count == 0 {
        warn!(
            token_scope = token.scope.as_deref().unwrap_or("<not returned>"),
            profile_client_id = yandex_user.client_id.as_deref().unwrap_or("<not returned>"),
            has_default_email,
            email_count,
            has_avatar = yandex_user.default_avatar_id.is_some(),
            "Yandex profile response does not contain an email"
        );
    }
    let email = yandex_user
        .default_email
        .filter(|email| !email.trim().is_empty())
        .or_else(|| {
            yandex_user
                .emails
                .and_then(|emails| emails.into_iter().find(|email| !email.trim().is_empty()))
        });
    let mut first_name = normalize_profile_name(yandex_user.first_name);
    let last_name = normalize_profile_name(yandex_user.last_name);
    if first_name.is_none() && last_name.is_none() {
        first_name = normalize_profile_name(yandex_user.display_name.or(yandex_user.real_name));
    }
    let picture_url = match (
        yandex_user.is_avatar_empty.unwrap_or(true),
        yandex_user.default_avatar_id,
    ) {
        (false, Some(avatar_id)) => Some(format!(
            "https://avatars.yandex.net/get-yapic/{avatar_id}/islands-200"
        )),
        _ => None,
    };
    let login = OAuthLogin {
        provider: "yandex",
        provider_user_id: yandex_user.id,
        email,
        first_name,
        last_name,
        picture_url,
        access_token: token.access_token,
        refresh_token: token.refresh_token,
        expires_at: token_expires_at(token.expires_in),
    };
    complete_browser_oauth(
        &state,
        settings.public_registration_enabled,
        state_data,
        login,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/oauth/vk",
    tag = "auth",
    responses(
        (status = 302, description = "Redirects to VK ID"),
        (status = 403, description = "VK login is disabled"),
        (status = 500, description = "OAuth configuration error")
    )
)]
#[instrument(skip(state))]
pub async fn oauth_vk(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<OAuthStartQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let settings = server_settings::load(&state.database).await.map_err(|_| {
        oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read server settings.",
        )
    })?;
    if !settings.vk_login_enabled {
        return Err(oauth_error(StatusCode::FORBIDDEN, "VK login is disabled."));
    }

    let client_id = required_env("VK_CLIENT_ID")?;
    let redirect_url = required_env("VK_REDIRECT_URL")?;
    let base_url = get_env_with_default("VK_ID_BASE_URL", "https://id.vk.ru");
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let csrf_state = CsrfToken::new_random();
    let mut auth_url = url::Url::parse(&format!("{}/authorize", base_url.trim_end_matches('/')))
        .map_err(|e| oauth_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    auth_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &client_id)
        .append_pair("app_id", &client_id)
        .append_pair("redirect_uri", &redirect_url)
        .append_pair("scope", "email")
        .append_pair("state", csrf_state.secret())
        .append_pair("code_challenge", pkce_challenge.as_str())
        .append_pair("code_challenge_method", "S256");

    let state_data = OAuthStateData {
        provider: "vk".to_string(),
        nonce: None,
        pkce_verifier: pkce_verifier.secret().clone(),
        redirect: safe_redirect(query.redirect),
    };
    store_oauth_state(&state, csrf_state.secret(), &state_data).await?;
    Ok(Redirect::to(auth_url.as_str()))
}

#[utoipa::path(
    get,
    path = "/oauth/vk/callback",
    tag = "auth",
    responses(
        (status = 302, description = "Completes VK ID login"),
        (status = 400, description = "Invalid callback or provider response"),
        (status = 403, description = "Login or public registration is disabled")
    )
)]
#[instrument(skip(state))]
pub async fn oauth_vk_callback(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<VkOAuthCallbackQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let settings = server_settings::load(&state.database).await.map_err(|_| {
        oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read server settings.",
        )
    })?;
    if !settings.vk_login_enabled {
        return Err(oauth_error(StatusCode::FORBIDDEN, "VK login is disabled."));
    }

    let state_data = take_oauth_state(&state, &query.state, "vk").await?;
    let client_id = required_env("VK_CLIENT_ID")?;
    let redirect_url = required_env("VK_REDIRECT_URL")?;
    let base_url = get_env_with_default("VK_ID_BASE_URL", "https://id.vk.ru");
    let http = reqwest::Client::new();
    let token_url = format!("{}/oauth2/auth", base_url.trim_end_matches('/'));
    let token_response = http
        .post(token_url)
        .query(&[
            ("grant_type", "authorization_code"),
            ("redirect_uri", redirect_url.as_str()),
            ("client_id", client_id.as_str()),
            ("code_verifier", state_data.pkce_verifier.as_str()),
            ("state", query.state.as_str()),
            ("device_id", query.device_id.as_str()),
        ])
        .form(&[("code", query.code.as_str())])
        .send()
        .await
        .map_err(|e| oauth_error(StatusCode::BAD_GATEWAY, format!("VK token request: {e}")))?;
    let token_status = token_response.status();
    if !token_status.is_success() {
        let details = token_response.text().await.unwrap_or_default();
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            format!("VK token exchange failed ({token_status}): {details}"),
        ));
    }
    let token: VkTokenResponse = token_response.json().await.map_err(|e| {
        oauth_error(
            StatusCode::BAD_GATEWAY,
            format!("Invalid VK token response: {e}"),
        )
    })?;
    if token.state != query.state {
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            "VK returned an invalid OAuth state.",
        ));
    }

    let user_info_url = format!("{}/oauth2/user_info", base_url.trim_end_matches('/'));
    let user_response = http
        .post(user_info_url)
        .query(&[("client_id", client_id.as_str())])
        .form(&[("access_token", token.access_token.as_str())])
        .send()
        .await
        .map_err(|e| oauth_error(StatusCode::BAD_GATEWAY, format!("VK user info: {e}")))?;
    let user_status = user_response.status();
    if !user_status.is_success() {
        let details = user_response.text().await.unwrap_or_default();
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            format!("VK user info failed ({user_status}): {details}"),
        ));
    }
    let vk_response: VkUserInfoResponse = user_response.json().await.map_err(|e| {
        oauth_error(
            StatusCode::BAD_GATEWAY,
            format!("Invalid VK user response: {e}"),
        )
    })?;
    let provider_user_id = vk_response
        .user
        .user_id
        .as_ref()
        .and_then(json_id)
        .or_else(|| json_id(&token.user_id))
        .ok_or_else(|| oauth_error(StatusCode::BAD_REQUEST, "VK user ID not available"))?;
    let email = vk_response
        .user
        .email
        .filter(|email| !email.trim().is_empty());
    let login = OAuthLogin {
        provider: "vk",
        provider_user_id,
        email,
        first_name: normalize_profile_name(vk_response.user.first_name),
        last_name: normalize_profile_name(vk_response.user.last_name),
        picture_url: vk_response.user.avatar,
        access_token: token.access_token,
        refresh_token: token.refresh_token,
        expires_at: token_expires_at(token.expires_in),
    };
    complete_browser_oauth(
        &state,
        settings.public_registration_enabled,
        state_data,
        login,
    )
    .await
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

    let token = crate::utils::auth::encode_jwt(user.id.to_string()).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;
    Ok(Json(json!({ "token": token })).into_response())
}

#[cfg(test)]
mod tests {
    use super::{
        append_totp_challenge, json_id, safe_redirect_for_origin, VkTokenResponse,
        VkUserInfoResponse, YandexUserInfo,
    };

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

    #[test]
    fn oauth_redirect_must_use_the_exact_frontend_origin() {
        let origin = "https://app.example";
        assert_eq!(
            safe_redirect_for_origin(Some("/boards?selected=1"), origin),
            "https://app.example/boards?selected=1"
        );
        assert_eq!(
            safe_redirect_for_origin(Some("https://app.example/login"), origin),
            "https://app.example/login"
        );
        assert_eq!(
            safe_redirect_for_origin(Some("https://app.example.evil.test/login"), origin),
            "https://app.example/"
        );
        assert_eq!(
            safe_redirect_for_origin(Some("//evil.test/login"), origin),
            "https://app.example/"
        );
        assert_eq!(
            safe_redirect_for_origin(Some("/\\evil.test/login"), origin),
            "https://app.example/"
        );
    }

    #[test]
    fn yandex_user_info_supports_email_and_avatar_fields() {
        let user: YandexUserInfo = serde_json::from_value(serde_json::json!({
            "id": "1000034426",
            "first_name": "Roman",
            "last_name": "Black",
            "display_name": "Roman Black",
            "default_email": "user@yandex.ru",
            "emails": ["user@yandex.ru"],
            "is_avatar_empty": false,
            "default_avatar_id": "131652443"
        }))
        .unwrap();
        assert_eq!(user.id, "1000034426");
        assert_eq!(user.first_name.as_deref(), Some("Roman"));
        assert_eq!(user.last_name.as_deref(), Some("Black"));
        assert_eq!(user.default_email.as_deref(), Some("user@yandex.ru"));
        assert_eq!(user.default_avatar_id.as_deref(), Some("131652443"));
    }

    #[test]
    fn yandex_user_info_allows_missing_email() {
        let user: YandexUserInfo = serde_json::from_value(serde_json::json!({
            "id": "1000034426",
            "client_id": "client-id",
            "is_avatar_empty": false,
            "default_avatar_id": "131652443"
        }))
        .unwrap();

        assert!(user.default_email.is_none());
        assert!(user.emails.is_none());
    }

    #[test]
    fn vk_responses_support_numeric_and_string_user_ids() {
        let token: VkTokenResponse = serde_json::from_value(serde_json::json!({
            "access_token": "access",
            "refresh_token": "refresh",
            "expires_in": 3600,
            "state": "state",
            "user_id": 42
        }))
        .unwrap();
        assert_eq!(json_id(&token.user_id).as_deref(), Some("42"));

        let user: VkUserInfoResponse = serde_json::from_value(serde_json::json!({
            "user": {
                "user_id": "42",
                "email": "user@example.com",
                "avatar": "https://sun.example/avatar.jpg"
            }
        }))
        .unwrap();
        assert_eq!(
            user.user.user_id.as_ref().and_then(json_id).as_deref(),
            Some("42")
        );
        assert_eq!(user.user.email.as_deref(), Some("user@example.com"));
    }
}
