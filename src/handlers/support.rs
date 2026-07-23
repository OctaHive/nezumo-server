//! `POST /support/reports` — "report a problem" submissions.
//!
//! Accepts a multipart form (text fields + diagnostics JSON + optional
//! attachments), stores any attachments in S3, and opens an issue in the
//! configured GitHub repository. Attachments are NOT presigned (those expire);
//! instead each gets a permanent, signed proxy URL served by
//! [`get_support_attachment`] (`GET /support/attachments/...`). The signature is
//! a JWT (signed with `JWT_SECRET_KEY`) binding the URL to the exact S3 key, so
//! the links are unguessable but need no GitHub/login session — GitHub's image
//! proxy and maintainers can fetch them directly.
//!
//! Required env: `GITHUB_ISSUES_REPO` (`owner/repo`), `GITHUB_ISSUES_TOKEN`
//! (token with `issues:write`), and `PUBLIC_BASE_URL` (e.g.
//! `https://app.nezumo.ru`) so attachment URLs are absolute. Optional
//! `GITHUB_ISSUES_LABELS` (comma-separated, default `support`).

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Extension, Multipart, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::Client as S3Client;
use deadpool_redis::redis::AsyncCommands;
use tokio::sync::OnceCell;

use crate::core::config::{get_env, get_env_with_default};
use crate::database::server_settings;
use crate::models::user::User;
use crate::routes::AppState;
use crate::storage::download::download_from_storage;
use crate::storage::upload::upload_to_storage;
use crate::storage::StorageState;

type ApiError = (StatusCode, Json<Value>);

fn err(status: StatusCode, msg: &str) -> ApiError {
    (status, Json(json!({ "error": msg })))
}

const MAX_ATTACHMENTS: usize = 5;
const MAX_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;
/// Attachment proxy tokens are effectively permanent (10 years).
const ATTACHMENT_TOKEN_TTL_SECS: i64 = 10 * 365 * 24 * 60 * 60;

/// JWT payload binding an attachment proxy URL to one S3 object key.
#[derive(Serialize, Deserialize)]
struct AttachmentClaims {
    /// Full S3 object key, e.g. `support/<uuid>/<file>`.
    key: String,
    exp: usize,
}

struct Attachment {
    filename: String,
    content_type: String,
    bytes: Vec<u8>,
}

/// S3 bucket for support attachments: dedicated `STORAGE_BUCKET_SUPPORT` if set,
/// otherwise the shared board-files bucket.
fn support_bucket() -> String {
    let dedicated = get_env_with_default("STORAGE_BUCKET_SUPPORT", "");
    if !dedicated.trim().is_empty() {
        dedicated.trim().to_string()
    } else {
        get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files")
    }
}

/// Cached dedicated storage client for the support bucket (`None` = reuse main).
static SUPPORT_STORAGE: OnceCell<Option<StorageState>> = OnceCell::const_new();

/// Storage client for support attachments. If `STORAGE_SUPPORT_ACCESS_KEY` and
/// `STORAGE_SUPPORT_SECRET_KEY` are set, a dedicated S3 client is built once for
/// the support bucket (which may live on separate credentials / host). Host,
/// port and region fall back to the main `STORAGE_*` values when the
/// `STORAGE_SUPPORT_*` overrides are absent. Otherwise the app's main storage
/// client is reused.
async fn support_storage(default: &StorageState) -> StorageState {
    let built = SUPPORT_STORAGE.get_or_init(build_support_storage).await;
    built.clone().unwrap_or_else(|| default.clone())
}

async fn build_support_storage() -> Option<StorageState> {
    let access_key = get_env_with_default("STORAGE_SUPPORT_ACCESS_KEY", "");
    let secret_key = get_env_with_default("STORAGE_SUPPORT_SECRET_KEY", "");
    if access_key.trim().is_empty() || secret_key.trim().is_empty() {
        return None; // No dedicated creds → reuse the main storage client.
    }

    let host = get_env_with_default(
        "STORAGE_SUPPORT_HOST",
        &get_env_with_default("STORAGE_HOST", ""),
    );
    let port = get_env_with_default(
        "STORAGE_SUPPORT_PORT",
        &get_env_with_default("STORAGE_PORT", "9000"),
    );
    let region = get_env_with_default(
        "STORAGE_SUPPORT_REGION",
        &get_env_with_default("STORAGE_REGION", "us-east-1"),
    );

    let endpoint = if port.is_empty() || port == "443" || port == "80" {
        host.trim_end_matches('/').to_string()
    } else {
        format!("{}:{}", host.trim_end_matches('/'), port)
    };

    let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.clone()))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
        .region(Region::new(region))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            access_key.trim(),
            secret_key.trim(),
            None,
            None,
            "support",
        ))
        .build();

    Some(StorageState {
        client: S3Client::from_conf(s3_config),
        endpoint_url: endpoint,
    })
}

/// `(display name, absolute proxy url, is_image)` for an uploaded attachment.
type AttachmentLink = (String, String, bool);

pub async fn post_support_report(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, ApiError> {
    let runtime_settings = server_settings::load(&state.database).await.map_err(|_| {
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read server settings.",
        )
    })?;
    // Enforced before parsing the (potentially large) body: don't even read the
    // upload if the user is over their daily quota.
    rate_limit_check(
        &state.cache,
        &user.id,
        runtime_settings.support_max_reports_per_day,
    )
    .await?;

    let mut source = String::from("web");
    let mut what_did_you_do = String::new();
    let mut expected = String::new();
    let mut actual = String::new();
    let mut board_url: Option<String> = None;
    let mut context_json: Option<String> = None;
    let mut attachments: Vec<Attachment> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| err(StatusCode::BAD_REQUEST, "Malformed upload."))?
    {
        match field.name() {
            Some("source") => source = field.text().await.unwrap_or_default(),
            Some("what_did_you_do") => what_did_you_do = field.text().await.unwrap_or_default(),
            Some("expected") => expected = field.text().await.unwrap_or_default(),
            Some("actual") => actual = field.text().await.unwrap_or_default(),
            Some("board_url") => {
                board_url = field.text().await.ok().filter(|s| !s.trim().is_empty());
            }
            Some("context_json") => context_json = field.text().await.ok(),
            Some("attachments") => {
                let filename = field.file_name().unwrap_or("attachment").to_string();
                let content_type = field
                    .content_type()
                    .unwrap_or("application/octet-stream")
                    .to_string();
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|_| err(StatusCode::BAD_REQUEST, "Could not read attachment."))?;
                if bytes.is_empty() || attachments.len() >= MAX_ATTACHMENTS {
                    continue;
                }
                if bytes.len() > MAX_ATTACHMENT_BYTES {
                    return Err(err(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "Attachment too large (max 25 MB each).",
                    ));
                }
                attachments.push(Attachment {
                    filename,
                    content_type,
                    bytes: bytes.to_vec(),
                });
            }
            // include_board_link and any unknown fields are ignored.
            _ => {
                let _ = field.bytes().await;
            }
        }
    }

    if what_did_you_do.trim().is_empty() && expected.trim().is_empty() && actual.trim().is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "Empty report."));
    }

    let repo = get_env_with_default("GITHUB_ISSUES_REPO", "");
    let token = get_env_with_default("GITHUB_ISSUES_TOKEN", "");
    if repo.trim().is_empty() || token.trim().is_empty() {
        tracing::error!("Support report received but GITHUB_ISSUES_REPO/TOKEN are not configured");
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "Issue reporting is not configured.",
        ));
    }

    // Store attachments in S3 and mint a permanent signed proxy URL for each.
    let bucket = support_bucket();
    let storage = support_storage(&state.storage).await;
    let base = public_base_url(&headers);
    if !attachments.is_empty() && base.is_none() {
        tracing::warn!("PUBLIC_BASE_URL unset and no Origin header — attachments will be omitted");
    }
    let report_id = Uuid::new_v4();
    let mut attachment_links: Vec<AttachmentLink> = Vec::new();
    for att in &attachments {
        let Some(base) = base.as_deref() else { break };
        let safe_name = sanitize_filename(&att.filename);
        let key = format!("support/{report_id}/{safe_name}");
        if let Err(e) = upload_to_storage(&storage, &bucket, &key, &att.bytes).await {
            tracing::warn!("Support attachment upload failed ({}): {e}", att.filename);
            continue;
        }
        match attachment_proxy_url(base, &report_id.to_string(), &safe_name, &key) {
            Some(url) => attachment_links.push((
                att.filename.clone(),
                url,
                att.content_type.starts_with("image/"),
            )),
            None => tracing::warn!("Could not sign support attachment URL"),
        }
    }

    let title = build_title(&what_did_you_do, &actual);
    let body = build_body(
        Some(&user),
        &source,
        &what_did_you_do,
        &expected,
        &actual,
        board_url.as_deref(),
        context_json.as_deref(),
        &attachment_links,
    );

    let labels = issue_labels("GITHUB_ISSUES_LABELS", "support");
    let issue_url = create_github_issue(&repo, &token, &title, &body, labels)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create GitHub issue: {e}");
            err(StatusCode::BAD_GATEWAY, "Could not create the issue.")
        })?;

    // Only count a successful report against the daily quota.
    rate_limit_record(
        &state.cache,
        &user.id,
        runtime_settings.support_max_reports_per_day,
    )
    .await;

    Ok(Json(json!({ "issueUrl": issue_url })))
}

#[derive(Deserialize)]
pub struct FeatureRequestBody {
    title: String,
    description: String,
    contact: Option<String>,
    source: Option<String>,
    page_url: Option<String>,
    company: Option<String>,
}

/// `POST /support/feature-requests` — public website form that opens a GitHub
/// issue tagged as a feature request. It is intentionally separate from
/// authenticated bug reports from inside the board.
pub async fn post_feature_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<FeatureRequestBody>,
) -> Result<impl IntoResponse, ApiError> {
    let runtime_settings = server_settings::load(&state.database).await.map_err(|_| {
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read server settings.",
        )
    })?;
    if input.company.as_deref().unwrap_or("").trim().len() > 0 {
        return Ok(Json(json!({ "ok": true })));
    }

    let title_text = input.title.trim();
    let description = input.description.trim();
    if title_text.is_empty() || description.is_empty() {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "Title and description are required.",
        ));
    }
    if title_text.chars().count() > 140 || description.chars().count() > 4000 {
        return Err(err(StatusCode::BAD_REQUEST, "Request is too long."));
    }

    let reporter_key = feature_requester_key(&headers);
    feature_rate_limit_check(
        &state.cache,
        &reporter_key,
        runtime_settings.feature_request_max_per_day,
    )
    .await?;

    let repo = get_env_with_default("GITHUB_ISSUES_REPO", "");
    let token = get_env_with_default("GITHUB_ISSUES_TOKEN", "");
    if repo.trim().is_empty() || token.trim().is_empty() {
        tracing::error!("Feature request received but GITHUB_ISSUES_REPO/TOKEN are not configured");
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "Feature requests are not configured.",
        ));
    }

    let title = build_feature_title(title_text);
    let body = build_feature_body(
        description,
        input.contact.as_deref(),
        input.source.as_deref(),
        input.page_url.as_deref(),
    );
    let labels = issue_labels("GITHUB_FEATURE_REQUEST_LABELS", "feature request");
    let issue_url = create_github_issue(&repo, &token, &title, &body, labels)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create GitHub feature request issue: {e}");
            err(StatusCode::BAD_GATEWAY, "Could not create the issue.")
        })?;

    feature_rate_limit_record(
        &state.cache,
        &reporter_key,
        runtime_settings.feature_request_max_per_day,
    )
    .await;

    if runtime_settings.feature_request_expose_issue_url {
        Ok(Json(json!({ "ok": true, "issueUrl": issue_url })))
    } else {
        Ok(Json(json!({ "ok": true })))
    }
}

fn rate_limit_key(user_id: &Uuid) -> String {
    let day = chrono::Utc::now().format("%Y%m%d");
    format!("support:rate:{user_id}:{day}")
}

fn feature_rate_limit_key(reporter_key: &str) -> String {
    let day = chrono::Utc::now().format("%Y%m%d");
    format!("feature-request:rate:{reporter_key}:{day}")
}

fn feature_requester_key(headers: &HeaderMap) -> String {
    let raw = headers
        .get("cf-connecting-ip")
        .or_else(|| headers.get("x-real-ip"))
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");
    raw.split(',')
        .next()
        .unwrap_or("unknown")
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | ':' | '-' | '_'))
        .take(80)
        .collect::<String>()
}

async fn feature_rate_limit_check(
    cache: &deadpool_redis::Pool,
    reporter_key: &str,
    limit: i64,
) -> Result<(), ApiError> {
    if limit <= 0 {
        return Ok(());
    }
    let mut conn = match cache.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("feature request rate-limit: redis unavailable, allowing: {e}");
            return Ok(());
        }
    };
    let count: i64 = conn
        .get(feature_rate_limit_key(reporter_key))
        .await
        .unwrap_or(0);
    if count >= limit {
        return Err(err(
            StatusCode::TOO_MANY_REQUESTS,
            "Daily feature request limit reached. Please try again tomorrow.",
        ));
    }
    Ok(())
}

async fn feature_rate_limit_record(cache: &deadpool_redis::Pool, reporter_key: &str, limit: i64) {
    if limit <= 0 {
        return;
    }
    let Ok(mut conn) = cache.get().await else {
        return;
    };
    let key = feature_rate_limit_key(reporter_key);
    match conn.incr::<_, _, i64>(&key, 1).await {
        Ok(1) => {
            let _: Result<(), _> = conn.expire(&key, 24 * 60 * 60).await;
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("feature request rate-limit incr failed: {e}"),
    }
}

/// Reject if the user already reached today's limit (does not increment).
async fn rate_limit_check(
    cache: &deadpool_redis::Pool,
    user_id: &Uuid,
    limit: i64,
) -> Result<(), ApiError> {
    if limit <= 0 {
        return Ok(());
    }
    let mut conn = match cache.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("support rate-limit: redis unavailable, allowing: {e}");
            return Ok(());
        }
    };
    let count: i64 = conn.get(rate_limit_key(user_id)).await.unwrap_or(0);
    if count >= limit {
        return Err(err(
            StatusCode::TOO_MANY_REQUESTS,
            "Daily report limit reached. Please try again tomorrow.",
        ));
    }
    Ok(())
}

/// Record one successful report; sets a 24h TTL on the first of the day.
async fn rate_limit_record(cache: &deadpool_redis::Pool, user_id: &Uuid, limit: i64) {
    if limit <= 0 {
        return;
    }
    let Ok(mut conn) = cache.get().await else {
        return;
    };
    let key = rate_limit_key(user_id);
    match conn.incr::<_, _, i64>(&key, 1).await {
        Ok(1) => {
            let _: Result<(), _> = conn.expire(&key, 24 * 60 * 60).await;
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("support rate-limit incr failed: {e}"),
    }
}

#[derive(Deserialize)]
pub struct AttachmentQuery {
    /// File name carried in the query (NOT the path) so the URL doesn't end in an
    /// asset extension — otherwise a static-file location in a fronting nginx/CDN
    /// (`location ~* \.(png|jpg|...)$`) intercepts it before the `/api` proxy.
    file: String,
    t: String,
}

/// `GET /support/attachments/{report_id}?file=<name>&t=<jwt>` — stream a stored
/// support attachment. No login needed: the `t` JWT (signed with the server
/// secret and bound to the exact S3 key) is the access control, so GitHub's
/// image proxy and maintainers can open the link directly.
pub async fn get_support_attachment(
    State(state): State<Arc<AppState>>,
    Path(report_id): Path<String>,
    Query(query): Query<AttachmentQuery>,
) -> Result<Response, ApiError> {
    // Reconstruct the exact key and require the token to be bound to it.
    let safe_name = sanitize_filename(&query.file);
    if !is_uuid(&report_id) || safe_name != query.file {
        return Err(err(StatusCode::NOT_FOUND, "Not found."));
    }
    let key = format!("support/{report_id}/{safe_name}");

    let secret = get_env("JWT_SECRET_KEY");
    // The app's normal JWTs carry an audience; ours intentionally does not, so
    // disable audience validation (otherwise the token is rejected → broken
    // image in the issue). The signature + key binding are the real check.
    let mut validation = Validation::default();
    validation.validate_aud = false;
    let data = decode::<AttachmentClaims>(
        &query.t,
        &DecodingKey::from_secret(secret.as_ref()),
        &validation,
    )
    .map_err(|_| err(StatusCode::FORBIDDEN, "Invalid or expired link."))?;
    if data.claims.key != key {
        return Err(err(StatusCode::FORBIDDEN, "Invalid link."));
    }

    let bucket = support_bucket();
    let storage = support_storage(&state.storage).await;
    let bytes = download_from_storage(&storage, &bucket, &key)
        .await
        .map_err(|_| err(StatusCode::NOT_FOUND, "Attachment not found."))?;

    Response::builder()
        .header(header::CONTENT_TYPE, content_type_for(&safe_name))
        .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .body(Body::from(bytes))
        .map_err(|_| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not serve attachment.",
            )
        })
}

/// Sign and assemble the permanent proxy URL for one attachment.
fn attachment_proxy_url(base: &str, report_id: &str, safe_name: &str, key: &str) -> Option<String> {
    let exp = (chrono::Utc::now().timestamp() + ATTACHMENT_TOKEN_TTL_SECS) as usize;
    let claims = AttachmentClaims {
        key: key.to_string(),
        exp,
    };
    let secret = get_env("JWT_SECRET_KEY");
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_ref()),
    )
    .ok()?;
    Some(format!(
        "{base}/api/v1/support/attachments/{report_id}?file={safe_name}&t={token}"
    ))
}

/// Absolute public origin for building attachment URLs: `PUBLIC_BASE_URL` if set,
/// else the request's forwarded/Origin header.
fn public_base_url(headers: &HeaderMap) -> Option<String> {
    let env = get_env_with_default("PUBLIC_BASE_URL", "");
    if !env.trim().is_empty() {
        return Some(env.trim().trim_end_matches('/').to_string());
    }
    for name in ["x-forwarded-origin", "origin"] {
        if let Some(v) = headers.get(name).and_then(|v| v.to_str().ok()) {
            let v = v.trim().trim_end_matches('/');
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok()
}

fn content_type_for(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    match lower.rsplit('.').next().unwrap_or("") {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        _ => "application/octet-stream",
    }
}

/// POST the issue to GitHub. Returns the issue's `html_url`.
async fn create_github_issue(
    repo: &str,
    token: &str,
    title: &str,
    body: &str,
    labels: Vec<String>,
) -> Result<String, String> {
    let url = format!("https://api.github.com/repos/{}/issues", repo.trim());
    let payload = json!({ "title": title, "body": body, "labels": labels });

    let resp = reqwest::Client::new()
        .post(&url)
        .header("Authorization", format!("Bearer {}", token.trim()))
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", "nezumo-support")
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("request error: {e}"))?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("GitHub API {status}: {text}"));
    }

    serde_json::from_str::<Value>(&text)
        .ok()
        .as_ref()
        .and_then(|v| v.get("html_url"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "GitHub response missing html_url".to_string())
}

fn issue_labels(env_key: &str, default: &str) -> Vec<String> {
    get_env_with_default(env_key, default)
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// One-line issue title derived from the most descriptive field, truncated.
fn build_title(what_did_you_do: &str, actual: &str) -> String {
    let base = if !actual.trim().is_empty() {
        actual.trim()
    } else {
        what_did_you_do.trim()
    };
    let first_line = base.lines().next().unwrap_or("Проблема").trim();
    let mut title: String = first_line.chars().take(80).collect();
    if first_line.chars().count() > 80 {
        title.push('…');
    }
    if title.is_empty() {
        title = "Сообщение о проблеме".to_string();
    }
    format!("[Report] {title}")
}

fn build_feature_title(title: &str) -> String {
    let first_line = title.lines().next().unwrap_or("Feature request").trim();
    let mut title: String = first_line.chars().take(90).collect();
    if first_line.chars().count() > 90 {
        title.push('…');
    }
    if title.is_empty() {
        title = "Feature request".to_string();
    }
    format!("[Feature] {title}")
}

fn build_feature_body(
    description: &str,
    contact: Option<&str>,
    source: Option<&str>,
    page_url: Option<&str>,
) -> String {
    let mut s = String::new();
    s.push_str("**Source:** ");
    s.push_str(source.unwrap_or("website").trim());
    s.push('\n');
    if let Some(contact) = contact.map(str::trim).filter(|v| !v.is_empty()) {
        s.push_str(&format!("**Contact:** {contact}\n"));
    }
    if let Some(page_url) = page_url.map(str::trim).filter(|v| !v.is_empty()) {
        s.push_str(&format!("**Page:** {page_url}\n"));
    }
    s.push_str("\n### Запрос\n");
    s.push_str(description);
    s.push('\n');
    s
}

#[allow(clippy::too_many_arguments)]
fn build_body(
    user: Option<&User>,
    source: &str,
    what_did_you_do: &str,
    expected: &str,
    actual: &str,
    board_url: Option<&str>,
    context_json: Option<&str>,
    attachments: &[AttachmentLink],
) -> String {
    let mut s = String::new();

    let reporter = match user {
        Some(u) => format!("{} (`{}`)", u.email, u.id),
        None => "anonymous".to_string(),
    };
    s.push_str(&format!("**Reporter:** {reporter}\n"));
    s.push_str(&format!("**Source:** {}\n", source.trim()));
    if let Some(url) = board_url {
        s.push_str(&format!("**Board:** {url}\n"));
    }
    s.push('\n');

    let section = |s: &mut String, heading: &str, value: &str| {
        let value = value.trim();
        if !value.is_empty() {
            s.push_str(&format!("### {heading}\n{value}\n\n"));
        }
    };
    section(&mut s, "Что делал", what_did_you_do);
    section(&mut s, "Ожидаемо", expected);
    section(&mut s, "Фактически", actual);

    if !attachments.is_empty() {
        s.push_str("### Вложения\n");
        for (name, url, is_image) in attachments {
            // Keep brackets out of the markdown link/alt text.
            let label = name.replace(['[', ']'], " ");
            if *is_image {
                s.push_str(&format!("![{label}]({url})\n\n"));
            } else {
                s.push_str(&format!("- [{label}]({url})\n"));
            }
        }
        s.push('\n');
    }

    if let Some(ctx) = context_json {
        let ctx = ctx.trim();
        if !ctx.is_empty() {
            let pretty = serde_json::from_str::<Value>(ctx)
                .ok()
                .and_then(|v| serde_json::to_string_pretty(&v).ok())
                .unwrap_or_else(|| ctx.to_string());
            s.push_str("<details><summary>Диагностика</summary>\n\n```json\n");
            s.push_str(&pretty.replace("```", "ʼʼʼ"));
            s.push_str("\n```\n\n</details>\n");
        }
    }

    s
}

/// Keep only filesystem/URL-safe characters for the S3 object key.
fn sanitize_filename(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('.').to_string();
    if trimmed.is_empty() {
        "attachment".to_string()
    } else {
        trimmed
    }
}
