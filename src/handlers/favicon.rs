//! Link favicon resolver + durable S3 cache.
//!
//! Browsers can't read a cross-origin site's `/favicon.ico` (the response is
//! opaque under CORS), so the web client's link badges never showed a favicon.
//! This endpoint moves the fetch server-side (no CORS there): given a page URL it
//! resolves the site's best icon, normalizes it to a 64×64 PNG, stores it in S3
//! keyed by domain (`favicons/<domain>.png`), and 302-redirects to a presigned
//! URL. The stored object is shared across every link to the same domain and
//! survives the site later changing/removing its icon.
//!
//! Resolution order (first that decodes wins):
//!   1. `<link rel="...icon...">` / `apple-touch-icon` parsed from the page HTML.
//!   2. `<origin>/favicon.ico`.
//!   3. Google's favicon service (`s2/favicons`) as a reliable last resort.
//!
//! Public + unauthenticated: it reveals no board content and must work on
//! embed/public boards where the viewer has no session.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Redirect};
use serde::Deserialize;

use crate::core::config::get_env_with_default;
use crate::routes::AppState;
use crate::storage::presign_url::generate_presigned_url;
use crate::storage::upload::upload_to_storage;

/// Normalized favicon side length in pixels.
const FAVICON_PX: u32 = 64;
/// Presigned-URL lifetime for the redirect (long — the object is immutable).
const PRESIGN_TTL_SECS: u64 = 60 * 60 * 24 * 7;
/// Cap on fetched bytes (HTML page or icon) to avoid abuse.
const MAX_FETCH_BYTES: usize = 2 * 1024 * 1024;
/// Per-request network timeout.
const FETCH_TIMEOUT_SECS: u64 = 8;
/// Browser-like UA — some sites 403 the default reqwest agent.
const USER_AGENT: &str = "Mozilla/5.0 (compatible; NezumoFaviconBot/1.0; +https://nezumo.ru)";

#[derive(Debug, Deserialize)]
pub struct FaviconQuery {
    /// The external page URL whose favicon we want.
    pub url: String,
}

/// `GET /link/favicon?url=<page>` — resolve, cache, and redirect to the favicon.
pub async fn get_link_favicon(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FaviconQuery>,
) -> impl IntoResponse {
    let parsed = match url::Url::parse(params.url.trim()) {
        Ok(u) if matches!(u.scheme(), "http" | "https") => u,
        _ => return (StatusCode::BAD_REQUEST, "invalid url").into_response(),
    };
    let Some(domain) = parsed.host_str().map(|h| h.to_ascii_lowercase()) else {
        return (StatusCode::BAD_REQUEST, "url has no host").into_response();
    };

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let key = format!("favicons/{}.png", sanitize_domain(&domain));

    // Cache hit: object already in S3 → redirect to a fresh presigned URL.
    if object_exists(&state, &bucket, &key).await {
        return presigned_redirect(&state, &bucket, &key).await;
    }

    // Cache miss: resolve the icon, normalize, and store.
    match resolve_favicon_png(&parsed, &domain).await {
        Some(png) => {
            if let Err(e) = upload_to_storage(&state.storage, &bucket, &key, &png).await {
                tracing::warn!("favicon upload failed for {domain}: {e}");
                // Still serve it inline this time even if the cache write failed.
                return png_response(png);
            }
            presigned_redirect(&state, &bucket, &key).await
        }
        None => (StatusCode::NOT_FOUND, "favicon not found").into_response(),
    }
}

/// Whether `key` already exists in the bucket (HEAD, no body transfer).
async fn object_exists(state: &AppState, bucket: &str, key: &str) -> bool {
    state
        .storage
        .client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .is_ok()
}

/// 302 to a presigned URL for the cached object, or 500 if presigning fails.
async fn presigned_redirect(state: &AppState, bucket: &str, key: &str) -> axum::response::Response {
    match generate_presigned_url(&state.storage, bucket, key, PRESIGN_TTL_SECS).await {
        Ok(url) => Redirect::temporary(&url).into_response(),
        Err(e) => {
            tracing::warn!("favicon presign failed for {key}: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "presign failed").into_response()
        }
    }
}

/// Inline PNG response with a long immutable cache (used as an upload-failure
/// fallback so the badge still gets pixels).
fn png_response(png: Vec<u8>) -> axum::response::Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        png,
    )
        .into_response()
}

/// Try each candidate source in order; return the first that decodes to a PNG.
async fn resolve_favicon_png(page: &url::Url, domain: &str) -> Option<Vec<u8>> {
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .ok()?;

    for candidate in candidate_icon_urls(&client, page, domain).await {
        if let Some(png) = fetch_and_normalize(&client, &candidate).await {
            return Some(png);
        }
    }
    None
}

/// Build an ordered list of icon URLs to try: HTML-declared icons first, then the
/// well-known `/favicon.ico`, then Google's favicon service as a last resort.
async fn candidate_icon_urls(
    client: &reqwest::Client,
    page: &url::Url,
    domain: &str,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();

    // 1. Parse the page HTML for <link rel="...icon..."> hrefs.
    if let Ok(resp) = client.get(page.clone()).send().await {
        if resp.status().is_success() {
            if let Some(html) = read_capped_text(resp).await {
                for href in parse_icon_hrefs(&html) {
                    if let Ok(abs) = page.join(&href) {
                        out.push(abs.to_string());
                    }
                }
            }
        }
    }

    // 2. Conventional /favicon.ico at the origin.
    if let Ok(origin_fav) = page.join("/favicon.ico") {
        out.push(origin_fav.to_string());
    }

    // 3. Google favicon service — reliable PNG for almost any domain.
    out.push(format!(
        "https://www.google.com/s2/favicons?sz={FAVICON_PX}&domain={domain}"
    ));

    out.dedup();
    out
}

/// Fetch an icon URL and normalize whatever we get (ICO/PNG/JPEG/WEBP/GIF) into a
/// square `FAVICON_PX` PNG. Returns None on any fetch/decode failure.
async fn fetch_and_normalize(client: &reqwest::Client, url: &str) -> Option<Vec<u8>> {
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let bytes = read_capped_bytes(resp).await?;
    if bytes.is_empty() {
        return None;
    }
    let img = image::load_from_memory(&bytes).ok()?;
    let resized = img.resize_to_fill(
        FAVICON_PX,
        FAVICON_PX,
        image::imageops::FilterType::Lanczos3,
    );
    let mut png = std::io::Cursor::new(Vec::new());
    resized.write_to(&mut png, image::ImageFormat::Png).ok()?;
    Some(png.into_inner())
}

/// Extract `href`s from `<link>` tags whose `rel` contains "icon"
/// (`icon`, `shortcut icon`, `apple-touch-icon`, `mask-icon`, …).
fn parse_icon_hrefs(html: &str) -> Vec<String> {
    // Only scan the <head> region to keep the regex cheap on large pages.
    let head = match html.to_ascii_lowercase().find("</head>") {
        Some(end) => &html[..end.min(html.len())],
        None => html,
    };
    let mut hrefs = Vec::new();
    // Match each <link ...> tag, then require rel~="icon" and pull its href.
    let link_re = regex::Regex::new(r#"(?is)<link\b[^>]*>"#).unwrap();
    let rel_re = regex::Regex::new(r#"(?is)\brel\s*=\s*["']?([^"'>]*)"#).unwrap();
    let href_re = regex::Regex::new(r#"(?is)\bhref\s*=\s*["']([^"']+)["']"#).unwrap();
    for tag in link_re.find_iter(head) {
        let tag = tag.as_str();
        let is_icon = rel_re
            .captures(tag)
            .map(|c| c[1].to_ascii_lowercase().contains("icon"))
            .unwrap_or(false);
        if !is_icon {
            continue;
        }
        if let Some(h) = href_re.captures(tag) {
            hrefs.push(h[1].trim().to_string());
        }
    }
    hrefs
}

/// Read a response body as UTF-8 text, capped at `MAX_FETCH_BYTES`.
async fn read_capped_text(resp: reqwest::Response) -> Option<String> {
    let bytes = read_capped_bytes(resp).await?;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// Read a response body into memory, capped at `MAX_FETCH_BYTES`.
async fn read_capped_bytes(resp: reqwest::Response) -> Option<Vec<u8>> {
    let bytes = resp.bytes().await.ok()?;
    if bytes.len() > MAX_FETCH_BYTES {
        return None;
    }
    Some(bytes.to_vec())
}

/// Make a domain safe as an S3 key segment (strip anything unexpected).
fn sanitize_domain(domain: &str) -> String {
    domain
        .chars()
        .map(|c| match c {
            'a'..='z' | '0'..='9' | '.' | '-' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_various_icon_link_tags() {
        let html = r##"
            <html><head>
              <link rel="stylesheet" href="/app.css">
              <link rel="icon" type="image/png" href="/favicon-32.png">
              <link rel="shortcut icon" href="https://cdn.example.com/fav.ico">
              <link rel="apple-touch-icon" sizes="180x180" href="/touch.png">
              <link rel="mask-icon" href="/mask.svg" color="#000">
              <title>x</title>
            </head><body>
              <link rel="icon" href="/should-still-be-found-if-in-head.png">
            </body></html>
        "##;
        let hrefs = parse_icon_hrefs(html);
        assert!(hrefs.contains(&"/favicon-32.png".to_string()));
        assert!(hrefs.contains(&"https://cdn.example.com/fav.ico".to_string()));
        assert!(hrefs.contains(&"/touch.png".to_string()));
        assert!(hrefs.contains(&"/mask.svg".to_string()));
        // The stylesheet is not an icon.
        assert!(!hrefs.iter().any(|h| h == "/app.css"));
    }

    #[test]
    fn sanitizes_domain_for_s3_key() {
        // The caller lowercases the host before calling this; it only guards the
        // S3-key charset.
        assert_eq!(sanitize_domain("sub.example.co.uk"), "sub.example.co.uk");
        assert_eq!(sanitize_domain("evil/../key"), "evil_.._key");
    }

    // Opt-in network test: `cargo test favicon_live -- --ignored --nocapture`.
    // Verifies real-world resolution (HTML parse → icon fetch → PNG normalize).
    #[tokio::test]
    #[ignore]
    async fn favicon_live() {
        for site in ["https://github.com", "https://news.ycombinator.com"] {
            let url = url::Url::parse(site).unwrap();
            let domain = url.host_str().unwrap().to_string();
            let png = resolve_favicon_png(&url, &domain).await;
            match png {
                Some(bytes) => {
                    // Must be a valid PNG of the expected size.
                    let img = image::load_from_memory(&bytes).expect("valid png");
                    assert_eq!((img.width(), img.height()), (FAVICON_PX, FAVICON_PX));
                    eprintln!(
                        "{site}: {} bytes, {}x{}",
                        bytes.len(),
                        img.width(),
                        img.height()
                    );
                }
                None => panic!("{site}: no favicon resolved"),
            }
        }
    }
}
