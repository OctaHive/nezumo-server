//! Same-origin proxy for one-shot website preview screenshots.
//!
//! The browser persists the returned image into board storage immediately. The
//! proxy keeps third-party screenshot traffic out of the application's
//! `connect-src` policy and never becomes a render-loop dependency.

use std::time::Duration;

use axum::body::Body;
use axum::extract::Query;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::core::config::get_env_with_default;
use crate::handlers::embed_policy::parse_public_http_url;

const DEFAULT_SCREENSHOT_BASE: &str =
    "https://image.thum.io/get/width/1280/crop/720/wait/5/noanimate/";
const FETCH_TIMEOUT_SECS: u64 = 35;
const MAX_PREVIEW_BYTES: u64 = 8 * 1024 * 1024;
const USER_AGENT: &str = "Mozilla/5.0 (compatible; NezumoSitePreviewBot/1.0; +https://nezumo.ru)";

#[derive(Debug, Deserialize)]
pub struct SitePreviewQuery {
    pub url: String,
}

/// `GET /link/site-preview?url=<page>` — fetch a screenshot through the
/// configured provider and return the image from the Nezumo API origin.
pub async fn get_link_site_preview(Query(params): Query<SitePreviewQuery>) -> Response {
    let target = match parse_public_http_url(params.url.trim()) {
        Ok(url) => url,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    let base = get_env_with_default("SITE_SCREENSHOT_URL", DEFAULT_SCREENSHOT_BASE);
    let provider_url = match screenshot_provider_url(&base, &target) {
        Ok(url) => url,
        Err(reason) => return (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response(),
    };

    let client = match reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
        .build()
    {
        Ok(client) => client,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "client_error").into_response(),
    };
    let upstream = match client.get(provider_url).send().await {
        Ok(response) => response,
        Err(_) => return (StatusCode::BAD_GATEWAY, "preview_network_error").into_response(),
    };
    if !upstream.status().is_success() {
        return (StatusCode::BAD_GATEWAY, "preview_http_error").into_response();
    }
    if upstream
        .content_length()
        .is_some_and(|length| length > MAX_PREVIEW_BYTES)
    {
        return (StatusCode::PAYLOAD_TOO_LARGE, "preview_too_large").into_response();
    }
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("image/png"));
    if !content_type
        .to_str()
        .is_ok_and(|value| value.to_ascii_lowercase().starts_with("image/"))
    {
        return (StatusCode::BAD_GATEWAY, "preview_not_image").into_response();
    }
    let bytes = match upstream.bytes().await {
        Ok(bytes) if !bytes.is_empty() && bytes.len() as u64 <= MAX_PREVIEW_BYTES => bytes,
        Ok(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "preview_invalid_size").into_response(),
        Err(_) => return (StatusCode::BAD_GATEWAY, "preview_read_error").into_response(),
    };

    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

fn screenshot_provider_url(base: &str, target: &url::Url) -> Result<url::Url, &'static str> {
    let mut provider = url::Url::parse(base.trim()).map_err(|_| "invalid_preview_provider")?;
    if !matches!(provider.scheme(), "http" | "https") || provider.host_str().is_none() {
        return Err("invalid_preview_provider");
    }
    provider
        .query_pairs_mut()
        .append_pair("url", target.as_str());
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_encoded_provider_url_without_changing_the_target() {
        let target = url::Url::parse(
            "https://yandex.ru/maps/org/bazar/166729948994/?utm_medium=mapframe&utm_source=maps",
        )
        .unwrap();
        let provider = screenshot_provider_url(DEFAULT_SCREENSHOT_BASE, &target).unwrap();
        assert_eq!(
            provider
                .query_pairs()
                .find(|(key, _)| key == "url")
                .unwrap()
                .1,
            target.as_str()
        );
        assert_eq!(provider.host_str(), Some("image.thum.io"));
    }

    #[test]
    fn rejects_invalid_provider_configuration() {
        let target = url::Url::parse("https://example.com/").unwrap();
        assert_eq!(
            screenshot_provider_url("file:///tmp/preview", &target),
            Err("invalid_preview_provider")
        );
    }
}
