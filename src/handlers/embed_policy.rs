//! Server-side iframe embedding policy probe.
//!
//! Browsers deliberately hide cross-origin iframe load failures from the parent
//! document, so the web client cannot distinguish a rendered page from an
//! `X-Frame-Options` / CSP rejection. This endpoint follows redirects with DNS
//! pinning, rejects non-public destinations, and classifies the final response
//! headers for the configured Nezumo application origin.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use axum::extract::Query;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::core::config::get_env_with_default;

const FETCH_TIMEOUT_SECS: u64 = 8;
const MAX_REDIRECTS: usize = 5;
const USER_AGENT: &str = "Mozilla/5.0 (compatible; NezumoEmbedPolicyBot/1.0; +https://nezumo.ru)";

#[derive(Debug, Deserialize)]
pub struct EmbedPolicyQuery {
    pub url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum EmbedPolicyStatus {
    Allowed,
    Blocked,
    Unknown,
}

#[derive(Debug, Serialize)]
struct EmbedPolicyResponse {
    status: EmbedPolicyStatus,
    reason: &'static str,
    final_url: String,
}

/// `GET /link/embed-policy?url=<page>` — determine whether the configured web
/// application origin is permitted to render the final page in an iframe.
pub async fn get_link_embed_policy(Query(params): Query<EmbedPolicyQuery>) -> Response {
    let parsed = match parse_public_http_url(params.url.trim()) {
        Ok(url) => url,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    let app_origin =
        match url::Url::parse(&get_env_with_default("APP_ORIGIN", "http://localhost:5173")) {
            Ok(origin) => origin,
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "APP_ORIGIN is invalid").into_response()
            }
        };

    let result = match fetch_final_headers(parsed, &app_origin).await {
        Ok(result) => result,
        Err((url, reason)) => EmbedPolicyResponse {
            status: EmbedPolicyStatus::Unknown,
            reason,
            final_url: url.to_string(),
        },
    };

    (
        [(header::CACHE_CONTROL, "public, max-age=300")],
        Json(result),
    )
        .into_response()
}

async fn fetch_final_headers(
    mut current: url::Url,
    app_origin: &url::Url,
) -> Result<EmbedPolicyResponse, (url::Url, &'static str)> {
    for redirects in 0..=MAX_REDIRECTS {
        let addresses = resolve_public_addresses(&current)
            .await
            .map_err(|reason| (current.clone(), reason))?;
        let host = current
            .host_str()
            .ok_or_else(|| (current.clone(), "missing_host"))?;
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
            .redirect(reqwest::redirect::Policy::none())
            // Pin the already-validated addresses so a second DNS answer cannot
            // redirect this public endpoint into a private network.
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| (current.clone(), "client_error"))?;
        let response = client
            .get(current.clone())
            .header(header::ACCEPT, "text/html,application/xhtml+xml")
            .send()
            .await
            .map_err(|_| (current.clone(), "network_error"))?;

        if response.status().is_redirection() {
            if redirects == MAX_REDIRECTS {
                return Err((current, "too_many_redirects"));
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| (current.clone(), "redirect_without_location"))?;
            current = current
                .join(location)
                .map_err(|_| (current.clone(), "invalid_redirect"))?;
            parse_public_http_url(current.as_str()).map_err(|reason| (current.clone(), reason))?;
            continue;
        }

        if !response.status().is_success() {
            return Ok(EmbedPolicyResponse {
                status: EmbedPolicyStatus::Unknown,
                reason: "http_error",
                final_url: current.to_string(),
            });
        }

        let (status, reason) = classify_headers(response.headers(), &current, app_origin);
        return Ok(EmbedPolicyResponse {
            status,
            reason,
            final_url: current.to_string(),
        });
    }
    Err((current, "too_many_redirects"))
}

pub(crate) fn parse_public_http_url(raw: &str) -> Result<url::Url, &'static str> {
    let url = url::Url::parse(raw).map_err(|_| "invalid_url")?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("invalid_scheme");
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("credentials_not_allowed");
    }
    match url.host().ok_or("missing_host")? {
        url::Host::Domain(host)
            if host.eq_ignore_ascii_case("localhost")
                || host.to_ascii_lowercase().ends_with(".localhost") =>
        {
            return Err("non_public_host");
        }
        url::Host::Ipv4(address) if !is_public_address(IpAddr::V4(address)) => {
            return Err("non_public_host");
        }
        url::Host::Ipv6(address) if !is_public_address(IpAddr::V6(address)) => {
            return Err("non_public_host");
        }
        _ => {}
    }
    Ok(url)
}

async fn resolve_public_addresses(url: &url::Url) -> Result<Vec<SocketAddr>, &'static str> {
    let host = url.host_str().ok_or("missing_host")?;
    let port = url.port_or_known_default().ok_or("missing_port")?;
    let mut addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| "dns_error")?
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err("dns_error");
    }
    if addresses
        .iter()
        .any(|address| !is_public_address(address.ip()))
    {
        return Err("non_public_host");
    }
    Ok(addresses)
}

fn is_public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let [a, b, c, _] = address.octets();
            !matches!(
                (a, b, c),
                (0, _, _)
                    | (10, _, _)
                    | (100, 64..=127, _)
                    | (127, _, _)
                    | (169, 254, _)
                    | (172, 16..=31, _)
                    | (192, 0, 0 | 2)
                    | (192, 88, 99)
                    | (192, 168, _)
                    | (198, 18 | 19, _)
                    | (198, 51, 100)
                    | (203, 0, 113)
                    | (224..=255, _, _)
            )
        }
        IpAddr::V6(address) => {
            if let Some(address) = address.to_ipv4_mapped() {
                return is_public_address(IpAddr::V4(address));
            }
            let segments = address.segments();
            let [a, b, _, _, _, _, _, _] = segments;
            !(address.is_unspecified()
                || address.is_loopback()
                || segments[..6].iter().all(|segment| *segment == 0)
                || (a & 0xfe00) == 0xfc00
                || (a & 0xffc0) == 0xfe80
                || (a & 0xff00) == 0xff00
                || (a == 0x0064 && b == 0xff9b)
                || (a == 0x0100 && b == 0)
                || (a == 0x2001 && b == 0x0db8))
        }
    }
}

fn classify_headers(
    headers: &HeaderMap,
    final_url: &url::Url,
    app_origin: &url::Url,
) -> (EmbedPolicyStatus, &'static str) {
    let same_origin = origins_equal(final_url, app_origin);
    for value in headers.get_all("x-frame-options") {
        let Ok(value) = value.to_str() else { continue };
        for directive in value.split(',').map(|part| part.trim()) {
            if directive.eq_ignore_ascii_case("deny")
                || (directive.eq_ignore_ascii_case("sameorigin") && !same_origin)
            {
                return (EmbedPolicyStatus::Blocked, "x_frame_options");
            }
        }
    }

    for value in headers.get_all("content-security-policy") {
        let Ok(value) = value.to_str() else { continue };
        for directive in value.split(';') {
            let mut parts = directive.split_ascii_whitespace();
            if !parts
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("frame-ancestors"))
            {
                continue;
            }
            let sources: Vec<&str> = parts.collect();
            if sources.is_empty()
                || !sources
                    .iter()
                    .any(|source| ancestor_source_allows(source, final_url, app_origin))
            {
                return (EmbedPolicyStatus::Blocked, "frame_ancestors");
            }
        }
    }

    (EmbedPolicyStatus::Allowed, "allowed")
}

fn ancestor_source_allows(source: &str, final_url: &url::Url, app_origin: &url::Url) -> bool {
    let source = source.trim();
    if source == "*" {
        return true;
    }
    if source.eq_ignore_ascii_case("'none'") {
        return false;
    }
    if source.eq_ignore_ascii_case("'self'") {
        return origins_equal(final_url, app_origin);
    }
    if source.ends_with(':') && !source.contains("//") {
        return source[..source.len() - 1].eq_ignore_ascii_case(app_origin.scheme());
    }

    let explicit_scheme = source.contains("://");
    let candidate = if explicit_scheme {
        source.to_owned()
    } else if source.starts_with("//") {
        format!("{}:{source}", app_origin.scheme())
    } else {
        format!("{}://{source}", app_origin.scheme())
    };
    let wildcard_host = candidate.contains("://*.");
    let candidate = candidate.replacen("://*.", "://wildcard.", 1);
    let Ok(allowed) = url::Url::parse(&candidate) else {
        return false;
    };
    if explicit_scheme && !allowed.scheme().eq_ignore_ascii_case(app_origin.scheme()) {
        return false;
    }
    let (Some(allowed_host), Some(app_host)) = (allowed.host_str(), app_origin.host_str()) else {
        return false;
    };
    let host_matches = if wildcard_host {
        allowed_host.strip_prefix("wildcard.").is_some_and(|root| {
            !app_host.eq_ignore_ascii_case(root)
                && app_host
                    .to_ascii_lowercase()
                    .ends_with(&format!(".{}", root.to_ascii_lowercase()))
        })
    } else {
        allowed_host.eq_ignore_ascii_case(app_host)
    };
    host_matches
        && (allowed.port_or_known_default().is_none()
            || allowed.port_or_known_default() == app_origin.port_or_known_default())
}

fn origins_equal(left: &url::Url, right: &url::Url) -> bool {
    left.scheme().eq_ignore_ascii_case(right.scheme())
        && left
            .host_str()
            .zip(right.host_str())
            .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
        && left.port_or_known_default() == right.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn url(value: &str) -> url::Url {
        url::Url::parse(value).unwrap()
    }

    #[test]
    fn rejects_private_and_credentialed_targets() {
        assert_eq!(
            parse_public_http_url("http://127.0.0.1/admin"),
            Err("non_public_host")
        );
        assert_eq!(
            parse_public_http_url("http://[::1]/admin"),
            Err("non_public_host")
        );
        assert_eq!(
            parse_public_http_url("http://localhost/admin"),
            Err("non_public_host")
        );
        assert_eq!(
            parse_public_http_url("https://user:secret@example.com/"),
            Err("credentials_not_allowed")
        );
        assert!(parse_public_http_url("https://ya.ru/").is_ok());
    }

    #[test]
    fn rejects_non_public_address_ranges() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.169.254",
            "172.31.255.255",
            "192.168.1.1",
            "198.51.100.1",
            "224.0.0.1",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "::127.0.0.1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(!is_public_address(address.parse().unwrap()), "{address}");
        }
        for address in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            assert!(is_public_address(address.parse().unwrap()), "{address}");
        }
    }

    #[test]
    fn x_frame_options_blocks_cross_origin_embedding() {
        let app = url("https://app.nezumo.ru/");
        let page = url("https://ya.ru/");
        for value in ["DENY", "SAMEORIGIN"] {
            let mut headers = HeaderMap::new();
            headers.insert("x-frame-options", HeaderValue::from_static(value));
            assert_eq!(
                classify_headers(&headers, &page, &app),
                (EmbedPolicyStatus::Blocked, "x_frame_options")
            );
        }
    }

    #[test]
    fn same_origin_and_explicit_csp_sources_are_allowed() {
        let app = url("https://app.nezumo.ru/");
        let mut headers = HeaderMap::new();
        headers.insert("x-frame-options", HeaderValue::from_static("SAMEORIGIN"));
        assert_eq!(
            classify_headers(&headers, &url("https://app.nezumo.ru/embed"), &app).0,
            EmbedPolicyStatus::Allowed
        );

        headers.remove("x-frame-options");
        headers.insert(
            "content-security-policy",
            HeaderValue::from_static("default-src 'self'; frame-ancestors https://app.nezumo.ru"),
        );
        assert_eq!(
            classify_headers(&headers, &url("https://widgets.example/"), &app).0,
            EmbedPolicyStatus::Allowed
        );

        for source in ["https://*.nezumo.ru", "app.nezumo.ru"] {
            headers.insert(
                "content-security-policy",
                HeaderValue::from_str(&format!("frame-ancestors {source}")).unwrap(),
            );
            assert_eq!(
                classify_headers(&headers, &url("https://widgets.example/"), &app).0,
                EmbedPolicyStatus::Allowed
            );
        }
    }

    #[test]
    fn restrictive_frame_ancestors_blocks_embedding() {
        let app = url("https://app.nezumo.ru/");
        for policy in ["frame-ancestors 'none'", "frame-ancestors 'self'"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-security-policy",
                HeaderValue::from_str(policy).unwrap(),
            );
            assert_eq!(
                classify_headers(&headers, &url("https://example.com/"), &app),
                (EmbedPolicyStatus::Blocked, "frame_ancestors")
            );
        }
    }

    #[tokio::test]
    #[ignore = "live network probe"]
    async fn ya_ru_live_policy_is_blocked() {
        let result = fetch_final_headers(url("https://ya.ru/"), &url("https://app.nezumo.ru/"))
            .await
            .unwrap();
        assert_eq!(result.status, EmbedPolicyStatus::Blocked);
    }
}
