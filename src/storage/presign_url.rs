//! Presigned object URLs and internal/public storage URL normalization.

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::warn;

use crate::core::config::{get_env, get_env_u64, get_env_with_default};
use crate::storage::StorageState;

/// Generates a pre-signed (pre-authenticated) URL for accessing a private S3/MinIO object.
///
/// This function creates a temporary, signed URL that allows anyone with the link to access
/// the specified object in your storage bucket, even if the bucket is not public. The URL
/// is valid for the given number of seconds (`expires_in_seconds`).
///
/// # Arguments
///
/// * `state` - Reference to the `StorageState` containing the S3 client.
/// * `bucket` - The name of the S3/MinIO bucket.
/// * `object_key` - The key (path) of the object in the bucket.
/// * `expires_in_seconds` - Duration in seconds for which the URL will remain valid.
///
/// # Returns
///
/// * `Ok(String)` containing the pre-signed URL if successful.
/// * `Err(String)` with an error message if the URL could not be generated.
///
/// # Examples
///
/// ```
/// # use your_crate::{StorageState, generate_presigned_url};
/// # async fn example(state: &StorageState) -> Result<(), String> {
/// let url = generate_presigned_url(state, "mybucket", "path/to/object.jpg", 900).await?;
/// println!("Pre-signed URL: {}", url);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if the presigning configuration cannot be created or if the S3 client fails to generate the URL.
///
/// # See also
///
/// - [AWS S3 Pre-signed URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)
///
pub async fn generate_presigned_url(
    state: &StorageState,
    bucket: &str,
    object_key: &str,
    expires_in_seconds: u64,
) -> Result<String, String> {
    let presign_config = PresigningConfig::expires_in(Duration::from_secs(expires_in_seconds))
        .map_err(|e| format!("Failed to create presign config: {}", e))?;

    let public_endpoint = get_env_with_default("STORAGE_PUBLIC_ENDPOINT", "");

    let presigned_req = if !public_endpoint.trim().is_empty() {
        let region = get_env_with_default("STORAGE_REGION", "us-east-1");
        let access_key = get_env("STORAGE_ACCESS_KEY");
        let secret_key = get_env("STORAGE_SECRET_KEY");

        let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
            .region(Region::new(region))
            .endpoint_url(public_endpoint.trim_end_matches('/'))
            .force_path_style(true)
            .credentials_provider(Credentials::new(
                access_key, secret_key, None, None, "custom",
            ))
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);
        client
            .get_object()
            .bucket(bucket)
            .key(object_key)
            .presigned(presign_config)
            .await
            .map_err(|e| format!("Failed to presign URL: {}", e))?
    } else {
        state
            .client
            .get_object()
            .bucket(bucket)
            .key(object_key)
            .presigned(presign_config)
            .await
            .map_err(|e| format!("Failed to presign URL: {}", e))?
    };

    Ok(presigned_req.uri().to_string())
}

/// If `stored_url` is a full URL into our own storage endpoint (the value kept
/// in e.g. `users.profile_picture_url`), return a short-lived presigned GET URL
/// for it so a client can actually fetch it from the private bucket. Returns
/// `None` for empty/external/unparseable values (caller falls back to no avatar).
pub async fn presign_stored_url(storage: &StorageState, stored_url: &str) -> Option<String> {
    let endpoint = &storage.endpoint_url;
    if stored_url.is_empty() || !stored_url.starts_with(endpoint.as_str()) {
        return None;
    }
    let rest = stored_url
        .strip_prefix(endpoint.as_str())
        .unwrap_or(stored_url)
        .trim_start_matches('/');
    let mut parts = rest.splitn(2, '/');
    let bucket = parts.next().unwrap_or("");
    let object_key = parts.next().unwrap_or("");
    if bucket.is_empty() || object_key.is_empty() {
        return None;
    }
    generate_presigned_url(storage, bucket, object_key, 900)
        .await
        .ok()
}

/// Replace the (likely expired) presigned media URLs baked into a board state
/// JSON with freshly signed ones, derived from the stable `*_object_key` fields
/// the state also stores. Mutates `state_value` in place. Best-effort: a key
/// that fails to presign just leaves its stale URL untouched, and a board with
/// no uploaded media is a no-op.
///
/// Board snapshots/events persist short-lived presigned URLs (TTL
/// `STORAGE_PRESIGN_TTL_SECONDS`, default 1h) that have long expired by the time
/// state is served or rendered server-side. Any path that hands board state to a
/// consumer which will actually fetch those URLs must call this first; it is the
/// server-side counterpart of the frontend's `refreshPresignedUrls`.
///
/// Each object-key field maps to its sibling URL field by name
/// (`object_key` -> `url`, `poster_object_key` -> `poster_url`, …).
pub async fn refresh_state_presigned_urls(
    storage: &StorageState,
    state_value: &mut serde_json::Value,
) {
    let mut keys = HashSet::new();
    crate::handlers::events::collect_object_keys(state_value, &mut keys);
    if keys.is_empty() {
        return;
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let ttl = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);

    let mut signed = HashMap::new();
    for key in keys {
        match generate_presigned_url(storage, &bucket, &key, ttl).await {
            Ok(url) => {
                signed.insert(key, url);
            }
            Err(err) => warn!("presign failed for key {}: {}", key, err),
        }
    }
    if signed.is_empty() {
        return;
    }

    rewrite_media_urls(state_value, &signed);
}

/// Replaces the current page URL of every `pdf.page` component with a fresh
/// presigned storage URL. PDF pages are addressed by `docId` + `pageIndex` and
/// therefore have no `*_object_key` sibling for [`refresh_state_presigned_urls`]
/// to discover.
///
/// Native preview/export renderers cannot fetch the relative API URL persisted
/// in `svgUrl` when `ASSET_BASE_URL` is empty. Signing the deterministic S3 key
/// makes the render input self-sufficient without persisting temporary URLs.
pub async fn refresh_pdf_page_presigned_urls(
    storage: &StorageState,
    board_id: uuid::Uuid,
    state_value: &mut serde_json::Value,
) {
    let mut keys = HashSet::new();
    collect_pdf_page_object_keys(state_value, board_id, &mut keys);
    if keys.is_empty() {
        return;
    }

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    let ttl = get_env_u64("STORAGE_PRESIGN_TTL_SECONDS", 3600);
    let mut signed = HashMap::new();
    for key in keys {
        match generate_presigned_url(storage, &bucket, &key, ttl).await {
            Ok(url) => {
                signed.insert(key, url);
            }
            Err(err) => warn!("PDF page presign failed for key {}: {}", key, err),
        }
    }
    rewrite_pdf_page_urls(state_value, board_id, &signed);
}

fn pdf_page_object_key(
    map: &serde_json::Map<String, serde_json::Value>,
    board_id: uuid::Uuid,
) -> Option<String> {
    if map.get("type").and_then(serde_json::Value::as_str) != Some("pdf.page") {
        return None;
    }
    let doc_id = map
        .get("docId")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| uuid::Uuid::parse_str(value).ok())?;
    let page_index = map
        .get("pageIndex")
        .or_else(|| map.get("page_index"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let page_index = u32::try_from(page_index).ok()?;
    Some(format!(
        "boards/{board_id}/pdf/{doc_id}/page-{page_index}.svg"
    ))
}

fn collect_pdf_page_object_keys(
    value: &serde_json::Value,
    board_id: uuid::Uuid,
    keys: &mut HashSet<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(key) = pdf_page_object_key(map, board_id) {
                keys.insert(key);
            }
            for child in map.values() {
                collect_pdf_page_object_keys(child, board_id, keys);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_pdf_page_object_keys(child, board_id, keys);
            }
        }
        _ => {}
    }
}

fn rewrite_pdf_page_urls(
    value: &mut serde_json::Value,
    board_id: uuid::Uuid,
    signed: &HashMap<String, String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(url) = pdf_page_object_key(map, board_id)
                .and_then(|key| signed.get(&key))
                .cloned()
            {
                map.insert("svgUrl".to_string(), serde_json::Value::String(url));
            }
            for child in map.values_mut() {
                rewrite_pdf_page_urls(child, board_id, signed);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                rewrite_pdf_page_urls(child, board_id, signed);
            }
        }
        _ => {}
    }
}

/// Recursively walk `v`; for every object holding a `*object_key` string whose
/// value resolves in `signed`, set the sibling URL field (the key with
/// `object_key` replaced by `url`) to the fresh presigned URL.
fn rewrite_media_urls(v: &mut serde_json::Value, signed: &HashMap<String, String>) {
    match v {
        serde_json::Value::Object(map) => {
            // Collect the (url_field, fresh_url) pairs first to avoid mutating the
            // map while iterating it.
            let mut updates = Vec::new();
            for (k, val) in map.iter() {
                if !k.contains("object_key") {
                    continue;
                }
                if let serde_json::Value::String(key) = val {
                    if let Some(url) = signed.get(key) {
                        updates.push((k.replace("object_key", "url"), url.clone()));
                    }
                }
            }
            for (url_field, url) in updates {
                map.insert(url_field, serde_json::Value::String(url));
            }
            for val in map.values_mut() {
                rewrite_media_urls(val, signed);
            }
        }
        serde_json::Value::Array(arr) => {
            for val in arr {
                rewrite_media_urls(val, signed);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_pdf_page_to_its_deterministic_signed_object_url() {
        let board_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let doc_id = "22222222-2222-2222-2222-222222222222";
        let key = format!("boards/{board_id}/pdf/{doc_id}/page-3.svg");
        let mut state = serde_json::json!({
            "entities": [{
                "components": [{
                    "type": "pdf.page",
                    "docId": doc_id,
                    "pageIndex": 3,
                    "svgUrl": "/boards/old/page/3",
                    "urlTemplate": "/boards/old/page/{n}"
                }]
            }]
        });

        let mut keys = HashSet::new();
        collect_pdf_page_object_keys(&state, board_id, &mut keys);
        assert_eq!(keys, HashSet::from([key.clone()]));

        rewrite_pdf_page_urls(
            &mut state,
            board_id,
            &HashMap::from([(key, "https://storage.test/signed-page-3".to_string())]),
        );
        let page = &state["entities"][0]["components"][0];
        assert_eq!(page["svgUrl"], "https://storage.test/signed-page-3");
        assert_eq!(page["urlTemplate"], "/boards/old/page/{n}");
    }

    #[test]
    fn ignores_non_pdf_and_invalid_document_references() {
        let board_id = uuid::Uuid::nil();
        let state = serde_json::json!({
            "entities": [{"components": [
                {"type": "image", "docId": uuid::Uuid::new_v4(), "pageIndex": 0},
                {"type": "pdf.page", "docId": "../other-object", "pageIndex": 0}
            ]}]
        });
        let mut keys = HashSet::new();
        collect_pdf_page_object_keys(&state, board_id, &mut keys);
        assert!(keys.is_empty());
    }
}
