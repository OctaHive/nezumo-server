//! Read an object's raw bytes from storage (the inverse of `upload`).

use super::StorageState;

/// Fetch the full contents of `object_key` in `bucket`. Used by board export to
/// bundle referenced assets into the backup archive.
pub async fn download_from_storage(
    state: &StorageState,
    bucket: &str,
    object_key: &str,
) -> Result<Vec<u8>, String> {
    let obj = state
        .client
        .get_object()
        .bucket(bucket)
        .key(object_key)
        .send()
        .await
        .map_err(|e| format!("get_object {object_key}: {e}"))?;

    let bytes = obj
        .body
        .collect()
        .await
        .map_err(|e| format!("read body {object_key}: {e}"))?
        .into_bytes();

    Ok(bytes.to_vec())
}
