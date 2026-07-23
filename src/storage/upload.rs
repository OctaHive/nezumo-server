//! Validated uploads to S3-compatible storage.

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;

use crate::core::config::get_env_with_default;
use crate::storage::StorageState;

/// Uploads a file to S3/MinIO and returns the public URL (or error)
#[allow(dead_code)]
pub async fn upload_to_storage(
    state: &StorageState,
    bucket: &str,
    object_key: &str,
    data: &[u8],
) -> Result<String, String> {
    // Input validation
    if bucket.trim().is_empty() {
        return Err("Upload error: bucket name is empty".to_string());
    }
    if object_key.trim().is_empty() {
        return Err("Upload error: object key is empty".to_string());
    }
    if data.is_empty() {
        return Err("Upload error: data buffer is empty".to_string());
    }

    let data_vec = data.to_vec();
    let put_result = state
        .client
        .put_object()
        .bucket(bucket)
        .key(object_key)
        .body(ByteStream::from(data_vec.clone()))
        .send()
        .await;

    let is_production = get_env_with_default("ENVIRONMENT", "development") == "production";

    let put_result = match put_result {
        Err(err) if err.code() == Some("NoSuchBucket") => {
            if is_production {
                return Err(format!(
                    "Bucket '{}' does not exist. In production, buckets must be created beforehand.",
                    bucket
                ));
            }

            // Try to create bucket on-the-fly (useful for local MinIO dev)
            let create_result = state.client.create_bucket().bucket(bucket).send().await;
            if let Err(create_err) = create_result {
                let code = create_err.code().unwrap_or("Unknown");
                let message = create_err.message().unwrap_or("No error message provided");
                return Err(format!(
                    "Failed to create bucket {} (code: {}): {}",
                    bucket, code, message
                ));
            }

            state
                .client
                .put_object()
                .bucket(bucket)
                .key(object_key)
                .body(ByteStream::from(data_vec))
                .send()
                .await
        }
        other => other,
    };

    match put_result {
        Ok(_) => Ok(format!(
            "{}/{}/{}",
            state.endpoint_url.trim_end_matches('/'),
            bucket,
            object_key
        )),
        Err(err) => {
            // Try to extract more info from the error, if available
            let code = err.code().unwrap_or("Unknown");
            let message = err.message().unwrap_or("No error message provided");
            Err(format!(
                "Failed to upload to storage (code: {}): {}",
                code, message
            ))
        }
    }
}
