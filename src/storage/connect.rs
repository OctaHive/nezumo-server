//! Construction and validation of the S3-compatible storage client.

use aws_sdk_s3::{
    config::{Credentials, Region},
    Client as S3Client,
};
use thiserror::Error;
use url::Url;

use crate::core::config::{get_env, get_env_with_default};
use crate::storage::StorageState;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("❌  Environment error: {0}")]
    EnvError(String),

    #[error("❌  URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("❌  AWS config error: {0}")]
    AwsConfigError(String),

    #[error("❌  Storage connection error: {0}")]
    ConnectionError(String),

    #[error("❌  Storage operation error: {0}")]
    OperationError(String),
}

/// Creates the S3 client, validates its endpoint, and returns shared storage state.
pub async fn connect_to_storage() -> Result<StorageState, StorageError> {
    // Load environment variables with clear errors
    let endpoint_base = get_env("STORAGE_HOST");
    let port = get_env_with_default("STORAGE_PORT", "9000"); // Default
    let region = get_env_with_default("STORAGE_REGION", "us-east-1");
    let access_key = get_env("STORAGE_ACCESS_KEY");
    let secret_key = get_env("STORAGE_SECRET_KEY");

    // Validate endpoint URL
    let endpoint = if port.is_empty() || port == "443" || port == "80" {
        endpoint_base.trim_end_matches('/').to_string()
    } else {
        format!("{}:{}", endpoint_base.trim_end_matches('/'), port)
    };
    let endpoint_url = Url::parse(&endpoint)?;
    println!("ℹ️  STORAGE_ENDPOINT={}", endpoint_url);

    // Build base AWS config
    let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.clone()))
        .load()
        .await;

    // Build S3 config with custom endpoint and credentials
    let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
        .region(Region::new(region))
        .endpoint_url(endpoint_url.as_str())
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            access_key, secret_key, None,     // session_token
            None,     // expiration
            "custom", // provider name
        ))
        .build();

    // Create the S3 client
    let client = S3Client::from_conf(s3_config);

    let connect_retries: u32 = get_env_with_default("STORAGE_CONNECT_RETRIES", "10")
        .parse()
        .unwrap_or(10);
    let connect_delay_ms: u64 = get_env_with_default("STORAGE_CONNECT_RETRY_DELAY_MS", "1000")
        .parse()
        .unwrap_or(1000);

    let mut last_err: Option<String> = None;
    for attempt in 1..=connect_retries {
        match client.list_buckets().send().await {
            Ok(_response) => {
                return Ok(StorageState {
                    client,
                    endpoint_url: endpoint_url.to_string(),
                });
            }
            Err(err) => {
                last_err = Some(err.to_string());
                if attempt < connect_retries {
                    tokio::time::sleep(std::time::Duration::from_millis(connect_delay_ms)).await;
                }
            }
        }
    }

    Err(StorageError::ConnectionError(format!(
        "Failed to connect to storage: {}",
        last_err.unwrap_or_else(|| "unknown error".to_string())
    )))
}
