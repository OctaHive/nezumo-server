//! S3-compatible object-storage connection and object operations.

pub mod connect;
pub mod delete;
pub mod download;
pub mod presign_url;
pub mod upload;

use aws_sdk_s3::Client as S3Client;

/// Shared S3 client and public endpoint configuration held by application state.
#[derive(Clone, Debug)]
pub struct StorageState {
    pub client: S3Client,
    pub endpoint_url: String, // e.g. "http://127.0.0.1:9000"
}
