//! Metadata models for files uploaded to boards and stored externally in S3.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, FromRow, Serialize, Deserialize, Clone, ToSchema)]
pub struct BoardFile {
    pub id: Uuid,
    pub board_id: Uuid,
    pub uploader_id: Option<Uuid>,
    pub object_key: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub original_name: Option<String>,
    pub url: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BoardFileUploadResponse {
    pub id: Uuid,
    pub url: String,
    pub presigned_url: Option<String>,
    pub object_key: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub original_name: Option<String>,
}

/// Validated values required to create a board-file database row.
pub struct BoardFileInsert {
    pub board_id: Uuid,
    pub uploader_id: Option<Uuid>,
    pub object_key: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub original_name: Option<String>,
    pub url: String,
}

impl From<BoardFile> for BoardFileUploadResponse {
    fn from(file: BoardFile) -> Self {
        Self {
            id: file.id,
            url: file.url,
            presigned_url: None,
            object_key: file.object_key,
            content_type: file.content_type,
            size_bytes: file.size_bytes,
            original_name: file.original_name,
        }
    }
}
