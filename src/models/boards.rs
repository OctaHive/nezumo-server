//! Board, membership, access, configuration, and state-transfer models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, FromRow)]
pub struct BoardRow {
    pub id: Uuid,
    pub project_id: Uuid,
    pub owner_id: Uuid,
    pub title: String,
    pub visibility: String,
    pub link_access: String,
    pub grid_type: String,
    pub background_color: String,
    pub privacy_mode: bool,
    pub sticker_authors: bool,
    pub is_favorite: bool,
    pub created_at: DateTime<Utc>,
    /// S3 key of the server-rendered preview; only selected by some queries
    /// (defaults to `None` when the column isn't in the row).
    #[sqlx(default)]
    pub preview_object_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Board {
    pub id: Uuid,
    pub project_id: Uuid,
    pub owner_id: Uuid,
    pub title: String,
    pub visibility: String,
    pub link_access: String,
    pub grid_type: String,
    pub background_color: String,
    pub privacy_mode: bool,
    pub sticker_authors: bool,
    pub is_favorite: bool,
    pub created_at: DateTime<Utc>,
    /// Internal S3 key — never serialized to clients (they get `preview_url`).
    #[serde(skip)]
    pub preview_object_key: Option<String>,
    /// Presigned URL of the preview thumbnail, filled in by the handler.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_url: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct BoardCreateBody {
    pub project_id: Uuid,
    #[validate(length(min = 1, max = 200))]
    pub title: String,
    #[validate(length(min = 3, max = 16))]
    pub visibility: String,
    #[serde(default)]
    pub grid_type: Option<String>,
    #[serde(default)]
    pub background_color: Option<String>,
    #[serde(default)]
    pub link_access: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct BoardUpdateBody {
    #[validate(length(min = 1, max = 200))]
    pub title: Option<String>,
    pub visibility: Option<String>,
    pub link_access: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, sqlx::FromRow)]
pub struct BoardMember {
    pub board_id: Uuid,
    pub user_id: Uuid,
    pub role: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BoardStateResponse {
    pub seq: i64,
    pub state: serde_json::Value,
}

#[derive(Debug, Serialize, Clone, ToSchema, sqlx::FromRow)]
pub struct BoardMemberWithUser {
    pub board_id: Uuid,
    pub user_id: Uuid,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub username: String,
    pub email: String,
    pub profile_picture_url: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct AddBoardMemberBody {
    pub user_id: Uuid,
    #[validate(length(min = 1))]
    pub role: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RemoveBoardMemberBody {
    pub user_id: Uuid,
}

#[derive(Debug, Serialize, Clone, ToSchema, FromRow)]
pub struct BoardWithOwner {
    pub id: Uuid,
    pub project_id: Uuid,
    pub owner_id: Uuid,
    pub title: String,
    pub visibility: String,
    pub link_access: String,
    pub grid_type: String,
    pub background_color: String,
    pub privacy_mode: bool,
    pub sticker_authors: bool,
    pub is_favorite: bool,
    pub created_at: DateTime<Utc>,
    pub owner_username: String,
    pub user_role: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    /// Internal S3 key — never serialized to clients (they get `preview_url`).
    #[serde(skip)]
    #[sqlx(default)]
    pub preview_object_key: Option<String>,
    /// Presigned URL of the preview thumbnail, filled in by the handler (not a
    /// DB column).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    pub preview_url: Option<String>,
}

impl From<BoardRow> for Board {
    fn from(row: BoardRow) -> Self {
        Self {
            id: row.id,
            project_id: row.project_id,
            owner_id: row.owner_id,
            title: row.title,
            visibility: row.visibility,
            link_access: row.link_access,
            grid_type: row.grid_type,
            background_color: row.background_color,
            privacy_mode: row.privacy_mode,
            sticker_authors: row.sticker_authors,
            is_favorite: row.is_favorite,
            created_at: row.created_at,
            preview_object_key: row.preview_object_key,
            preview_url: None,
        }
    }
}
