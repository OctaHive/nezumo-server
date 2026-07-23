//! Request, persistence, and response models for board invitation links.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Clone, FromRow, ToSchema)]
pub struct BoardInviteLink {
    pub id: Uuid,
    pub board_id: Uuid,
    pub token: String,
    pub role: String,
    pub created_by: Uuid,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateInviteLinkBody {
    #[validate(length(min = 1))]
    pub role: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InviteLinkResponse {
    pub id: Uuid,
    pub token: String,
    pub role: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub url: String,
}

#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct InviteLinkInfo {
    pub board_id: Uuid,
    pub board_title: String,
    pub role: String,
    pub creator_username: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DeleteInviteLinkBody {
    pub id: Uuid,
}
