//! Ordered board event and compacted snapshot persistence/API models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CommitEventBody {
    #[validate(length(min = 1, max = 200))]
    pub event_type: String,
    pub payload: serde_json::Value,
    pub client_event_id: Option<String>,
    pub session_id: Option<String>,
    pub yrs: Option<crate::realtime::ClientYrsUpdate>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CommitEventResponse {
    pub seq: i64,
    pub server_event_id: Uuid,
    pub client_event_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema, FromRow)]
pub struct EventRecord {
    pub id: Uuid,
    pub board_id: Uuid,
    pub seq: i64,
    pub user_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub session_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema, FromRow)]
pub struct SnapshotRecord {
    pub id: Uuid,
    pub board_id: Uuid,
    pub seq: i64,
    pub state: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct SnapshotCreateBody {
    pub seq: i64,
    pub state: serde_json::Value,
}
