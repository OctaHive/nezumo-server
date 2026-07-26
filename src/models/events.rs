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

/// Latest legacy snapshot response. A board with no materialized snapshot has
/// a valid empty baseline (`seq = 0`) instead of an exceptional HTTP 404.
#[derive(Debug, Serialize, ToSchema)]
pub struct LatestSnapshotResponse {
    pub id: Option<Uuid>,
    pub board_id: Uuid,
    pub seq: i64,
    pub state: serde_json::Value,
    pub created_at: Option<DateTime<Utc>>,
}

impl LatestSnapshotResponse {
    pub fn empty(board_id: Uuid) -> Self {
        Self {
            id: None,
            board_id,
            seq: 0,
            state: serde_json::json!({ "entities": [] }),
            created_at: None,
        }
    }
}

impl From<SnapshotRecord> for LatestSnapshotResponse {
    fn from(snapshot: SnapshotRecord) -> Self {
        Self {
            id: Some(snapshot.id),
            board_id: snapshot.board_id,
            seq: snapshot.seq,
            state: snapshot.state,
            created_at: Some(snapshot.created_at),
        }
    }
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct SnapshotCreateBody {
    pub seq: i64,
    pub state: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::LatestSnapshotResponse;
    use uuid::Uuid;

    #[test]
    fn empty_latest_snapshot_is_a_valid_zero_baseline() {
        let board_id = Uuid::new_v4();
        let snapshot = LatestSnapshotResponse::empty(board_id);
        assert_eq!(snapshot.board_id, board_id);
        assert_eq!(snapshot.seq, 0);
        assert_eq!(snapshot.state, serde_json::json!({ "entities": [] }));
        assert!(snapshot.id.is_none());
        assert!(snapshot.created_at.is_none());
    }
}
