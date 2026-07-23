//! Project-scoped tag persistence and mutation payloads.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;

/// A tag in a project's tag dictionary. Reusable across elements (task cards
/// today, other elements later).
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ProjectTag {
    pub id: Uuid,
    pub project_id: Uuid,
    pub label: String,
    pub color: String,
    pub position: i32,
    pub created_at: DateTime<Utc>,
}

/// Body for creating a project tag.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ProjectTagCreateBody {
    pub label: String,
    pub color: String,
}

/// Body for updating a project tag (any subset of fields).
#[derive(Debug, Deserialize, ToSchema)]
pub struct ProjectTagUpdateBody {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub color: Option<String>,
}
