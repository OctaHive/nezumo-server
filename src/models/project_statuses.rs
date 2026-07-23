//! Project-scoped task status models and the built-in default status set.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;

/// A status in a project's task-card status dictionary.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ProjectStatus {
    pub id: Uuid,
    pub project_id: Uuid,
    pub label: String,
    pub color: String,
    pub position: i32,
    pub created_at: DateTime<Utc>,
}

/// Body for creating a project status.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ProjectStatusCreateBody {
    pub label: String,
    pub color: String,
}

/// Body for updating a project status (any subset of fields).
#[derive(Debug, Deserialize, ToSchema)]
pub struct ProjectStatusUpdateBody {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub color: Option<String>,
}

/// The preset default statuses (label + color), seeded into a project on first
/// read. This is the single source of truth for the task-card status preset —
/// clients load the per-project dictionary via `GET /projects/{id}/statuses`
/// and no longer hardcode a copy.
pub const DEFAULT_STATUSES: &[(&str, &str)] = &[
    ("To Do", "#94A3B8"),
    ("In Progress", "#4C6FFF"),
    ("In Review", "#F59E0B"),
    ("Done", "#22C55E"),
    ("Blocked", "#EF4444"),
];
