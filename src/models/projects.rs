//! Project, project-member, creation, and database-row models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, FromRow)]
pub struct ProjectRow {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub is_favorite: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Project {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub is_favorite: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, sqlx::FromRow)]
pub struct ProjectMember {
    pub project_id: Uuid,
    pub user_id: Uuid,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub username: String,
    pub display_name: String,
    pub profile_picture_url: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct ProjectCreateBody {
    #[validate(length(min = 1, max = 200))]
    pub name: String,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct ProjectUpdateBody {
    #[validate(length(min = 1, max = 200))]
    pub name: String,
}

impl From<ProjectRow> for Project {
    fn from(row: ProjectRow) -> Self {
        Self {
            id: row.id,
            owner_id: row.owner_id,
            name: row.name,
            is_favorite: row.is_favorite,
            created_at: row.created_at,
        }
    }
}
