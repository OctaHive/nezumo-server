//! Per-project tag dictionary persistence. Runtime queries (no compile-time
//! `query!` macro) so the crate builds without a live DB / offline cache.
//! Unlike statuses there are no seeded defaults — a project starts with none.

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::models::project_tags::ProjectTag;

fn row_to_tag(r: &sqlx::postgres::PgRow) -> ProjectTag {
    ProjectTag {
        id: r.get("id"),
        project_id: r.get("project_id"),
        label: r.get("label"),
        color: r.get("color"),
        position: r.get("position"),
        created_at: r.get("created_at"),
    }
}

/// List a project's tags ordered by position.
pub async fn list_tags(pool: &PgPool, project_id: Uuid) -> Result<Vec<ProjectTag>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, project_id, label, color, position, created_at
         FROM project_tags WHERE project_id = $1
         ORDER BY position ASC, created_at ASC",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_tag).collect())
}

/// Add a tag to a project's dictionary (appended after the last position).
pub async fn create_tag(
    pool: &PgPool,
    project_id: Uuid,
    label: &str,
    color: &str,
) -> Result<ProjectTag, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO project_tags (project_id, label, color, position)
         VALUES ($1, $2, $3,
            COALESCE((SELECT MAX(position) + 1 FROM project_tags WHERE project_id = $1), 0))
         RETURNING id, project_id, label, color, position, created_at",
    )
    .bind(project_id)
    .bind(label)
    .bind(color)
    .fetch_one(pool)
    .await?;
    Ok(row_to_tag(&row))
}

/// Update a tag's label and/or color (a `None` field is left unchanged).
/// Returns `None` if no tag with that id exists in the project.
pub async fn update_tag(
    pool: &PgPool,
    project_id: Uuid,
    tag_id: Uuid,
    label: Option<&str>,
    color: Option<&str>,
) -> Result<Option<ProjectTag>, sqlx::Error> {
    let row = sqlx::query(
        "UPDATE project_tags
         SET label = COALESCE($3, label), color = COALESCE($4, color)
         WHERE id = $1 AND project_id = $2
         RETURNING id, project_id, label, color, position, created_at",
    )
    .bind(tag_id)
    .bind(project_id)
    .bind(label)
    .bind(color)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(row_to_tag))
}

/// Delete a tag from a project's dictionary.
pub async fn delete_tag(pool: &PgPool, project_id: Uuid, tag_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM project_tags WHERE id = $1 AND project_id = $2")
        .bind(tag_id)
        .bind(project_id)
        .execute(pool)
        .await?;
    Ok(())
}
