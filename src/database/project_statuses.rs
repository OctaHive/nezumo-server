//! Per-project task-card status dictionary persistence. Runtime queries (no
//! compile-time `query!` macro) so the crate builds without a live DB / offline
//! cache. The default set is seeded lazily on first read of a project.

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::models::project_statuses::{ProjectStatus, DEFAULT_STATUSES};

fn row_to_status(r: &sqlx::postgres::PgRow) -> ProjectStatus {
    ProjectStatus {
        id: r.get("id"),
        project_id: r.get("project_id"),
        label: r.get("label"),
        color: r.get("color"),
        position: r.get("position"),
        created_at: r.get("created_at"),
    }
}

async fn fetch_statuses(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Vec<ProjectStatus>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, project_id, label, color, position, created_at
         FROM project_statuses WHERE project_id = $1
         ORDER BY position ASC, created_at ASC",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_status).collect())
}

/// List a project's statuses, seeding the preset defaults on first access.
pub async fn list_or_seed_statuses(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Vec<ProjectStatus>, sqlx::Error> {
    let existing = fetch_statuses(pool, project_id).await?;
    if !existing.is_empty() {
        return Ok(existing);
    }
    for (i, (label, color)) in DEFAULT_STATUSES.iter().enumerate() {
        sqlx::query(
            "INSERT INTO project_statuses (project_id, label, color, position)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(project_id)
        .bind(label)
        .bind(color)
        .bind(i as i32)
        .execute(pool)
        .await?;
    }
    fetch_statuses(pool, project_id).await
}

/// Add a status to a project's dictionary (appended after the last position).
pub async fn create_status(
    pool: &PgPool,
    project_id: Uuid,
    label: &str,
    color: &str,
) -> Result<ProjectStatus, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO project_statuses (project_id, label, color, position)
         VALUES ($1, $2, $3,
            COALESCE((SELECT MAX(position) + 1 FROM project_statuses WHERE project_id = $1), 0))
         RETURNING id, project_id, label, color, position, created_at",
    )
    .bind(project_id)
    .bind(label)
    .bind(color)
    .fetch_one(pool)
    .await?;
    Ok(row_to_status(&row))
}

/// Update a status's label and/or color (a `None` field is left unchanged).
/// Returns `None` if no status with that id exists in the project.
pub async fn update_status(
    pool: &PgPool,
    project_id: Uuid,
    status_id: Uuid,
    label: Option<&str>,
    color: Option<&str>,
) -> Result<Option<ProjectStatus>, sqlx::Error> {
    let row = sqlx::query(
        "UPDATE project_statuses
         SET label = COALESCE($3, label), color = COALESCE($4, color)
         WHERE id = $1 AND project_id = $2
         RETURNING id, project_id, label, color, position, created_at",
    )
    .bind(status_id)
    .bind(project_id)
    .bind(label)
    .bind(color)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(row_to_status))
}

/// Delete a status from a project's dictionary.
pub async fn delete_status(
    pool: &PgPool,
    project_id: Uuid,
    status_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM project_statuses WHERE id = $1 AND project_id = $2")
        .bind(status_id)
        .bind(project_id)
        .execute(pool)
        .await?;
    Ok(())
}
