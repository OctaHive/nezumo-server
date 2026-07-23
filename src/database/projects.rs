//! Project creation, lookup, listing, update, and deletion helpers.
//!
//! Queries combine owned and shared projects where required by the application;
//! authorization remains the responsibility of the calling handler.

use crate::models::projects::{Project, ProjectCreateBody, ProjectRow};
use sqlx::PgPool;
use uuid::Uuid;

/// Inserts a project owned by the supplied user.
pub async fn create_project(
    pool: &PgPool,
    owner_id: Uuid,
    body: &ProjectCreateBody,
) -> Result<Project, sqlx::Error> {
    let row = sqlx::query_as!(
        ProjectRow,
        r#"
        INSERT INTO projects (owner_id, name)
        VALUES ($1, $2)
        RETURNING id, owner_id, name, is_favorite, created_at
        "#,
        owner_id,
        body.name
    )
    .fetch_one(pool)
    .await?;

    Ok(Project::from(row))
}

/// Lists projects owned by or explicitly shared with a user.
pub async fn list_projects_for_user(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<Project>, sqlx::Error> {
    let rows = sqlx::query_as::<_, ProjectRow>(
        "
        SELECT id, owner_id, name, is_favorite, created_at
        FROM projects p
        WHERE p.owner_id = $1
           OR EXISTS (
             SELECT 1 FROM project_members pm
             WHERE pm.project_id = p.id AND pm.user_id = $1
           )
        ORDER BY created_at DESC
        ",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(Project::from).collect())
}

/// Lists every project for administrative endpoints.
pub async fn list_all_projects(pool: &PgPool) -> Result<Vec<Project>, sqlx::Error> {
    let rows = sqlx::query_as!(
        ProjectRow,
        r#"
        SELECT id, owner_id, name, is_favorite, created_at
        FROM projects
        ORDER BY created_at DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(Project::from).collect())
}

/// Fetches one project by primary key.
pub async fn get_project_by_id(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Option<Project>, sqlx::Error> {
    let row = sqlx::query_as!(
        ProjectRow,
        r#"
        SELECT id, owner_id, name, is_favorite, created_at
        FROM projects
        WHERE id = $1
        "#,
        project_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(Project::from))
}

/// Changes a project's display name.
pub async fn rename_project(
    pool: &PgPool,
    project_id: Uuid,
    new_name: &str,
) -> Result<Project, sqlx::Error> {
    let row = sqlx::query_as!(
        ProjectRow,
        r#"
        UPDATE projects SET name = $2
        WHERE id = $1
        RETURNING id, owner_id, name, is_favorite, created_at
        "#,
        project_id,
        new_name
    )
    .fetch_one(pool)
    .await?;

    Ok(Project::from(row))
}

/// Deletes a project and dependent rows according to database constraints.
pub async fn delete_project(pool: &PgPool, project_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        DELETE FROM projects WHERE id = $1
        "#,
        project_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Toggles the requesting user's favorite state for a project.
pub async fn toggle_project_favorite(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Project, sqlx::Error> {
    let row = sqlx::query_as!(
        ProjectRow,
        r#"
        UPDATE projects SET is_favorite = NOT is_favorite
        WHERE id = $1
        RETURNING id, owner_id, name, is_favorite, created_at
        "#,
        project_id
    )
    .fetch_one(pool)
    .await?;

    Ok(Project::from(row))
}
