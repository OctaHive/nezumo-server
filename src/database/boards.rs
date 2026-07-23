//! Board persistence, listing, access resolution, and event-sequence allocation.
//!
//! This module owns board rows and query-time authorization metadata. Event
//! payloads and compacted state are persisted by the sibling `events` and
//! `snapshots` modules.

use crate::models::boards::{Board, BoardCreateBody, BoardRow, BoardWithOwner};
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::database::quotas::{ensure_board_available, QuotaError};

#[derive(Debug, Error)]
pub enum BoardCreateError {
    #[error(transparent)]
    Quota(#[from] QuotaError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Inserts a board and initializes its event sequence state.
pub async fn create_board(
    pool: &PgPool,
    owner_id: Uuid,
    body: &BoardCreateBody,
) -> Result<Board, BoardCreateError> {
    let grid_type = body
        .grid_type
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("lines");
    let background_color = body
        .background_color
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("#f5f5f5");
    let link_access = body
        .link_access
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("none");

    let mut tx = pool.begin().await?;
    ensure_board_available(&mut tx, owner_id).await?;

    let row = sqlx::query_as::<_, BoardRow>(
        "INSERT INTO boards (project_id, owner_id, title, visibility, link_access, grid_type, background_color)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, project_id, owner_id, title, visibility, link_access, grid_type, background_color, privacy_mode, sticker_authors, is_favorite, created_at"
    )
    .bind(body.project_id)
    .bind(owner_id)
    .bind(&body.title)
    .bind(&body.visibility)
    .bind(link_access)
    .bind(grid_type)
    .bind(background_color)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(Board::from(row))
}

/// Lists boards that belong to a project and are visible to a user.
pub async fn list_boards_by_project(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Vec<Board>, sqlx::Error> {
    let rows = sqlx::query_as::<_, BoardRow>(
        "SELECT id, project_id, owner_id, title, visibility, link_access, grid_type, background_color, privacy_mode, sticker_authors, is_favorite, created_at, preview_object_key
         FROM boards
         WHERE project_id = $1
         ORDER BY created_at DESC"
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(Board::from).collect())
}

/// Fetches one board by primary key.
pub async fn get_board_by_id(pool: &PgPool, board_id: Uuid) -> Result<Option<Board>, sqlx::Error> {
    let row = sqlx::query_as::<_, BoardRow>(
        "SELECT id, project_id, owner_id, title, visibility, link_access, grid_type, background_color, privacy_mode, sticker_authors, is_favorite, created_at, preview_object_key
         FROM boards
         WHERE id = $1"
    )
    .bind(board_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(Board::from))
}

/// Atomically claim the next event `seq` for a board, in its OWN short
/// transaction (runs directly on the pool → autocommit).
///
/// CRITICAL: this must NOT share the caller's commit transaction. A Postgres
/// row write-lock is held until the end of the transaction that took it, so if
/// Reserves one event sequence inside `tx`. The coordinator persists the
/// sequence, event, and Yrs update atomically under the same transaction.
pub async fn reserve_next_event_seq_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    board_id: Uuid,
) -> Result<i64, sqlx::Error> {
    let start: i64 = sqlx::query_scalar(
        r#"
        UPDATE boards
        SET next_event_seq = next_event_seq + 1
        WHERE id = $1
        RETURNING next_event_seq - 1
        "#,
    )
    .bind(board_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(start)
}

/// Lists boards visible to a user across ownership and memberships.
pub async fn list_boards_for_user(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<BoardWithOwner>, sqlx::Error> {
    let rows = sqlx::query_as::<_, BoardWithOwner>(
        "SELECT DISTINCT b.id, b.project_id, b.owner_id, b.title, b.visibility, b.link_access,
                b.grid_type, b.background_color, b.privacy_mode, b.sticker_authors, b.is_favorite, b.created_at,
                b.preview_object_key,
                u.username AS owner_username,
                bm.role AS user_role,
                p.name AS project_name
         FROM boards b
         JOIN users u ON u.id = b.owner_id
         JOIN projects p ON p.id = b.project_id
         LEFT JOIN board_members bm ON bm.board_id = b.id AND bm.user_id = $1
         WHERE b.owner_id = $1 OR bm.user_id IS NOT NULL OR b.visibility = 'public' OR b.link_access != 'none'
         ORDER BY b.created_at DESC"
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Lists every board for administrative endpoints.
pub async fn list_all_boards(pool: &PgPool) -> Result<Vec<BoardWithOwner>, sqlx::Error> {
    let rows = sqlx::query_as::<_, BoardWithOwner>(
        "SELECT DISTINCT b.id, b.project_id, b.owner_id, b.title, b.visibility, b.link_access,
                b.grid_type, b.background_color, b.privacy_mode, b.sticker_authors, b.is_favorite, b.created_at,
                b.preview_object_key,
                u.username AS owner_username,
                'owner' AS user_role,
                p.name AS project_name
         FROM boards b
         JOIN users u ON u.id = b.owner_id
         JOIN projects p ON p.id = b.project_id
         ORDER BY b.created_at DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Deletes a board by primary key.
pub async fn delete_board_by_id(pool: &PgPool, board_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        DELETE FROM boards
        WHERE id = $1
        "#,
        board_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Replaces the board configuration JSON without changing content state.
pub async fn update_board_config(
    tx: &mut Transaction<'_, Postgres>,
    board_id: Uuid,
    grid_type: Option<&str>,
    background_color: Option<&str>,
    privacy_mode: Option<bool>,
    sticker_authors: Option<bool>,
) -> Result<(), sqlx::Error> {
    if grid_type.is_none()
        && background_color.is_none()
        && privacy_mode.is_none()
        && sticker_authors.is_none()
    {
        return Ok(());
    }
    sqlx::query!(
        r#"
        UPDATE boards
        SET
            grid_type = COALESCE($2, grid_type),
            background_color = COALESCE($3, background_color),
            privacy_mode = COALESCE($4, privacy_mode),
            sticker_authors = COALESCE($5, sticker_authors)
        WHERE id = $1
        "#,
        board_id,
        grid_type,
        background_color,
        privacy_mode,
        sticker_authors
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Updates mutable board metadata fields.
pub async fn update_board(
    pool: &PgPool,
    board_id: Uuid,
    title: Option<&str>,
    visibility: Option<&str>,
    link_access: Option<&str>,
) -> Result<Board, sqlx::Error> {
    let row = sqlx::query_as::<_, BoardRow>(
        "UPDATE boards
         SET title = COALESCE($2, title),
             visibility = COALESCE($3, visibility),
             link_access = COALESCE($4, link_access)
         WHERE id = $1
         RETURNING id, project_id, owner_id, title, visibility, link_access, grid_type, background_color, privacy_mode, sticker_authors, is_favorite, created_at"
    )
    .bind(board_id)
    .bind(title)
    .bind(visibility)
    .bind(link_access)
    .fetch_one(pool)
    .await?;

    Ok(Board::from(row))
}

/// Toggles the requesting user's favorite state for a board.
pub async fn toggle_board_favorite(pool: &PgPool, board_id: Uuid) -> Result<Board, sqlx::Error> {
    let row = sqlx::query_as::<_, BoardRow>(
        "UPDATE boards SET is_favorite = NOT is_favorite
         WHERE id = $1
         RETURNING id, project_id, owner_id, title, visibility, link_access, grid_type, background_color, privacy_mode, sticker_authors, is_favorite, created_at"
    )
    .bind(board_id)
    .fetch_one(pool)
    .await?;

    Ok(Board::from(row))
}
