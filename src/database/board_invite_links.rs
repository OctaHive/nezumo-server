//! Persistence for shareable board invitation links.
//!
//! Invite tokens grant a configured board role until revoked or expired. The
//! handlers are responsible for authorization before creating or deleting them.

use crate::models::board_invite_links::{BoardInviteLink, InviteLinkInfo};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Creates a role-bearing invitation link for a board.
pub async fn create_invite_link(
    pool: &PgPool,
    board_id: Uuid,
    token: &str,
    role: &str,
    created_by: Uuid,
    expires_at: Option<DateTime<Utc>>,
) -> Result<BoardInviteLink, sqlx::Error> {
    let row = sqlx::query_as!(
        BoardInviteLink,
        r#"
        INSERT INTO board_invite_links (board_id, token, role, created_by, expires_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, board_id, token, role, created_by, expires_at, created_at
        "#,
        board_id,
        token,
        role,
        created_by,
        expires_at
    )
    .fetch_one(pool)
    .await?;

    Ok(row)
}

/// Lists active and expired invitation links owned by a board.
pub async fn list_invite_links(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Vec<BoardInviteLink>, sqlx::Error> {
    let rows = sqlx::query_as!(
        BoardInviteLink,
        r#"
        SELECT id, board_id, token, role, created_by, expires_at, created_at
        FROM board_invite_links
        WHERE board_id = $1
          AND (expires_at IS NULL OR expires_at > NOW())
        ORDER BY created_at DESC
        "#,
        board_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Resolves an invitation link by its opaque token.
pub async fn get_invite_link_by_token(
    pool: &PgPool,
    token: &str,
) -> Result<Option<BoardInviteLink>, sqlx::Error> {
    let row = sqlx::query_as!(
        BoardInviteLink,
        r#"
        SELECT id, board_id, token, role, created_by, expires_at, created_at
        FROM board_invite_links
        WHERE token = $1
        "#,
        token
    )
    .fetch_optional(pool)
    .await?;

    Ok(row)
}

/// Deletes an invitation link belonging to a board.
pub async fn delete_invite_link(
    pool: &PgPool,
    id: Uuid,
    board_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        DELETE FROM board_invite_links
        WHERE id = $1 AND board_id = $2
        "#,
        id,
        board_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Reads the public board and inviter information shown before acceptance.
pub async fn get_invite_link_info(
    pool: &PgPool,
    token: &str,
) -> Result<Option<InviteLinkInfo>, sqlx::Error> {
    let row = sqlx::query_as!(
        InviteLinkInfo,
        r#"
        SELECT bil.board_id, b.title AS board_title, bil.role,
               u.username AS creator_username, bil.expires_at
        FROM board_invite_links bil
        JOIN boards b ON b.id = bil.board_id
        JOIN users u ON u.id = bil.created_by
        WHERE bil.token = $1
        "#,
        token
    )
    .fetch_optional(pool)
    .await?;

    Ok(row)
}
