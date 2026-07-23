//! Board membership and role persistence.
//!
//! These helpers add, update, list, and remove explicit board members. Board
//! ownership remains on the board row and is handled separately by callers.

use crate::models::boards::BoardMemberWithUser;
use sqlx::PgPool;
use uuid::Uuid;

/// Adds or updates a user's role on a board.
pub async fn add_board_member(
    pool: &PgPool,
    board_id: Uuid,
    user_id: Uuid,
    role: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO board_members (board_id, user_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT (board_id, user_id)
        DO UPDATE SET role = EXCLUDED.role
        "#,
        board_id,
        user_id,
        role
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Fetches one user's board role, if any.
pub async fn get_member_role(
    pool: &PgPool,
    board_id: Uuid,
    user_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT role
        FROM board_members
        WHERE board_id = $1 AND user_id = $2
        "#,
        board_id,
        user_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.role))
}

/// Removes a user's explicit board membership.
pub async fn remove_board_member(
    pool: &PgPool,
    board_id: Uuid,
    user_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        DELETE FROM board_members
        WHERE board_id = $1 AND user_id = $2
        "#,
        board_id,
        user_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Lists board memberships joined with public user details.
pub async fn list_board_members_with_users(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Vec<BoardMemberWithUser>, sqlx::Error> {
    let rows = sqlx::query_as!(
        BoardMemberWithUser,
        r#"
        SELECT bm.board_id, bm.user_id, bm.role, bm.created_at,
               u.username, u.email, u.profile_picture_url
        FROM board_members bm
        JOIN users u ON u.id = bm.user_id
        WHERE bm.board_id = $1
        ORDER BY bm.created_at ASC
        "#,
        board_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
