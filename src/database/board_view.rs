//! Per-user, per-board camera (pan + zoom) persistence — so reopening a board
//! restores where the user left it. Runtime queries (no compile-time `query!`
//! macro) so the crate builds without a live DB / offline cache.

use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Fetch a user's saved view `(x, y, zoom)` for a board, or `None`.
pub async fn get_board_view(
    pool: &PgPool,
    user_id: Uuid,
    board_id: Uuid,
) -> Result<Option<(f64, f64, f64)>, sqlx::Error> {
    let row =
        sqlx::query("SELECT x, y, zoom FROM board_view_state WHERE user_id = $1 AND board_id = $2")
            .bind(user_id)
            .bind(board_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| {
        (
            r.get::<f64, _>("x"),
            r.get::<f64, _>("y"),
            r.get::<f64, _>("zoom"),
        )
    }))
}

/// Insert or update a user's view for a board.
pub async fn upsert_board_view(
    pool: &PgPool,
    user_id: Uuid,
    board_id: Uuid,
    x: f64,
    y: f64,
    zoom: f64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO board_view_state (user_id, board_id, x, y, zoom, updated_at)
         VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT (user_id, board_id)
         DO UPDATE SET x = $3, y = $4, zoom = $5, updated_at = now()",
    )
    .bind(user_id)
    .bind(board_id)
    .bind(x)
    .bind(y)
    .bind(zoom)
    .execute(pool)
    .await?;
    Ok(())
}
